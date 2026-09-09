package pb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/log"
	"github.com/dyne/pinkmask/internal/schema"
	"github.com/dyne/pinkmask/internal/transform"
	_ "modernc.org/sqlite"
)

// BridgeOptions configures one-sided SQLite/PocketBase transfers.
type BridgeOptions struct {
	SourceURL      string
	SourceEmail    string
	SourcePassword string
	DestURL        string
	DestEmail      string
	DestPassword   string
	SourcePath     string
	DestPath       string
	Config         *config.Config
	Salt           string
	Seed           int64
	BatchSize      int
	Logger         *log.Logger
}

// RunSQLiteToPB copies a local SQLite database into a PocketBase instance.
func RunSQLiteToPB(ctx context.Context, opts BridgeOptions) error {
	if opts.SourcePath == "" {
		return fmt.Errorf("source SQLite path is required")
	}
	if opts.DestURL == "" {
		return fmt.Errorf("destination PocketBase URL is required")
	}
	if opts.Config == nil {
		opts.Config = &config.Config{}
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 25
	}
	logger := bridgeLogger(opts.Logger)
	db, err := sql.Open("sqlite", bridgeSQLiteDSN(opts.SourcePath))
	if err != nil {
		return fmt.Errorf("open source SQLite: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("open source SQLite: %w", err)
	}
	s, err := schema.Load(ctx, db)
	if err != nil {
		return fmt.Errorf("load SQLite schema: %w", err)
	}
	selected := make(map[string]*schema.Table)
	for name, tbl := range s.Tables {
		if bridgeTableIncluded(opts.Config, name) {
			selected[name] = tbl
		}
	}
	if len(selected) == 0 {
		return nil
	}
	rowsByTable, keys, err := bridgeLoadSQLiteRows(ctx, db, selected, s, opts.Salt)
	if err != nil {
		return err
	}

	dst := New(opts.DestURL)
	logger.Infof("logging into destination %s", opts.DestURL)
	if err := dst.Login(ctx, opts.DestEmail, opts.DestPassword); err != nil {
		return fmt.Errorf("destination login: %w", err)
	}
	destCols, err := dst.ListCollections(ctx)
	if err != nil {
		return fmt.Errorf("list destination collections: %w", err)
	}
	byName := make(map[string]*Collection, len(destCols))
	for _, col := range destCols {
		byName[col.Name] = col
	}
	collectionIDs := make(map[string]string, len(byName)+len(selected))
	for name, col := range byName {
		collectionIDs[name] = col.ID
	}
	if err := bridgeCreateSQLiteCollections(ctx, dst, selected, s, byName, collectionIDs, logger); err != nil {
		return err
	}

	for _, name := range bridgeSQLiteOrder(selected, s) {
		tbl := selected[name]
		col := byName[name]
		if col == nil {
			// bridgeCreateSQLiteCollections adds newly-created collections to byName
			return fmt.Errorf("destination collection %s was not created", name)
		}
		transformers, err := bridgeTransformers(opts.Config, name, opts.Salt)
		if err != nil {
			return err
		}
		records := make([]map[string]any, 0, len(rowsByTable[name]))
		for _, row := range rowsByTable[name] {
			rec := map[string]any{"id": keys[name][bridgeValueKey(row.sourceKey)]}
			for _, c := range tbl.Columns {
				v := row.values[c.Name]
				if c.Name == "id" || c.Name == "created" || c.Name == "updated" {
					continue
				}
				if target, fk := bridgeFKForColumn(tbl, c.Name); fk != nil {
					if _, included := selected[target]; !included {
						rec[c.Name] = bridgePBValue(v)
						continue
					}
					if v == nil {
						rec[c.Name] = nil
					} else if mapped := keys[target][bridgeValueKey(v)]; mapped != "" {
						rec[c.Name] = mapped
					} else {
						rec[c.Name] = nil
					}
					continue
				}
				rec[c.Name] = bridgePBValue(v)
			}
			rowCtx := transform.RowContext{Table: name, PK: []any{row.sourceKey}, Seed: opts.Seed, Salt: opts.Salt}
			for field, tr := range transformers {
				if _, ok := rec[field]; !ok {
					continue
				}
				v, err := tr.Transform(rec[field], rowCtx)
				if err != nil {
					return fmt.Errorf("transform %s.%s: %w", name, field, err)
				}
				rec[field] = bridgePBValue(v)
			}
			records = append(records, rec)
		}
		for start := 0; start < len(records); start += opts.BatchSize {
			end := start + opts.BatchSize
			if end > len(records) {
				end = len(records)
			}
			batch := records[start:end]
			if err := dst.Batch(ctx, col.Name, batch); err != nil {
				for _, rec := range batch {
					if err := dst.CreateRecord(ctx, col.Name, rec); err != nil {
						return fmt.Errorf("create %s record %v: %w", name, rec["id"], err)
					}
				}
			}
		}
		logger.Infof("collection %s: %d records copied", name, len(records))
	}
	return nil
}

// RunPBToSQLite copies selected PocketBase collections into a new SQLite file.
func RunPBToSQLite(ctx context.Context, opts BridgeOptions) error {
	if opts.SourceURL == "" {
		return fmt.Errorf("source PocketBase URL is required")
	}
	if opts.DestPath == "" {
		return fmt.Errorf("destination SQLite path is required")
	}
	if opts.Config == nil {
		opts.Config = &config.Config{}
	}
	logger := bridgeLogger(opts.Logger)
	src := New(opts.SourceURL)
	logger.Infof("logging into source %s", opts.SourceURL)
	if err := src.Login(ctx, opts.SourceEmail, opts.SourcePassword); err != nil {
		return fmt.Errorf("source login: %w", err)
	}
	all, err := src.ListCollections(ctx)
	if err != nil {
		return fmt.Errorf("list source collections: %w", err)
	}
	selected := make(map[string]*Collection)
	for _, col := range all {
		if col.System || col.Name == "_superusers" || col.Type == "view" || !bridgeTableIncluded(opts.Config, col.Name) {
			continue
		}
		selected[col.Name] = col
	}
	if err := os.RemoveAll(opts.DestPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove destination SQLite: %w", err)
	}
	if dir := filepath.Dir(opts.DestPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create destination directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", bridgeSQLiteDSN(opts.DestPath))
	if err != nil {
		return fmt.Errorf("open destination SQLite: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("open destination SQLite: %w", err)
	}
	specs, err := bridgePBTableSpecs(selected)
	if err != nil {
		return err
	}
	order := bridgePBOrder(specs)
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("disable foreign keys during schema setup: %w", err)
	}
	for _, name := range order {
		if _, err := db.ExecContext(ctx, specs[name].createSQL); err != nil {
			return fmt.Errorf("create SQLite table %s with %s: %w", name, specs[name].createSQL, err)
		}
	}
	for _, name := range order {
		col := selected[name]
		transformers, err := bridgeTransformers(opts.Config, name, opts.Salt)
		if err != nil {
			return err
		}
		fields, err := col.Fields()
		if err != nil {
			return err
		}
		fieldByName := make(map[string]Field, len(fields))
		for _, field := range fields {
			if !field.System && field.Name != "id" && field.Name != "created" && field.Name != "updated" {
				fieldByName[field.Name] = field
			}
		}
		insertCols := specs[name].insertCols
		stmt, err := db.PrepareContext(ctx, bridgeInsertSQL(name, insertCols))
		if err != nil {
			return fmt.Errorf("prepare SQLite insert %s: %w", name, err)
		}
		page := 1
		count := 0
		for {
			recs, err := src.ListRecordsPage(ctx, name, page)
			if err != nil {
				_ = stmt.Close()
				return fmt.Errorf("list records %s page %d: %w", name, page, err)
			}
			for _, rec := range recs.Items {
				rowCtx := transform.RowContext{Table: name, PK: []any{rec["id"]}, Seed: opts.Seed, Salt: opts.Salt}
				values := make([]any, 0, len(insertCols))
				for _, fieldName := range insertCols {
					var value any
					switch fieldName {
					case "id", "created", "updated":
						value = rec[fieldName]
					default:
						value = rec[fieldName]
						if tr := transformers[fieldName]; tr != nil {
							value, err = tr.Transform(value, rowCtx)
							if err != nil {
								_ = stmt.Close()
								return fmt.Errorf("transform %s.%s: %w", name, fieldName, err)
							}
						}
					}
					values = append(values, bridgeSQLiteValue(value, fieldByName[fieldName]))
				}
				if _, err := stmt.ExecContext(ctx, values...); err != nil {
					_ = stmt.Close()
					return fmt.Errorf("insert %s record %v: %w", name, rec["id"], err)
				}
				count++
			}
			if len(recs.Items) == 0 || page >= recs.TotalPages {
				break
			}
			page++
		}
		_ = stmt.Close()
		logger.Infof("collection %s: %d records copied", name, count)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable foreign keys: %w", err)
	}
	var violations int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pragma_foreign_key_check").Scan(&violations); err != nil {
		return fmt.Errorf("check foreign keys: %w", err)
	}
	if violations != 0 {
		return fmt.Errorf("foreign key check failed: %d violation(s)", violations)
	}
	return nil
}

type bridgeSQLiteRow struct {
	sourceKey any
	values    map[string]any
}

type bridgePBSpec struct {
	createSQL  string
	insertCols []string
	deps       []string
}

func bridgeLogger(l *log.Logger) *log.Logger {
	if l != nil {
		return l
	}
	return log.New(log.LevelInfo, io.Discard)
}

func bridgeSQLiteDSN(path string) string { return fmt.Sprintf("file:%s?_busy_timeout=5000", path) }

func bridgeTableIncluded(cfg *config.Config, name string) bool {
	if cfg == nil {
		return true
	}
	if len(cfg.IncludeTables) > 0 && !schema.MatchAny(cfg.IncludeTables, name) {
		return false
	}
	return !schema.MatchAny(cfg.ExcludeTables, name)
}

func bridgeValueKey(v any) string {
	if v == nil {
		return "<nil>"
	}
	switch x := v.(type) {
	case []byte:
		return string(x)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	default:
		return fmt.Sprint(v)
	}
}

func bridgePBID(table string, key any, salt string) string {
	s := bridgeValueKey(key)
	if len(s) == 15 {
		valid := true
		for _, r := range s {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
				valid = false
				break
			}
		}
		if valid {
			return s
		}
	}
	h := sha256.Sum256([]byte(salt + "|" + table + "|" + s))
	return hex.EncodeToString(h[:])[:15]
}

func bridgeLoadSQLiteRows(ctx context.Context, db *sql.DB, selected map[string]*schema.Table, full *schema.Schema, salt string) (map[string][]bridgeSQLiteRow, map[string]map[string]string, error) {
	rowsOut := make(map[string][]bridgeSQLiteRow, len(selected))
	keys := make(map[string]map[string]string, len(selected))
	for name, tbl := range selected {
		cols := make([]string, 0, len(tbl.Columns))
		for _, c := range tbl.Columns {
			cols = append(cols, c.Name)
		}
		if len(cols) == 0 {
			return nil, nil, fmt.Errorf("table %s has no columns", name)
		}
		useRowID := len(tbl.PrimaryKeys) == 0 && !tbl.WithoutRowID
		selectSQL := "SELECT " + strings.Join(bridgeQuotedCols(cols), ", ") + " FROM " + schema.QuoteIdent(name)
		if useRowID {
			selectSQL = "SELECT rowid, " + strings.Join(bridgeQuotedCols(cols), ", ") + " FROM " + schema.QuoteIdent(name)
		}
		rs, err := db.QueryContext(ctx, selectSQL)
		if err != nil {
			return nil, nil, fmt.Errorf("read table %s: %w", name, err)
		}
		var out []bridgeSQLiteRow
		for rs.Next() {
			values := make([]any, len(cols))
			if useRowID {
				values = make([]any, len(cols)+1)
			}
			args := make([]any, len(values))
			for i := range values {
				args[i] = &values[i]
			}
			if err := rs.Scan(args...); err != nil {
				_ = rs.Close()
				return nil, nil, fmt.Errorf("scan table %s: %w", name, err)
			}
			start := 0
			var key any
			if useRowID {
				key, start = values[0], 1
			}
			m := make(map[string]any, len(cols))
			for i, c := range cols {
				m[c] = values[i+start]
				if c == "id" || (key == nil && len(tbl.PrimaryKeys) == 1 && tbl.PrimaryKeys[0] == c) {
					key = values[i+start]
				}
			}
			if key == nil {
				parts := make([]string, 0, len(cols))
				for _, c := range cols {
					parts = append(parts, bridgeValueKey(m[c]))
				}
				key = strings.Join(parts, "|")
			}
			out = append(out, bridgeSQLiteRow{sourceKey: key, values: m})
		}
		if err := rs.Err(); err != nil {
			_ = rs.Close()
			return nil, nil, fmt.Errorf("iterate %s: %w", name, err)
		}
		_ = rs.Close()
		rowsOut[name] = out
		keys[name] = make(map[string]string, len(out))
		for _, row := range out {
			id := bridgePBID(name, row.sourceKey, salt)
			keys[name][bridgeValueKey(row.sourceKey)] = id
			for _, pk := range tbl.PrimaryKeys {
				if v := row.values[pk]; v != nil {
					keys[name][bridgeValueKey(v)] = id
				}
			}
			for _, other := range full.Tables {
				for _, fk := range other.ForeignKeys {
					if fk.Table == name {
						if v := row.values[fk.To]; v != nil {
							keys[name][bridgeValueKey(v)] = id
						}
					}
				}
			}
		}
	}
	return rowsOut, keys, nil
}

func bridgeSQLiteOrder(selected map[string]*schema.Table, s *schema.Schema) []string {
	copySchema := &schema.Schema{Tables: selected}
	order := schema.TableOrder(copySchema)
	if len(order) == 0 {
		for name := range selected {
			order = append(order, name)
		}
		sort.Strings(order)
	}
	return order
}

func bridgeCreateSQLiteCollections(ctx context.Context, dst *Client, selected map[string]*schema.Table, s *schema.Schema, byName map[string]*Collection, ids map[string]string, logger *log.Logger) error {
	state := map[string]int{}
	var create func(string) error
	create = func(name string) error {
		if byName[name] != nil {
			return nil
		}
		if state[name] == 1 {
			return nil
		} // cyclic relations are created without an unresolved target.
		if state[name] == 2 {
			return nil
		}
		state[name] = 1
		tbl := selected[name]
		for _, fk := range tbl.ForeignKeys {
			if _, ok := selected[fk.Table]; ok {
				if err := create(fk.Table); err != nil {
					return err
				}
			}
		}
		fields := bridgeSQLiteFields(tbl, selected, ids)
		raw := map[string]any{"name": name, "type": "base", "fields": fields}
		created, err := dst.CreateCollection(ctx, &Collection{Name: name, Type: "base", Raw: raw}, nil)
		if err != nil {
			return fmt.Errorf("create collection %s: %w", name, err)
		}
		byName[name], ids[name] = created, created.ID
		state[name] = 2
		logger.Infof("created collection %s", name)
		return nil
	}
	names := make([]string, 0, len(selected))
	for name := range selected {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := create(name); err != nil {
			return err
		}
	}
	return nil
}

func bridgeSQLiteFields(tbl *schema.Table, selected map[string]*schema.Table, ids map[string]string) []any {
	fields := make([]any, 0, len(tbl.Columns))
	for _, c := range tbl.Columns {
		if c.Name == "id" || c.Name == "created" || c.Name == "updated" {
			continue
		}
		f := map[string]any{"name": c.Name, "type": bridgePBType(c.Type)}
		if target, fk := bridgeFKForColumn(tbl, c.Name); fk != nil && selected[target] != nil {
			f["type"] = "relation"
			if id := ids[target]; id != "" {
				f["options"] = map[string]any{"collectionId": id, "maxSelect": 1, "cascadeDelete": false}
			}
		}
		fields = append(fields, f)
	}
	return fields
}

func bridgeFKForColumn(tbl *schema.Table, column string) (string, *schema.ForeignKey) {
	for i := range tbl.ForeignKeys {
		if tbl.ForeignKeys[i].From == column {
			return tbl.ForeignKeys[i].Table, &tbl.ForeignKeys[i]
		}
	}
	return "", nil
}

func bridgePBType(sqlType string) string {
	t := strings.ToUpper(sqlType)
	switch {
	case strings.Contains(t, "BOOL"):
		return "bool"
	case strings.Contains(t, "INT"), strings.Contains(t, "REAL"), strings.Contains(t, "FLOA"), strings.Contains(t, "DOUB"), strings.Contains(t, "NUM"), strings.Contains(t, "DEC"):
		return "number"
	case strings.Contains(t, "DATE"), strings.Contains(t, "TIME"):
		return "date"
	case strings.Contains(t, "JSON"):
		return "json"
	default:
		return "text"
	}
}

func bridgePBValue(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func bridgeTransformers(cfg *config.Config, table, salt string) (map[string]transform.Transformer, error) {
	out := map[string]transform.Transformer{}
	if cfg == nil || cfg.Tables[table] == nil {
		return out, nil
	}
	for col, tc := range cfg.Tables[table].Columns {
		if tc == nil {
			continue
		}
		if tc.LookupTable != "" {
			return nil, fmt.Errorf("transformer Map with lookup_table is not supported in bridge mode (column %s of %s)", col, table)
		}
		tr, err := transform.Build(tc, salt)
		if err != nil {
			return nil, fmt.Errorf("build transformer %s.%s: %w", table, col, err)
		}
		if tr != nil {
			out[col] = tr
		}
	}
	return out, nil
}

func bridgePBTableSpecs(cols map[string]*Collection) (map[string]*bridgePBSpec, error) {
	out := make(map[string]*bridgePBSpec, len(cols))
	byID := make(map[string]string, len(cols))
	for name, c := range cols {
		byID[c.ID] = name
	}
	for name, col := range cols {
		fields, err := col.Fields()
		if err != nil {
			return nil, err
		}
		defs := []string{schema.QuoteIdent("id") + " TEXT PRIMARY KEY", schema.QuoteIdent("created") + " TEXT", schema.QuoteIdent("updated") + " TEXT"}
		constraints := []string{}
		insert := []string{"id", "created", "updated"}
		var deps []string
		for _, f := range fields {
			if f.System || f.Name == "id" || f.Name == "created" || f.Name == "updated" {
				continue
			}
			typ := "TEXT"
			multi := false
			switch f.Type {
			case "bool":
				typ = "INTEGER"
			case "number":
				typ = "REAL"
			case "json", "file", "relation":
				typ = "TEXT"
				multi = f.Type == "relation" && bridgeFieldMulti(f)
			}
			defs = append(defs, schema.QuoteIdent(f.Name)+" "+typ)
			if f.Type == "relation" && !multi {
				if targetID := bridgeRelationTarget(f.Raw); targetID != "" {
					if target := byID[targetID]; target != "" {
						constraints = append(constraints, "FOREIGN KEY ("+schema.QuoteIdent(f.Name)+") REFERENCES "+schema.QuoteIdent(target)+" ("+schema.QuoteIdent("id")+")")
						deps = append(deps, target)
					}
				}
			}
			insert = append(insert, f.Name)
		}
		defs = append(defs, constraints...)
		out[name] = &bridgePBSpec{createSQL: "CREATE TABLE " + schema.QuoteIdent(name) + " (" + strings.Join(defs, ", ") + ")", insertCols: insert, deps: deps}
	}
	return out, nil
}

func bridgeFieldMulti(f Field) bool {
	for _, k := range []string{"maxSelect", "max"} {
		if v, ok := f.Raw[k].(float64); ok && v > 1 {
			return true
		}
	}
	if o, ok := f.Raw["options"].(map[string]any); ok {
		if v, ok := o["maxSelect"].(float64); ok && v > 1 {
			return true
		}
	}
	return false
}

func bridgeRelationTarget(raw map[string]any) string {
	if id, ok := raw["collectionId"].(string); ok {
		return id
	}
	if o, ok := raw["options"].(map[string]any); ok {
		if id, ok := o["collectionId"].(string); ok {
			return id
		}
	}
	return ""
}

func bridgePBOrder(specs map[string]*bridgePBSpec) []string {
	state := map[string]bool{}
	var out []string
	var visit func(string)
	visit = func(n string) {
		if state[n] {
			return
		}
		state[n] = true
		for _, d := range specs[n].deps {
			visit(d)
		}
		out = append(out, n)
	}
	names := make([]string, 0, len(specs))
	for n := range specs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		visit(n)
	}
	return out
}

func bridgeQuotedCols(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = schema.QuoteIdent(c)
	}
	return out
}
func bridgeInsertSQL(table string, cols []string) string {
	p := make([]string, len(cols))
	for i := range p {
		p[i] = "?"
	}
	return "INSERT INTO " + schema.QuoteIdent(table) + " (" + strings.Join(bridgeQuotedCols(cols), ", ") + ") VALUES (" + strings.Join(p, ", ") + ")"
}

func bridgeSQLiteValue(v any, f Field) any {
	if v == nil {
		return nil
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	if f.Type == "bool" {
		switch x := v.(type) {
		case bool:
			if x {
				return int64(1)
			}
			return int64(0)
		case float64:
			if x != 0 {
				return int64(1)
			}
			return int64(0)
		}
	}
	if f.Type == "json" || f.Type == "relation" && bridgeFieldMulti(f) {
		if s, ok := v.(string); ok {
			return s
		}
		b, err := json.Marshal(v)
		if err == nil {
			return string(b)
		}
	}
	switch x := v.(type) {
	case []any, map[string]any:
		if b, err := json.Marshal(x); err == nil {
			return string(b)
		}
	case bool:
		if x {
			return int64(1)
		}
		return int64(0)
	}
	return v
}
