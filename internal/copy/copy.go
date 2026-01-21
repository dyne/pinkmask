package copy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/log"
	"github.com/dyne/pinkmask/internal/schema"
	"github.com/dyne/pinkmask/internal/subset"
	"github.com/dyne/pinkmask/internal/transform"
	_ "modernc.org/sqlite"
)

type Options struct {
	InPath   string
	OutPath  string
	Config   *config.Config
	Salt     string
	Seed     int64
	FKMode   string
	Triggers string
	Jobs     int
	TempDir  string
	Subset   bool
	Logger   *log.Logger
}

func Run(ctx context.Context, opts Options) error {
	if opts.InPath == "" || opts.OutPath == "" {
		return fmt.Errorf("input and output paths are required")
	}
	if opts.Config == nil {
		opts.Config = &config.Config{}
	}
	if err := os.RemoveAll(opts.OutPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove output: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(opts.OutPath), 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	inDB, err := sql.Open("sqlite", sqliteDSN(opts.InPath))
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer inDB.Close()

	outDB, err := sql.Open("sqlite", sqliteDSN(opts.OutPath))
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	defer outDB.Close()

	if err := setFKMode(ctx, outDB, opts.FKMode); err != nil {
		return err
	}

	s, err := schema.Load(ctx, inDB)
	if err != nil {
		return err
	}

	order := schema.TableOrder(s)
	var selection *subset.Selection
	if opts.Subset || opts.Config.Subset != nil {
		selection, err = subset.BuildSelection(ctx, inDB, s, opts.Config)
		if err != nil {
			return err
		}
	}
	if err := createSchema(ctx, outDB, s, order, opts); err != nil {
		return err
	}

	if err := copyData(ctx, inDB, outDB, s, order, opts, selection); err != nil {
		return err
	}

	if err := createPostDataSchema(ctx, outDB, s, opts); err != nil {
		return err
	}

	if opts.Logger != nil {
		opts.Logger.Infof("copy complete")
	}
	return nil
}

func sqliteDSN(path string) string {
	return fmt.Sprintf("file:%s?_busy_timeout=5000", path)
}

func setFKMode(ctx context.Context, db *sql.DB, mode string) error {
	mode = strings.ToLower(mode)
	switch mode {
	case "on", "off":
		_, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA foreign_keys = %s", strings.ToUpper(mode)))
		if err != nil {
			return fmt.Errorf("set foreign_keys: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("invalid fk mode: %s", mode)
	}
}

func createSchema(ctx context.Context, outDB *sql.DB, s *schema.Schema, order []string, opts Options) error {
	tx, err := outDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema tx: %w", err)
	}
	defer tx.Rollback()
	for _, name := range order {
		if !tableIncluded(opts.Config, name) {
			continue
		}
		tbl := s.Tables[name]
		if tbl == nil {
			continue
		}
		if _, err := tx.ExecContext(ctx, tbl.SQL); err != nil {
			return fmt.Errorf("create table %s: %w", name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema: %w", err)
	}
	return nil
}

func createPostDataSchema(ctx context.Context, outDB *sql.DB, s *schema.Schema, opts Options) error {
	tx, err := outDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin post-data tx: %w", err)
	}
	defer tx.Rollback()
	for _, v := range s.Views {
		if v.SQL == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, v.SQL); err != nil {
			return fmt.Errorf("create view %s: %w", v.Name, err)
		}
	}
	for _, idx := range s.Indexes {
		if idx.SQL == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, idx.SQL); err != nil {
			return fmt.Errorf("create index %s: %w", idx.Name, err)
		}
	}
	if strings.ToLower(opts.Triggers) == "on" {
		for _, tr := range s.Triggers {
			if tr.SQL == "" {
				continue
			}
			if _, err := tx.ExecContext(ctx, tr.SQL); err != nil {
				return fmt.Errorf("create trigger %s: %w", tr.Name, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit post-data: %w", err)
	}
	return nil
}

func copyData(ctx context.Context, inDB, outDB *sql.DB, s *schema.Schema, order []string, opts Options, selection *subset.Selection) error {
	for _, name := range order {
		if !tableIncluded(opts.Config, name) {
			if opts.Logger != nil {
				opts.Logger.Infof("skip table %s", name)
			}
			continue
		}
		bl := s.Tables[name]
		if bl == nil {
			continue
		}
		var selSet *subset.PKSet
		if selection != nil {
			selSet = selection.Sets[name]
			if selSet == nil {
				if opts.Logger != nil {
					opts.Logger.Infof("skip table %s (not selected)", name)
				}
				continue
			}
		}
		if opts.Logger != nil {
			opts.Logger.Infof("copy table %s", name)
		}
		if err := copyTable(ctx, inDB, outDB, bl, opts, selSet); err != nil {
			return err
		}
	}
	return nil
}

func copyTable(ctx context.Context, inDB, outDB *sql.DB, tbl *schema.Table, opts Options, selSet *subset.PKSet) error {
	colNames := make([]string, 0, len(tbl.Columns))
	colIndex := map[string]int{}
	for i, c := range tbl.Columns {
		colNames = append(colNames, c.Name)
		colIndex[c.Name] = i
	}
	pkCols := tbl.PrimaryKeys
	useRowID := len(pkCols) == 0 && !tbl.WithoutRowID
	selectCols := make([]string, 0, len(colNames)+1)
	if useRowID {
		selectCols = append(selectCols, "rowid")
	}
	for _, c := range colNames {
		selectCols = append(selectCols, schema.QuoteIdent(c))
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", schema.QuoteIdent(tbl.Name), strings.Join(quotedCols(colNames), ", "), placeholders(len(colNames)))
	stmt, err := outDB.PrepareContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("prepare insert %s: %w", tbl.Name, err)
	}
	defer stmt.Close()

	transformers, err := buildTransformers(ctx, inDB, opts.Config, tbl.Name, opts.Salt)
	if err != nil {
		return err
	}

	processRows := func(rows *sql.Rows) error {
		defer rows.Close()
		jobs := opts.Jobs
		if jobs < 1 {
			jobs = 1
		}
		if jobs == 1 || len(transformers) == 0 {
			return processRowsSequential(ctx, rows, stmt, selectCols, colIndex, pkCols, useRowID, transformers, opts, tbl)
		}
		return processRowsParallel(ctx, rows, stmt, selectCols, colIndex, pkCols, useRowID, transformers, opts, tbl, jobs)
	}

	if selSet == nil {
		orderBy := buildOrderBy(tbl, useRowID)
		query := fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(selectCols, ", "), schema.QuoteIdent(tbl.Name), orderBy)
		rows, err := inDB.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("select %s: %w", tbl.Name, err)
		}
		return processRows(rows)
	}

	pkValues, err := selSet.ValuesByColumns(selSet.Cols)
	if err != nil {
		return err
	}
	sortRows(pkValues)
	chunks := chunkValues(pkValues, 500)
	for _, chunk := range chunks {
		whereIn, args := buildTupleIn(selSet.Cols, chunk, useRowID)
		orderBy := buildOrderBy(tbl, useRowID)
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s %s", strings.Join(selectCols, ", "), schema.QuoteIdent(tbl.Name), whereIn, orderBy)
		rows, err := inDB.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("select subset %s: %w", tbl.Name, err)
		}
		if err := processRows(rows); err != nil {
			return err
		}
	}
	return nil
}

func processRowsSequential(ctx context.Context, rows *sql.Rows, stmt *sql.Stmt, selectCols []string, colIndex map[string]int, pkCols []string, useRowID bool, transformers map[string]transform.Transformer, opts Options, tbl *schema.Table) error {
	for rows.Next() {
		scanTargets := make([]any, len(selectCols))
		rowValues := make([]any, len(selectCols))
		for i := range scanTargets {
			scanTargets[i] = &rowValues[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return fmt.Errorf("scan row %s: %w", tbl.Name, err)
		}
		values, rowCtx := buildRowContext(rowValues, colIndex, pkCols, useRowID, opts, tbl)
		for col, tr := range transformers {
			idx := colIndex[col]
			newVal, err := tr.Transform(values[idx], rowCtx)
			if err != nil {
				return fmt.Errorf("transform %s.%s: %w", tbl.Name, col, err)
			}
			values[idx] = newVal
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("insert %s: %w", tbl.Name, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s: %w", tbl.Name, err)
	}
	return nil
}

func processRowsParallel(ctx context.Context, rows *sql.Rows, stmt *sql.Stmt, selectCols []string, colIndex map[string]int, pkCols []string, useRowID bool, transformers map[string]transform.Transformer, opts Options, tbl *schema.Table, jobs int) error {
	type job struct {
		index  int
		values []any
		rowCtx transform.RowContext
	}
	type result struct {
		index  int
		values []any
		err    error
	}
	jobsCh := make(chan job, jobs*2)
	resultsCh := make(chan result, jobs*2)
	for i := 0; i < jobs; i++ {
		go func() {
			for j := range jobsCh {
				values := make([]any, len(j.values))
				copy(values, j.values)
				var err error
				for col, tr := range transformers {
					idx := colIndex[col]
					values[idx], err = tr.Transform(values[idx], j.rowCtx)
					if err != nil {
						resultsCh <- result{index: j.index, err: err}
						goto next
					}
				}
				resultsCh <- result{index: j.index, values: values}
			next:
			}
		}()
	}
	index := 0
	inflight := 0
	pending := map[int]result{}
	nextIndex := 0
	flush := func(res result) error {
		if res.err != nil {
			return res.err
		}
		pending[res.index] = res
		for {
			r, ok := pending[nextIndex]
			if !ok {
				break
			}
			if _, err := stmt.ExecContext(ctx, r.values...); err != nil {
				return fmt.Errorf("insert %s: %w", tbl.Name, err)
			}
			delete(pending, nextIndex)
			nextIndex++
		}
		return nil
	}
	for rows.Next() {
		scanTargets := make([]any, len(selectCols))
		rowValues := make([]any, len(selectCols))
		for i := range scanTargets {
			scanTargets[i] = &rowValues[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			close(jobsCh)
			return fmt.Errorf("scan row %s: %w", tbl.Name, err)
		}
		values, rowCtx := buildRowContext(rowValues, colIndex, pkCols, useRowID, opts, tbl)
		jobsCh <- job{index: index, values: values, rowCtx: rowCtx}
		inflight++
		index++
		for inflight > jobs*2 {
			res := <-resultsCh
			inflight--
			if err := flush(res); err != nil {
				close(jobsCh)
				return err
			}
		}
	}
	close(jobsCh)
	for i := 0; i < inflight; i++ {
		res := <-resultsCh
		if err := flush(res); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s: %w", tbl.Name, err)
	}
	return nil
}

func buildRowContext(rowValues []any, colIndex map[string]int, pkCols []string, useRowID bool, opts Options, tbl *schema.Table) ([]any, transform.RowContext) {
	var rowid any
	start := 0
	if useRowID {
		rowid = rowValues[0]
		start = 1
	}
	values := make([]any, len(rowValues[start:]))
	copy(values, rowValues[start:])
	pkValues := make([]any, 0, len(pkCols))
	if len(pkCols) > 0 {
		for _, pk := range pkCols {
			idx := colIndex[pk]
			pkValues = append(pkValues, values[idx])
		}
	} else if useRowID {
		pkValues = append(pkValues, rowid)
	} else {
		pkValues = append(pkValues, rowFingerprint(values))
	}
	rowCtx := transform.RowContext{Table: tbl.Name, PK: pkValues, Seed: opts.Seed, Salt: opts.Salt}
	return values, rowCtx
}

func buildOrderBy(tbl *schema.Table, useRowID bool) string {
	if len(tbl.PrimaryKeys) > 0 {
		return "ORDER BY " + strings.Join(quotedCols(tbl.PrimaryKeys), ", ")
	}
	if useRowID {
		return "ORDER BY rowid"
	}
	return ""
}

func quotedCols(cols []string) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, schema.QuoteIdent(c))
	}
	return out
}

func placeholders(n int) string {
	vals := make([]string, n)
	for i := range vals {
		vals[i] = "?"
	}
	return strings.Join(vals, ", ")
}

func buildTransformers(ctx context.Context, db *sql.DB, cfg *config.Config, table string, salt string) (map[string]transform.Transformer, error) {
	result := map[string]transform.Transformer{}
	if cfg == nil {
		return result, nil
	}
	tbl := cfg.Tables[table]
	if tbl == nil {
		return result, nil
	}
	for col, tc := range tbl.Columns {
		if tc == nil {
			continue
		}
		tr, err := buildTransformerForColumn(ctx, db, tc, salt)
		if err != nil {
			return nil, fmt.Errorf("build transformer %s.%s: %w", table, col, err)
		}
		if tr != nil {
			result[col] = tr
		}
	}
	return result, nil
}

func buildTransformerForColumn(ctx context.Context, db *sql.DB, tc *config.TransformConfig, salt string) (transform.Transformer, error) {
	if tc.LookupTable != "" {
		mapping, err := loadLookupMap(ctx, db, tc)
		if err != nil {
			return nil, err
		}
		if len(mapping) > 0 {
			return transform.NewMapReplace(mapping), nil
		}
	}
	return transform.Build(tc, salt)
}

func loadLookupMap(ctx context.Context, db *sql.DB, tc *config.TransformConfig) (map[string]string, error) {
	if tc.LookupTable == "" {
		return nil, nil
	}
	if tc.LookupKey == "" || tc.LookupValue == "" {
		return nil, fmt.Errorf("lookup_table requires lookup_key and lookup_value")
	}
	query := fmt.Sprintf("SELECT %s, %s FROM %s", schema.QuoteIdent(tc.LookupKey), schema.QuoteIdent(tc.LookupValue), schema.QuoteIdent(tc.LookupTable))
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("lookup table %s: %w", tc.LookupTable, err)
	}
	defer rows.Close()
	result := map[string]string{}
	for k, v := range tc.Map {
		result[k] = v
	}
	for rows.Next() {
		var key, val any
		if err := rows.Scan(&key, &val); err != nil {
			return nil, fmt.Errorf("scan lookup table %s: %w", tc.LookupTable, err)
		}
		result[fmt.Sprint(key)] = fmt.Sprint(val)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate lookup table %s: %w", tc.LookupTable, err)
	}
	return result, nil
}

func tableIncluded(cfg *config.Config, name string) bool {
	if cfg == nil {
		return true
	}
	if len(cfg.IncludeTables) > 0 && !schema.MatchAny(cfg.IncludeTables, name) {
		return false
	}
	if schema.MatchAny(cfg.ExcludeTables, name) {
		return false
	}
	return true
}

func buildTupleIn(cols []string, values [][]any, useRowID bool) (string, []any) {
	if len(cols) == 1 {
		place := make([]string, 0, len(values))
		args := make([]any, 0, len(values))
		col := cols[0]
		if col == "rowid" && useRowID {
			col = "rowid"
		} else {
			col = schema.QuoteIdent(col)
		}
		for _, v := range values {
			place = append(place, "?")
			args = append(args, v[0])
		}
		return fmt.Sprintf("%s IN (%s)", col, strings.Join(place, ", ")), args
	}
	var builder strings.Builder
	builder.WriteString("(")
	builder.WriteString(strings.Join(quotedCols(cols), ", "))
	builder.WriteString(") IN (")
	args := make([]any, 0, len(values)*len(cols))
	for i, row := range values {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString("(")
		for j := range cols {
			if j > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString("?")
			args = append(args, row[j])
		}
		builder.WriteString(")")
	}
	builder.WriteString(")")
	return builder.String(), args
}

func chunkValues(values [][]any, size int) [][][]any {
	if size <= 0 || len(values) <= size {
		return [][][]any{values}
	}
	var chunks [][][]any
	for i := 0; i < len(values); i += size {
		end := i + size
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[i:end])
	}
	return chunks
}

func sortRows(values [][]any) {
	sort.Slice(values, func(i, j int) bool {
		return keyFor(values[i]) < keyFor(values[j])
	})
}

func keyFor(values []any) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, "|")
}

func rowFingerprint(values []any) string {
	h := sha256.New()
	for _, v := range values {
		_, _ = h.Write([]byte(fmt.Sprint(v)))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
