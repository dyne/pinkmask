package inspect

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/log"
	"github.com/dyne/pinkmask/internal/schema"
	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"
)

func Run(ctx context.Context, inPath string, draftPath string, logger *log.Logger) error {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", inPath))
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer func() { _ = db.Close() }()

	s, err := schema.Load(ctx, db)
	if err != nil {
		return err
	}

	fmt.Println("Tables:")
	order := schema.TableOrder(s)
	for _, name := range order {
		tbl := s.Tables[name]
		if tbl == nil {
			continue
		}
		count, err := rowCount(ctx, db, name)
		if err != nil {
			return err
		}
		fmt.Printf("- %s (%d rows)\n", name, count)
		pii := piiCandidates(tbl)
		if len(pii) > 0 {
			fmt.Printf("  PII candidates: %s\n", strings.Join(pii, ", "))
		}
	}
	if draftPath != "" {
		if err := writeDraftConfig(draftPath, buildDraftConfig(s)); err != nil {
			return err
		}
	}
	if logger != nil {
		logger.Infof("inspect complete")
	}
	return nil
}

func rowCount(ctx context.Context, db *sql.DB, table string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(1) FROM %s", schema.QuoteIdent(table))
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("count %s: %w", table, err)
	}
	return count, nil
}

func piiCandidates(tbl *schema.Table) []string {
	var out []string
	for _, c := range tbl.Columns {
		if suggestColumnTransformer(tbl, c) != nil {
			out = append(out, c.Name)
		}
	}
	return out
}

func buildDraftConfig(s *schema.Schema) map[string]any {
	tables := map[string]any{}
	for _, tbl := range s.Tables {
		if tbl == nil {
			continue
		}
		columns := map[string]any{}
		for _, col := range tbl.Columns {
			if tr := suggestColumnTransformer(tbl, col); tr != nil {
				columns[col.Name] = minimalTransformConfig(tr)
			}
		}
		if len(columns) > 0 {
			tables[tbl.Name] = map[string]any{
				"columns": columns,
			}
		}
	}
	if len(tables) == 0 {
		return map[string]any{}
	}
	return map[string]any{"tables": tables}
}

func suggestColumnTransformer(tbl *schema.Table, col schema.Column) *config.TransformConfig {
	if strings.EqualFold(tbl.Name, "_collections") {
		// Collection names, fields, rules, and indexes are schema metadata;
		// changing them can break PocketBase's internal table mapping.
		return nil
	}
	if isStructuralColumn(col.Name) || isBooleanColumn(col) {
		return nil
	}
	tr := suggestTransformer(col.Name)
	if tr == nil {
		return nil
	}
	if tr.Type == "SetNull" && col.NotNull {
		// SetNull is invalid for PocketBase fields and most required columns.
		tr = &config.TransformConfig{Type: "SetValue", Value: "redacted"}
	}
	if isUniqueColumn(tbl, col.Name) {
		// Faker pools are intentionally small; a keyed digest preserves
		// uniqueness much more reliably for UNIQUE fields.
		return &config.TransformConfig{Type: "HmacSha256", MaxLen: 32}
	}
	return tr
}

func isUniqueColumn(tbl *schema.Table, name string) bool {
	for _, constraint := range tbl.UniqueConstraints {
		if len(constraint) == 1 && constraint[0] == name {
			return true
		}
	}
	return false
}

func isStructuralColumn(name string) bool {
	n := strings.ToLower(name)
	switch n {
	case "id", "created", "updated", "system", "type", "fields", "indexes", "options",
		"collectionref", "recordref", "emailvisibility", "verified", "listrule", "viewrule",
		"createrule", "updaterule", "deleterule", "passwordconfirm":
		return true
	}
	return strings.HasSuffix(n, "rule")
}

func isBooleanColumn(col schema.Column) bool {
	typ := strings.ToUpper(col.Type)
	if strings.Contains(typ, "BOOL") {
		return true
	}
	if col.DefaultSQL != nil {
		defaultSQL := strings.ToUpper(strings.TrimSpace(*col.DefaultSQL))
		return defaultSQL == "TRUE" || defaultSQL == "FALSE"
	}
	return false
}

func suggestTransformer(name string) *config.TransformConfig {
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "email"):
		return &config.TransformConfig{Type: "FakerEmail"}
	case strings.Contains(n, "password"), strings.Contains(n, "passwd"), strings.Contains(n, "pwd"):
		return &config.TransformConfig{Type: "SetValue", Value: "redacted"}
	case strings.Contains(n, "token"), strings.Contains(n, "secret"), strings.Contains(n, "apikey"), strings.Contains(n, "api_key"), strings.Contains(n, "fingerprint"):
		return &config.TransformConfig{Type: "SetValue", Value: "redacted"}
	case strings.Contains(n, "phone"), strings.Contains(n, "mobile"), strings.Contains(n, "telephone"), strings.Contains(n, "fax"):
		return &config.TransformConfig{Type: "FakerPhone"}
	case strings.Contains(n, "ssn"), strings.Contains(n, "taxid"), strings.Contains(n, "tax_id"):
		return &config.TransformConfig{Type: "SetNull"}
	case strings.Contains(n, "birth"), strings.Contains(n, "birthday"), strings.Contains(n, "dob"):
		return &config.TransformConfig{Type: "DateShift", Params: map[string]any{"max_days": 60}}
	case isDateFieldName(n):
		return &config.TransformConfig{Type: "DateShift", Params: map[string]any{"max_days": 30}}
	case strings.Contains(n, "address"), strings.Contains(n, "street"), strings.Contains(n, "city"), strings.Contains(n, "postal"), strings.Contains(n, "zip"):
		return &config.TransformConfig{Type: "FakerAddress"}
	case strings.Contains(n, "name"), strings.Contains(n, "username"), strings.Contains(n, "user_name"), strings.Contains(n, "nickname"):
		return &config.TransformConfig{Type: "FakerName"}
	default:
		return nil
	}
}

func isDateFieldName(name string) bool {
	for _, suffix := range []string{"_at", "_date", "_time", "timestamp"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return strings.HasPrefix(name, "date_") || strings.HasPrefix(name, "time_")
}

func writeDraftConfig(path string, cfg map[string]any) error {
	header := "# Draft mask config\n# Passwords are redacted; add a seed_rows entry with a bcrypt hash to create a known login\n"
	if path == "-" {
		if _, err := fmt.Fprint(os.Stdout, "\n"+header); err != nil {
			return fmt.Errorf("write draft header: %w", err)
		}
		enc := yaml.NewEncoder(os.Stdout)
		enc.SetIndent(2)
		if err := enc.Encode(cfg); err != nil {
			return fmt.Errorf("encode draft config: %w", err)
		}
		return enc.Close()
	}
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create draft config: %w", err)
	}
	defer func() { _ = file.Close() }()
	if _, err := fmt.Fprint(file, header); err != nil {
		return fmt.Errorf("write draft header: %w", err)
	}
	enc := yaml.NewEncoder(file)
	enc.SetIndent(2)
	if err := enc.Encode(cfg); err != nil {
		return fmt.Errorf("encode draft config: %w", err)
	}
	return enc.Close()
}

func minimalTransformConfig(tr *config.TransformConfig) map[string]any {
	out := map[string]any{}
	if tr.Type != "" {
		out["type"] = tr.Type
	}
	if len(tr.Params) > 0 {
		out["params"] = tr.Params
	}
	if tr.Value != nil {
		out["value"] = tr.Value
	}
	if tr.Pattern != "" {
		out["pattern"] = tr.Pattern
	}
	if tr.Replace != "" {
		out["replace"] = tr.Replace
	}
	if tr.Locale != "" {
		out["locale"] = tr.Locale
	}
	if tr.MaxLen > 0 {
		out["maxlen"] = tr.MaxLen
	}
	if len(tr.Map) > 0 {
		out["map"] = tr.Map
	}
	if tr.LookupTable != "" {
		out["lookup_table"] = tr.LookupTable
	}
	if tr.LookupKey != "" {
		out["lookup_key"] = tr.LookupKey
	}
	if tr.LookupValue != "" {
		out["lookup_value"] = tr.LookupValue
	}
	return out
}
