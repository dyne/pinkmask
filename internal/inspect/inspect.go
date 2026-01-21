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
	defer db.Close()

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
		name := strings.ToLower(c.Name)
		if strings.Contains(name, "email") || strings.Contains(name, "name") || strings.Contains(name, "phone") ||
			strings.Contains(name, "ssn") || strings.Contains(name, "address") || strings.Contains(name, "street") {
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
			if tr := suggestTransformer(col.Name); tr != nil {
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

func suggestTransformer(name string) *config.TransformConfig {
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "email"):
		return &config.TransformConfig{Type: "HmacSha256", MaxLen: 24}
	case strings.Contains(n, "name"):
		return &config.TransformConfig{Type: "FakerName"}
	case strings.Contains(n, "phone"):
		return &config.TransformConfig{Type: "FakerPhone"}
	case strings.Contains(n, "ssn"):
		return &config.TransformConfig{Type: "SetNull"}
	case strings.Contains(n, "password"), strings.Contains(n, "passwd"), strings.Contains(n, "pwd"):
		return &config.TransformConfig{Type: "SetValue", Value: "redacted"}
	case strings.Contains(n, "birth"), strings.Contains(n, "birthday"), strings.Contains(n, "dob"):
		return &config.TransformConfig{Type: "DateShift", Params: map[string]any{"max_days": 60}}
	case strings.Contains(n, "createdat"), strings.Contains(n, "updatedat"), strings.Contains(n, "modifiedat"),
		strings.Contains(n, "created_at"), strings.Contains(n, "updated_at"), strings.Contains(n, "modified_at"),
		strings.Contains(n, "date"), strings.HasSuffix(n, "_at"), strings.Contains(n, "timestamp"):
		return &config.TransformConfig{Type: "DateShift", Params: map[string]any{"max_days": 30}}
	case strings.Contains(n, "address"), strings.Contains(n, "street"):
		return &config.TransformConfig{Type: "FakerAddress"}
	default:
		return nil
	}
}

func writeDraftConfig(path string, cfg map[string]any) error {
	if path == "-" {
		if _, err := fmt.Fprintln(os.Stdout, "\n# Draft mask config"); err != nil {
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
	defer file.Close()
	if _, err := fmt.Fprintln(file, "# Draft mask config"); err != nil {
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
	if tr.Params != nil && len(tr.Params) > 0 {
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
	if tr.Map != nil && len(tr.Map) > 0 {
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
