package inspect

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dyne/pinkmask/internal/log"
	"github.com/dyne/pinkmask/internal/schema"
	_ "modernc.org/sqlite"
)

func Run(ctx context.Context, inPath string, logger *log.Logger) error {
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
