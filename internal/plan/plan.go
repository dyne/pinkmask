package plan

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/log"
	"github.com/dyne/pinkmask/internal/schema"
	"github.com/dyne/pinkmask/internal/transform"
	_ "modernc.org/sqlite"
)

func Run(ctx context.Context, inPath string, cfg *config.Config, logger *log.Logger) error {
	if cfg == nil {
		cfg = &config.Config{}
	}
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", inPath))
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer db.Close()

	s, err := schema.Load(ctx, db)
	if err != nil {
		return err
	}

	order := schema.TableOrder(s)
	fmt.Println("Plan:")
	for _, name := range order {
		if !tableIncluded(cfg, name) {
			continue
		}
		tbl := cfg.Tables[name]
		fmt.Printf("- %s\n", name)
		if tbl == nil || len(tbl.Columns) == 0 {
			fmt.Println("  (no transforms)")
			continue
		}
		cols := make([]string, 0, len(tbl.Columns))
		for c := range tbl.Columns {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		for _, c := range cols {
			tr, err := transform.Build(tbl.Columns[c], "")
			if err != nil {
				return err
			}
			name := "unknown"
			if tr != nil {
				name = tr.Name()
			}
			fmt.Printf("  - %s: %s\n", c, name)
		}
	}
	if logger != nil {
		logger.Infof("plan complete")
	}
	return nil
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
