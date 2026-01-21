package plan

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/log"
	_ "modernc.org/sqlite"
)

func TestPlanOutput(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "plan.sqlite")
	if err := createPlanDB(inPath); err != nil {
		t.Fatalf("create db: %v", err)
	}
	cfg := &config.Config{
		Tables: map[string]*config.TableConfig{
			"users": {
				Columns: map[string]*config.TransformConfig{
					"email":     {Type: "HmacSha256"},
					"full_name": {Type: "FakerName"},
				},
			},
		},
	}
	out := captureStdout(func() error {
		return Run(ctx, inPath, cfg, log.New(log.LevelInfo, io.Discard))
	})
	goldenPath := filepath.Join("testdata", "plan_golden.txt")
	golden, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if out != string(golden) {
		t.Fatalf("plan output mismatch\nexpected:\n%s\nactual:\n%s", string(golden), out)
	}
}

func captureStdout(fn func() error) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	_ = fn()
	_ = w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	_ = r.Close()
	return buf.String()
}

func createPlanDB(path string) error {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", path))
	if err != nil {
		return err
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, full_name TEXT)`,
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT, FOREIGN KEY(user_id) REFERENCES users(id))`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
