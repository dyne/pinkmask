package copy

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/log"
	"github.com/dyne/pinkmask/internal/transform"
	_ "modernc.org/sqlite"
)

func TestCopyAndTransform(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "in.sqlite")
	outPath := filepath.Join(tmp, "out.sqlite")
	if err := createTestDB(inPath); err != nil {
		t.Fatalf("create db: %v", err)
	}
	cfg := &config.Config{
		Tables: map[string]*config.TableConfig{
			"users": {
				Columns: map[string]*config.TransformConfig{
					"email":     {Type: "HmacSha256", MaxLen: 16},
					"full_name": {Type: "FakerName"},
				},
			},
		},
	}
	opts := Options{
		InPath:  inPath,
		OutPath: outPath,
		Config:  cfg,
		Salt:    "salt",
		Seed:    7,
		FKMode:  "on",
		Jobs:    2,
		Logger:  log.New(log.LevelInfo, nil),
	}
	if err := Run(ctx, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	outDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", outPath))
	if err != nil {
		t.Fatalf("open out: %v", err)
	}
	defer outDB.Close()
	if err := checkFK(outDB); err != nil {
		t.Fatalf("fk check: %v", err)
	}
	var masked string
	if err := outDB.QueryRow(`SELECT email FROM users WHERE id = 1`).Scan(&masked); err != nil {
		t.Fatalf("select masked: %v", err)
	}
	expected, _ := transform.NewHmacSha256("salt", 16).Transform("user1@example.com", transform.RowContext{Table: "users", PK: []any{int64(1)}, Seed: 7, Salt: "salt"})
	if masked != expected {
		t.Fatalf("masked email mismatch: %v vs %v", masked, expected)
	}
}

func TestSubsetCopy(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "in.sqlite")
	outPath := filepath.Join(tmp, "out.sqlite")
	if err := createTestDB(inPath); err != nil {
		t.Fatalf("create db: %v", err)
	}
	cfg := &config.Config{
		Subset: &config.SubsetConfig{
			Roots: []config.RootConfig{{Table: "users", Where: "country = 'US'", Limit: 1}},
		},
	}
	opts := Options{
		InPath:  inPath,
		OutPath: outPath,
		Config:  cfg,
		Salt:    "salt",
		Seed:    7,
		FKMode:  "on",
		Jobs:    1,
		Subset:  true,
		Logger:  log.New(log.LevelInfo, nil),
	}
	if err := Run(ctx, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	outDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", outPath))
	if err != nil {
		t.Fatalf("open out: %v", err)
	}
	defer outDB.Close()
	if err := checkFK(outDB); err != nil {
		t.Fatalf("fk check: %v", err)
	}
	var count int
	if err := outDB.QueryRow(`SELECT COUNT(1) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count users: %v", err)
	}
	if count != 1 {
		t.Fatalf("unexpected user count: %d", count)
	}
}

func createTestDB(path string) error {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", path))
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		return err
	}
	stmts := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, full_name TEXT, country TEXT)`,
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, status TEXT, FOREIGN KEY(user_id) REFERENCES users(id))`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`INSERT INTO users (id, email, full_name, country) VALUES (1, 'user1@example.com', 'User One', 'US')`); err != nil {
		return err
	}
	if _, err := db.Exec(`INSERT INTO users (id, email, full_name, country) VALUES (2, 'user2@example.com', 'User Two', 'CA')`); err != nil {
		return err
	}
	if _, err := db.Exec(`INSERT INTO orders (id, user_id, status) VALUES (10, 1, 'pending')`); err != nil {
		return err
	}
	if _, err := db.Exec(`INSERT INTO orders (id, user_id, status) VALUES (11, 2, 'shipped')`); err != nil {
		return err
	}
	return nil
}

func checkFK(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("foreign key check failed")
	}
	return nil
}
