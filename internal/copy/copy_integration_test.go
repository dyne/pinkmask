package copy

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
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
	defer func() { _ = outDB.Close() }()
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

func TestCopyResolvesUniqueTransformCollisions(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "in.sqlite")
	outPath := filepath.Join(tmp, "out.sqlite")
	inDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", inPath))
	if err != nil {
		t.Fatal(err)
	}
	_, err = inDB.ExecContext(ctx, `CREATE TABLE _collections (id TEXT PRIMARY KEY, name TEXT UNIQUE NOT NULL)`)
	if err == nil {
		_, err = inDB.ExecContext(ctx, `INSERT INTO _collections (id, name) VALUES ('one', 'customers'), ('two', 'orders')`)
	}
	_ = inDB.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = Run(ctx, Options{
		InPath: inPath, OutPath: outPath,
		Config: &config.Config{Tables: map[string]*config.TableConfig{
			"_collections": {Columns: map[string]*config.TransformConfig{
				"name": {Type: "SetValue", Value: "masked"},
			}},
		}},
		FKMode: "on", Triggers: "on", Jobs: 2,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	outDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", outPath))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outDB.Close() }()
	rows, err := outDB.QueryContext(ctx, `SELECT name FROM _collections ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(names), "[masked masked_1]"; got != want {
		t.Fatalf("names = %s, want %s", got, want)
	}
}

func TestSeedRowsInsertKnownSuperuser(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "in.sqlite")
	outPath := filepath.Join(tmp, "out.sqlite")
	inDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", inPath))
	if err != nil {
		t.Fatal(err)
	}
	_, err = inDB.ExecContext(ctx, `CREATE TABLE _superusers (
		id TEXT PRIMARY KEY DEFAULT ('r'||lower(hex(randomblob(6)))) NOT NULL,
		email TEXT NOT NULL,
		password TEXT NOT NULL,
		tokenKey TEXT NOT NULL DEFAULT '',
		verified BOOLEAN NOT NULL DEFAULT FALSE,
		created TEXT NOT NULL DEFAULT '',
		updated TEXT NOT NULL DEFAULT ''
	)`)
	if err == nil {
		_, err = inDB.ExecContext(ctx, `INSERT INTO _superusers (email, password) VALUES ('old@example.com', '$2a$10$OLDHASH..')`)
	}
	_ = inDB.Close()
	if err != nil {
		t.Fatal(err)
	}

	const adminHash = "$2a$10$GDzXh6uguwvZMBxIOjsTK.UXZazOoT07yUciJr3cEX3Kqw6e8P4fy"
	err = Run(ctx, Options{
		InPath: inPath, OutPath: outPath,
		Config: &config.Config{
			SeedRows: map[string][]map[string]any{
				"_superusers": {
					{"email": "admin@example.com", "password": adminHash},
				},
			},
		},
		FKMode: "on", Triggers: "on", Jobs: 2,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	outDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", outPath))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outDB.Close() }()
	var email string
	if err := outDB.QueryRowContext(ctx, `SELECT email FROM _superusers WHERE password = ?`, adminHash).Scan(&email); err != nil {
		t.Fatalf("seeded superuser not found: %v", err)
	}
	if email != "admin@example.com" {
		t.Fatalf("seeded email = %q", email)
	}
}

func TestSeedRowsRejectsUnknownColumn(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	inPath := filepath.Join(tmp, "in.sqlite")
	outPath := filepath.Join(tmp, "out.sqlite")
	inDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", inPath))
	if err != nil {
		t.Fatal(err)
	}
	_, err = inDB.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`)
	_ = inDB.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = Run(ctx, Options{
		InPath: inPath, OutPath: outPath,
		Config: &config.Config{
			SeedRows: map[string][]map[string]any{
				"users": {{"email": "a@example.com", "nope": "x"}},
			},
		},
		FKMode: "on", Triggers: "on",
	})
	if err == nil || !strings.Contains(err.Error(), "seed_rows.users: column nope does not exist") {
		t.Fatalf("err = %v", err)
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
	defer func() { _ = outDB.Close() }()
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
	defer func() { _ = db.Close() }()
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
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		return fmt.Errorf("foreign key check failed")
	}
	return nil
}
