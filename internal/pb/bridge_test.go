package pb

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/dyne/pinkmask/internal/config"
	_ "modernc.org/sqlite"
)

func TestRunSQLiteToPBMapsForeignKeysAndTransforms(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	sourcePath := filepath.Join(tmp, "source.sqlite")
	db, err := sql.Open("sqlite", "file:"+sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT); CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT, FOREIGN KEY(user_id) REFERENCES users(id)); INSERT INTO users VALUES (1, 'Alice'); INSERT INTO orders VALUES (10, 1, 'pending')`)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	var created []map[string]any
	var batches []BatchRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/collections/_superusers/auth-with-password":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "token"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/collections":
			_ = json.NewEncoder(w).Encode(map[string]any{"totalPages": 1, "items": []any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/collections":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			created = append(created, body)
			id := "users1234567890"
			if body["name"] == "orders" {
				id = "orders123456789"
			}
			body["id"] = id
			_ = json.NewEncoder(w).Encode(body)
		case r.Method == http.MethodPost && r.URL.Path == "/api/batch":
			var body struct {
				Requests []BatchRequest `json:"requests"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			batches = append(batches, body.Requests...)
			_ = json.NewEncoder(w).Encode(map[string]any{"responses": []any{map[string]any{"status": 200}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	err = RunSQLiteToPB(context.Background(), BridgeOptions{
		SourcePath: sourcePath,
		DestURL:    server.URL,
		Config: &config.Config{Tables: map[string]*config.TableConfig{
			"orders": {Columns: map[string]*config.TransformConfig{
				"status": {Type: "SetValue", Value: "masked"},
			}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(created) != 2 {
		t.Fatalf("created collections = %d", len(created))
	}
	var relation map[string]any
	for _, col := range created {
		if col["name"] == "orders" {
			fields, _ := col["fields"].([]any)
			for _, raw := range fields {
				field, _ := raw.(map[string]any)
				if field["name"] == "user_id" {
					relation = field
				}
			}
		}
	}
	if relation == nil {
		t.Fatal("orders.user_id relation was not created")
	}
	options, _ := relation["options"].(map[string]any)
	if options["collectionId"] != "users1234567890" {
		t.Fatalf("relation target = %#v", options["collectionId"])
	}
	var userID string
	for _, request := range batches {
		if request.URL == "/api/collections/users/records" {
			userID, _ = request.Body["id"].(string)
		}
	}
	if userID == "" {
		t.Fatal("users record was not imported")
	}
	for _, request := range batches {
		if request.URL == "/api/collections/orders/records" {
			if request.Body["user_id"] != userID {
				t.Fatalf("relation value = %#v, user id = %s", request.Body["user_id"], userID)
			}
			if request.Body["status"] != "masked" {
				t.Fatalf("status = %#v", request.Body["status"])
			}
		}
	}
}

func TestRunPBToSQLiteExportsRelations(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	outPath := filepath.Join(tmp, "export.sqlite")
	userID := "users1234567890"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/collections/_superusers/auth-with-password":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "token"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/collections":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"totalPages": 1,
				"items": []any{
					map[string]any{"id": userID, "name": "users", "type": "base", "system": false, "fields": []any{map[string]any{"name": "name", "type": "text"}}},
					map[string]any{"id": "orders123456789", "name": "orders", "type": "base", "system": false, "fields": []any{map[string]any{"name": "user_id", "type": "relation", "options": map[string]any{"collectionId": userID}}, map[string]any{"name": "status", "type": "text"}}},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/collections/users/records":
			_ = json.NewEncoder(w).Encode(map[string]any{"totalPages": 1, "items": []any{map[string]any{"id": userID, "name": "Alice", "created": "2026-01-01", "updated": "2026-01-01"}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/collections/orders/records":
			_ = json.NewEncoder(w).Encode(map[string]any{"totalPages": 1, "items": []any{map[string]any{"id": "orders123456789", "user_id": userID, "status": "pending", "created": "2026-01-01", "updated": "2026-01-01"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	err := RunPBToSQLite(context.Background(), BridgeOptions{
		SourceURL: server.URL,
		DestPath:  outPath,
		Config:    &config.Config{},
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var name, relation string
	if err := db.QueryRow(`SELECT name FROM users WHERE id = ?`, userID).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT user_id FROM orders WHERE id = ?`, "orders123456789").Scan(&relation); err != nil {
		t.Fatal(err)
	}
	if name != "Alice" || relation != userID {
		t.Fatalf("exported values = %q, %q", name, relation)
	}
}
