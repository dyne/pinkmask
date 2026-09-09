package pb

import (
	"context"
	"encoding/json"
	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunMasksRecordsAndCreatesCollections(t *testing.T) {
	t.Parallel()

	collection := map[string]any{
		"id":     "users123456789",
		"name":   "users",
		"type":   "base",
		"system": false,
		"fields": []any{
			map[string]any{"name": "name", "type": "text"},
		},
		"listRule":   "",
		"createRule": "",
	}
	record := map[string]any{"id": "record1234567", "name": "Alice"}
	var createdCollection map[string]any
	var batchBody struct {
		Requests []struct {
			Body map[string]any `json:"body"`
		} `json:"requests"`
	}

	newServer := func(destination bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodPost && (r.URL.Path == "/api/collections/_superusers/auth-with-password" || r.URL.Path == "/api/admins/auth-with-password"):
				_ = json.NewEncoder(w).Encode(map[string]any{"token": "test-token"})
			case r.Method == http.MethodGet && r.URL.Path == "/api/collections":
				items := []any{collection}
				if destination {
					items = []any{}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"page": 1, "totalPages": 1, "items": items})
			case destination && r.Method == http.MethodPost && r.URL.Path == "/api/collections":
				if err := json.NewDecoder(r.Body).Decode(&createdCollection); err != nil {
					t.Fatal(err)
				}
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(collection)
			case !destination && r.Method == http.MethodGet && r.URL.Path == "/api/collections/users/records":
				_ = json.NewEncoder(w).Encode(map[string]any{"page": 1, "totalPages": 1, "items": []any{record}})
			case destination && r.Method == http.MethodPost && r.URL.Path == "/api/batch":
				if err := json.NewDecoder(r.Body).Decode(&batchBody); err != nil {
					t.Fatal(err)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"responses": []any{map[string]any{"status": 200}}})
			default:
				http.NotFound(w, r)
			}
		}))
	}

	src := newServer(false)
	defer src.Close()
	dst := newServer(true)
	defer dst.Close()

	cfg := &config.Config{Tables: map[string]*config.TableConfig{
		"users": {Columns: map[string]*config.TransformConfig{
			"name": {Type: "SetValue", Value: "masked"},
		}},
	}}
	logger := log.New(log.LevelDebug, nil)
	err := Run(context.Background(), Options{
		SourceURL: src.URL, SourceEmail: "source@example.test", SourcePassword: "source-pass",
		DestURL: dst.URL, DestEmail: "dest@example.test", DestPassword: "dest-pass",
		Config: cfg, Salt: "salt", Seed: 42, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if createdCollection["name"] != "users" {
		t.Fatalf("created collection = %#v", createdCollection)
	}
	fields, ok := createdCollection["fields"].([]any)
	if !ok || len(fields) != 1 {
		t.Fatalf("created fields = %#v", createdCollection["fields"])
	}
	if len(batchBody.Requests) != 1 || batchBody.Requests[0].Body["name"] != "masked" {
		t.Fatalf("batch body = %#v", batchBody)
	}
	if batchBody.Requests[0].Body["id"] != "record1234567" {
		t.Fatalf("record ID was not preserved: %#v", batchBody.Requests[0].Body)
	}
}

func TestMaskRecordRandomizesAuthPasswordAndFileName(t *testing.T) {
	t.Parallel()
	col := &Collection{Name: "members", Type: "auth"}
	rec := map[string]any{
		"id":              "record1234567",
		"password":        "source-password",
		"passwordConfirm": "source-password",
		"avatar":          "real.jpg",
	}
	if err := maskRecord(rec, col, nil, map[string][]string{"avatar": nil}, map[string]bool{"avatar": true}, nil, true, Options{Salt: "salt", Seed: 7}); err != nil {
		t.Fatal(err)
	}
	if rec["password"] == "source-password" || rec["password"] != rec["passwordConfirm"] {
		t.Fatalf("password was not randomized: %#v", rec)
	}
	if name, ok := rec["avatar"].(string); !ok || name == "real.jpg" || !strings.HasPrefix(name, "pinkmask_") || !strings.HasSuffix(name, ".jpg") {
		t.Fatalf("file name was not masked: %#v", rec["avatar"])
	}
	if rec["verified"] != false {
		t.Fatalf("verified = %#v", rec["verified"])
	}
}
