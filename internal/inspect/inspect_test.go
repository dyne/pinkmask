package inspect

import (
	"testing"

	"github.com/dyne/pinkmask/internal/schema"
)

func TestBuildDraftConfigAvoidsPocketBaseStructuralFields(t *testing.T) {
	tbl := &schema.Table{
		Name: "_collections",
		Columns: []schema.Column{
			{Name: "id", Type: "TEXT", PK: true, NotNull: true},
			{Name: "name", Type: "TEXT", NotNull: true},
			{Name: "fields", Type: "JSON", NotNull: true},
			{Name: "updated", Type: "TEXT", NotNull: true},
		},
		UniqueConstraints: [][]string{{"name"}},
	}
	cfg := buildDraftConfig(&schema.Schema{Tables: map[string]*schema.Table{"_collections": tbl}})
	if len(cfg) != 0 {
		t.Fatalf("metadata table draft = %#v, want no transforms", cfg)
	}
}

func TestBuildDraftConfigUsesConstraintsAndTypes(t *testing.T) {
	dateDefault := "FALSE"
	tbl := &schema.Table{
		Name: "users",
		Columns: []schema.Column{
			{Name: "email", Type: "TEXT", NotNull: true},
			{Name: "emailVisibility", Type: "BOOLEAN", DefaultSQL: &dateDefault},
			{Name: "ssn", Type: "TEXT", NotNull: true},
			{Name: "password", Type: "TEXT", NotNull: true},
			{Name: "updated_at", Type: "TEXT"},
			{Name: "display_name", Type: "TEXT"},
		},
		UniqueConstraints: [][]string{{"email"}},
	}
	cfg := buildDraftConfig(&schema.Schema{Tables: map[string]*schema.Table{"users": tbl}})
	columns := cfg["tables"].(map[string]any)["users"].(map[string]any)["columns"].(map[string]any)
	if got := columns["email"].(map[string]any)["type"]; got != "HmacSha256" {
		t.Fatalf("email transform = %v, want HmacSha256", got)
	}
	if _, ok := columns["emailVisibility"]; ok {
		t.Fatal("emailVisibility should not be treated as email")
	}
	ssn := columns["ssn"].(map[string]any)
	if ssn["type"] != "SetValue" || ssn["value"] != "redacted" {
		t.Fatalf("ssn transform = %#v, want required redaction", ssn)
	}
	if got := columns["password"].(map[string]any)["type"]; got != "SetValue" {
		t.Fatalf("password transform = %v, want SetValue", got)
	}
	if got := columns["updated_at"].(map[string]any)["type"]; got != "DateShift" {
		t.Fatalf("updated_at transform = %v, want DateShift", got)
	}
	if got := columns["display_name"].(map[string]any)["type"]; got != "FakerName" {
		t.Fatalf("display_name transform = %v, want FakerName", got)
	}
}
