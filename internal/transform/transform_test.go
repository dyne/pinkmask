package transform

import (
	"testing"
)

func TestDeterministicTransforms(t *testing.T) {
	row := RowContext{Table: "users", PK: []any{1}, Seed: 42, Salt: "salt"}
	cases := []Transformer{
		NewHashSha256("salt", 0),
		NewHmacSha256("salt", 0),
		NewStableTokenize(16),
		&FakerName{},
		&FakerEmail{},
		&FakerAddress{},
		&FakerPhone{},
		NewDateShift(7),
	}
	for _, tr := range cases {
		val1, err := tr.Transform("input", row)
		if err != nil {
			t.Fatalf("%s: %v", tr.Name(), err)
		}
		val2, err := tr.Transform("input", row)
		if err != nil {
			t.Fatalf("%s: %v", tr.Name(), err)
		}
		if val1 != val2 {
			t.Fatalf("%s not deterministic: %v vs %v", tr.Name(), val1, val2)
		}
	}
}

func TestRegexReplace(t *testing.T) {
	tr, err := NewRegexReplace("[0-9]+", "X")
	if err != nil {
		t.Fatal(err)
	}
	row := RowContext{Table: "t", PK: []any{1}, Seed: 1, Salt: "s"}
	out, err := tr.Transform("abc123def", row)
	if err != nil {
		t.Fatal(err)
	}
	if out != "abcXdef" {
		t.Fatalf("unexpected output: %v", out)
	}
}
