package transform

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"
)

type RowContext struct {
	Table string
	PK    []any
	Seed  int64
	Salt  string
}

type Transformer interface {
	Name() string
	Transform(value any, row RowContext) (any, error)
}

type HashSha256 struct {
	salt   string
	maxLen int
}

func NewHashSha256(salt string, maxLen int) *HashSha256 {
	return &HashSha256{salt: salt, maxLen: maxLen}
}

func (t *HashSha256) Name() string { return "HashSha256" }

func (t *HashSha256) Transform(value any, row RowContext) (any, error) {
	if value == nil {
		return nil, nil
	}
	str := fmt.Sprint(value)
	data := []byte(t.salt + str)
	sum := sha256.Sum256(data)
	out := hex.EncodeToString(sum[:])
	if t.maxLen > 0 && t.maxLen < len(out) {
		out = out[:t.maxLen]
	}
	return out, nil
}

type HmacSha256 struct {
	key    []byte
	maxLen int
}

func NewHmacSha256(salt string, maxLen int) *HmacSha256 {
	return &HmacSha256{key: []byte(salt), maxLen: maxLen}
}

func (t *HmacSha256) Name() string { return "HmacSha256" }

func (t *HmacSha256) Transform(value any, row RowContext) (any, error) {
	if value == nil {
		return nil, nil
	}
	mac := hmac.New(sha256.New, t.key)
	_, _ = mac.Write([]byte(fmt.Sprint(value)))
	out := hex.EncodeToString(mac.Sum(nil))
	if t.maxLen > 0 && t.maxLen < len(out) {
		out = out[:t.maxLen]
	}
	return out, nil
}

type StableTokenize struct {
	maxLen int
}

func NewStableTokenize(maxLen int) *StableTokenize {
	return &StableTokenize{maxLen: maxLen}
}

func (t *StableTokenize) Name() string { return "StableTokenize" }

func (t *StableTokenize) Transform(value any, row RowContext) (any, error) {
	if value == nil {
		return nil, nil
	}
	sum := sha256.Sum256([]byte(row.Salt + fmt.Sprint(value)))
	enc := base32.StdEncoding.WithPadding(base32.NoPadding)
	out := strings.ToLower(enc.EncodeToString(sum[:]))
	if t.maxLen > 0 && t.maxLen < len(out) {
		out = out[:t.maxLen]
	} else if t.maxLen == 0 && len(out) > 16 {
		out = out[:16]
	}
	return out, nil
}

type RegexReplace struct {
	re   *regexp.Regexp
	repl string
}

func NewRegexReplace(pattern, repl string) (*RegexReplace, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return &RegexReplace{re: re, repl: repl}, nil
}

func (t *RegexReplace) Name() string { return "RegexReplace" }

func (t *RegexReplace) Transform(value any, row RowContext) (any, error) {
	if value == nil {
		return nil, nil
	}
	return t.re.ReplaceAllString(fmt.Sprint(value), t.repl), nil
}

type SetNull struct{}

func (t *SetNull) Name() string { return "SetNull" }

func (t *SetNull) Transform(value any, row RowContext) (any, error) {
	return nil, nil
}

type SetValue struct{ value any }

func NewSetValue(v any) *SetValue { return &SetValue{value: v} }

func (t *SetValue) Name() string { return "SetValue" }

func (t *SetValue) Transform(value any, row RowContext) (any, error) {
	return t.value, nil
}

type MapReplace struct{ m map[string]string }

func NewMapReplace(m map[string]string) *MapReplace { return &MapReplace{m: m} }

func (t *MapReplace) Name() string { return "MapReplace" }

func (t *MapReplace) Transform(value any, row RowContext) (any, error) {
	if value == nil {
		return nil, nil
	}
	s := fmt.Sprint(value)
	if v, ok := t.m[s]; ok {
		return v, nil
	}
	return s, nil
}

type FakerName struct{}

type FakerEmail struct{}

type FakerAddress struct{}

type FakerPhone struct{}

func (t *FakerName) Name() string    { return "FakerName" }
func (t *FakerEmail) Name() string   { return "FakerEmail" }
func (t *FakerAddress) Name() string { return "FakerAddress" }
func (t *FakerPhone) Name() string   { return "FakerPhone" }

func (t *FakerName) Transform(value any, row RowContext) (any, error) {
	rng := DeterministicRand(row)
	first := firstNames[rng.Intn(len(firstNames))]
	last := lastNames[rng.Intn(len(lastNames))]
	return first + " " + last, nil
}

func (t *FakerEmail) Transform(value any, row RowContext) (any, error) {
	rng := DeterministicRand(row)
	first := strings.ToLower(firstNames[rng.Intn(len(firstNames))])
	last := strings.ToLower(lastNames[rng.Intn(len(lastNames))])
	domain := emailDomains[rng.Intn(len(emailDomains))]
	return fmt.Sprintf("%s.%s@%s", first, last, domain), nil
}

func (t *FakerAddress) Transform(value any, row RowContext) (any, error) {
	rng := DeterministicRand(row)
	num := rng.Intn(8999) + 100
	street := streetNames[rng.Intn(len(streetNames))]
	city := cityNames[rng.Intn(len(cityNames))]
	state := stateCodes[rng.Intn(len(stateCodes))]
	zip := rng.Intn(89999) + 10000
	return fmt.Sprintf("%d %s St, %s, %s %d", num, street, city, state, zip), nil
}

func (t *FakerPhone) Transform(value any, row RowContext) (any, error) {
	rng := DeterministicRand(row)
	area := rng.Intn(800) + 200
	prefix := rng.Intn(800) + 200
	line := rng.Intn(9000) + 1000
	return fmt.Sprintf("%d-%d-%d", area, prefix, line), nil
}

type DateShift struct {
	maxDays int
}

func NewDateShift(maxDays int) *DateShift {
	if maxDays <= 0 {
		maxDays = 30
	}
	return &DateShift{maxDays: maxDays}
}

func (t *DateShift) Name() string { return "DateShift" }

func (t *DateShift) Transform(value any, row RowContext) (any, error) {
	if value == nil {
		return nil, nil
	}
	shiftDays := DeterministicRand(row).Intn(t.maxDays*2+1) - t.maxDays
	shift := time.Duration(shiftDays) * 24 * time.Hour
	switch v := value.(type) {
	case time.Time:
		return v.Add(shift), nil
	case int64:
		return time.Unix(v, 0).Add(shift).Unix(), nil
	case string:
		if ts, err := time.Parse(time.RFC3339, v); err == nil {
			return ts.Add(shift).Format(time.RFC3339), nil
		}
		if ts, err := time.Parse("2006-01-02", v); err == nil {
			return ts.Add(shift).Format("2006-01-02"), nil
		}
		return v, nil
	default:
		return v, nil
	}
}

func DeterministicRand(row RowContext) *rand.Rand {
	seed := RowSeed(row)
	return rand.New(rand.NewSource(seed))
}

func RowSeed(row RowContext) int64 {
	h := sha256.New()
	_, _ = h.Write([]byte(row.Salt))
	_, _ = h.Write([]byte(row.Table))
	for _, v := range row.PK {
		_, _ = h.Write([]byte(fmt.Sprint(v)))
	}
	var seed int64
	for _, b := range h.Sum(nil)[:8] {
		seed = (seed << 8) | int64(b)
	}
	if seed == 0 {
		seed = row.Seed
	}
	return seed ^ row.Seed
}

var firstNames = []string{"Alex", "Jamie", "Taylor", "Jordan", "Morgan", "Casey", "Riley", "Avery", "Parker", "Reese"}
var lastNames = []string{"Smith", "Johnson", "Lee", "Brown", "Davis", "Miller", "Wilson", "Moore", "Clark", "Hall"}
var emailDomains = []string{"example.com", "mail.local", "demo.test", "sample.org"}
var streetNames = []string{"Oak", "Maple", "Cedar", "Pine", "Elm", "Birch", "Willow", "Lake", "Hill", "Sunset"}
var cityNames = []string{"Springfield", "Riverton", "Fairview", "Franklin", "Greenville", "Bristol", "Clinton", "Georgetown"}
var stateCodes = []string{"CA", "NY", "TX", "FL", "WA", "IL", "PA", "AZ"}
