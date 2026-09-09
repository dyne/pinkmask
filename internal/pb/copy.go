package pb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/log"
	"github.com/dyne/pinkmask/internal/schema"
	"github.com/dyne/pinkmask/internal/transform"
)

// Options configures the PocketBase instance-to-instance masking run.
type Options struct {
	SourceURL      string
	SourceEmail    string
	SourcePassword string
	DestURL        string
	DestEmail      string
	DestPassword   string
	Config         *config.Config
	Salt           string
	Seed           int64
	BatchSize      int
	Logger         *log.Logger
}

// Run authenticates to both instances, mirrors non-system collections to the
// destination, and copies every record with mask.yml transforms applied.
func Run(ctx context.Context, opts Options) error {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 25
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.New(log.LevelInfo, io.Discard)
	}

	src := New(opts.SourceURL)
	dst := New(opts.DestURL)
	logger.Infof("logging into source %s", opts.SourceURL)
	if err := src.Login(ctx, opts.SourceEmail, opts.SourcePassword); err != nil {
		return err
	}
	logger.Infof("logging into destination %s", opts.DestURL)
	if err := dst.Login(ctx, opts.DestEmail, opts.DestPassword); err != nil {
		return err
	}

	srcCols, err := src.ListCollections(ctx)
	if err != nil {
		return fmt.Errorf("list source collections: %w", err)
	}

	userCols := make([]*Collection, 0, len(srcCols))
	for _, c := range srcCols {
		if c.System || c.Name == "_superusers" || c.Type == "view" || !tableIncluded(opts.Config, c.Name) {
			continue
		}
		userCols = append(userCols, c)
	}
	ordered := sortCollections(userCols)

	dstCols, err := dst.ListCollections(ctx)
	if err != nil {
		return fmt.Errorf("list destination collections: %w", err)
	}
	dstNames := make(map[string]bool, len(dstCols))
	dstByName := make(map[string]*Collection, len(dstCols))
	for _, c := range dstCols {
		dstNames[c.Name] = true
		dstByName[c.Name] = c
	}
	relationIDs := make(map[string]string, len(ordered))
	for _, col := range ordered {
		if existing := dstByName[col.Name]; existing != nil {
			relationIDs[col.ID] = existing.ID
		}
	}
	for _, col := range ordered {
		if dstNames[col.Name] {
			logger.Debugf("collection %s already exists on destination", col.Name)
			continue
		}
		logger.Infof("creating collection %s on destination", col.Name)
		created, err := dst.CreateCollection(ctx, col, relationIDs)
		if err != nil {
			return fmt.Errorf("create collection %s: %w", col.Name, err)
		}
		dstNames[col.Name] = true
		relationIDs[col.ID] = created.ID
	}

	for _, col := range ordered {
		if err := copyCollection(ctx, src, dst, col, opts); err != nil {
			return fmt.Errorf("copy collection %s: %w", col.Name, err)
		}
	}
	logger.Infof("done: %d collections migrated", len(ordered))
	return nil
}

// sortCollections orders collections so relation targets come first.
func sortCollections(cols []*Collection) []*Collection {
	byName := make(map[string]*Collection, len(cols))
	for _, c := range cols {
		byName[c.Name] = c
	}
	deps := make(map[string]map[string]bool, len(cols))
	for _, c := range cols {
		d := map[string]bool{}
		fields, _ := c.Fields()
		for _, f := range fields {
			if f.Type != "relation" {
				continue
			}
			if id := relationTargetID(f.Raw); id != "" {
				for _, other := range cols {
					if other.ID == id {
						d[other.Name] = true
					}
				}
			}
		}
		deps[c.Name] = d
	}
	visited := map[string]bool{}
	var out []*Collection
	var visit func(string)
	visit = func(name string) {
		if visited[name] {
			return
		}
		visited[name] = true
		col, ok := byName[name]
		if !ok {
			return
		}
		for dep := range deps[name] {
			visit(dep)
		}
		out = append(out, col)
	}
	for _, c := range cols {
		visit(c.Name)
	}
	return out
}

func relationTargetID(raw map[string]any) string {
	if id, ok := raw["collectionId"].(string); ok {
		return id
	}
	if options, ok := raw["options"].(map[string]any); ok {
		if id, ok := options["collectionId"].(string); ok {
			return id
		}
	}
	return ""
}

func copyCollection(ctx context.Context, src, dst *Client, col *Collection, opts Options) error {
	logger := opts.Logger

	fields, err := col.Fields()
	if err != nil {
		return err
	}
	fileFields := map[string][]string{} // field name -> maxSelect>1 marker via len? use presence
	fileSingle := map[string]bool{}
	fileMulti := map[string]bool{}
	auth := col.Type == "auth"
	for _, f := range fields {
		if f.System {
			continue
		}
		if f.Type == "file" {
			if maxSel, ok := f.Raw["maxSelect"].(float64); ok && maxSel > 1 {
				fileMulti[f.Name] = true
			} else {
				fileSingle[f.Name] = true
			}
			fileFields[f.Name] = nil
		}
	}

	// Build transformers from mask.yml. Map with lookup_table is SQLite-only
	// and is rejected up front with a clear error.
	if opts.Config != nil {
		if tc := opts.Config.Tables[col.Name]; tc != nil {
			for _, tcc := range tc.Columns {
				if tcc.Type != "" && strings.EqualFold(tcc.Type, "map") && tcc.LookupTable != "" {
					return fmt.Errorf("transformer Map with lookup_table is not supported in pb mode (column %s of %s)", col.Name, col.Name)
				}
			}
		}
	}
	transformers, err := buildTransformers(opts.Config, col.Name, opts.Salt)
	if err != nil {
		return err
	}
	total := 0
	for page := 1; ; page++ {
		recs, err := src.ListRecordsPage(ctx, col.Name, page)
		if err != nil {
			return err
		}
		if len(recs.Items) == 0 {
			break
		}
		for i := 0; i < len(recs.Items); i += opts.BatchSize {
			end := i + opts.BatchSize
			if end > len(recs.Items) {
				end = len(recs.Items)
			}
			batch := recs.Items[i:end]
			if err := importBatch(ctx, dst, col, batch, transformers, fileFields, fileSingle, fileMulti, auth, opts); err != nil {
				return err
			}
			total += len(batch)
		}
		if page >= recs.TotalPages {
			break
		}
	}
	logger.Infof("collection %s: %d records copied", col.Name, total)
	return nil
}

func importBatch(
	ctx context.Context,
	dst *Client,
	col *Collection,
	records []map[string]any,
	transformers map[string]transform.Transformer,
	fileFields map[string][]string,
	fileSingle, fileMulti map[string]bool,
	auth bool,
	opts Options,
) error {
	for _, rec := range records {
		if err := maskRecord(rec, col, transformers, fileFields, fileSingle, fileMulti, auth, opts); err != nil {
			return err
		}
	}

	// Records carrying files need multipart uploads; the rest go through the
	// batch endpoint when available.
	withFiles := false
	for _, rec := range records {
		for name := range fileSingle {
			if v, _ := rec[name].(string); v != "" {
				withFiles = true
			}
		}
		if len(fileMulti) > 0 {
			for name := range fileMulti {
				if arr, ok := rec[name].([]any); ok && len(arr) > 0 {
					withFiles = true
				}
			}
		}
	}
	if !withFiles {
		err := dst.Batch(ctx, col.Name, records)
		if err == nil {
			return nil
		}
		// Older PocketBase has no /api/batch — fall through to per-record.
		_ = err
	}
	for _, rec := range records {
		uploads := map[string][]string{}
		for name := range fileFields {
			switch {
			case fileSingle[name]:
				if v, _ := rec[name].(string); v != "" {
					uploads[name] = []string{v}
				}
			case fileMulti[name]:
				if arr, ok := rec[name].([]any); ok {
					names := make([]string, 0, len(arr))
					for _, item := range arr {
						if s, ok := item.(string); ok && s != "" {
							names = append(names, s)
						}
					}
					if len(names) > 0 {
						uploads[name] = names
					}
				}
			}
		}
		if err := dst.CreateRecordMultipart(ctx, col.Name, rec, uploads); err != nil {
			// PocketBase rejects unknown placeholder filenames; retry once with
			// file fields cleared.
			if len(uploads) > 0 {
				r2 := cloneRecord(rec)
				for name := range uploads {
					r2[name] = nil
				}
				if err2 := dst.CreateRecord(ctx, col.Name, r2); err2 == nil {
					continue
				}
			}
			return err
		}
	}
	return nil
}

// maskRecord applies transformers, fake file names, and auth password
// randomization to a record in place.
func maskRecord(
	rec map[string]any,
	col *Collection,
	transformers map[string]transform.Transformer,
	fileFields map[string][]string,
	fileSingle, fileMulti map[string]bool,
	auth bool,
	opts Options,
) error {
	id, _ := rec["id"].(string)
	for name, tr := range transformers {
		row := transform.RowContext{Table: col.Name, PK: []any{id}, Seed: opts.Seed, Salt: opts.Salt}
		out, err := tr.Transform(rec[name], row)
		if err != nil {
			return fmt.Errorf("mask %s.%s: %w", col.Name, name, err)
		}
		rec[name] = out
	}

	// File fields: replace real filenames with deterministic fake ones.
	for name := range fileFields {
		switch {
		case fileSingle[name]:
			if v, _ := rec[name].(string); v != "" {
				rec[name] = fakeFileName(v, id, name, opts.Salt, opts.Seed)
			}
		case fileMulti[name]:
			arr, ok := rec[name].([]any)
			if !ok {
				continue
			}
			for i, item := range arr {
				if s, ok := item.(string); ok && s != "" {
					arr[i] = fakeFileName(s, id, name, opts.Salt, opts.Seed)
				}
			}
		}
	}

	// Auth records: randomized password so masked accounts cannot be logged
	// into, while still satisfying PocketBase validation.
	if auth {
		pw := deterministicPassword(id, opts.Salt, opts.Seed)
		rec["password"] = pw
		rec["passwordConfirm"] = pw
		rec["verified"] = false
	}
	return nil
}

func fakeFileName(original, id, field, salt string, seed int64) string {
	ext := filepath.Ext(original)
	h := sha256.Sum256([]byte(salt + fmt.Sprintf("%d", seed) + id + field + original))
	return "pinkmask_" + hex.EncodeToString(h[:8]) + ext
}

func deterministicPassword(id, salt string, seed int64) string {
	h := sha256.Sum256([]byte(salt + fmt.Sprintf("%d", seed) + "pw" + id))
	return hex.EncodeToString(h[:12]) // 24 hex chars
}

func cloneRecord(rec map[string]any) map[string]any {
	out := make(map[string]any, len(rec))
	for k, v := range rec {
		out[k] = v
	}
	return out
}

func buildTransformers(cfg *config.Config, table, salt string) (map[string]transform.Transformer, error) {
	if cfg == nil {
		return nil, nil
	}
	tc, ok := cfg.Tables[table]
	if !ok || tc == nil {
		return nil, nil
	}
	out := make(map[string]transform.Transformer, len(tc.Columns))
	for colName, colCfg := range tc.Columns {
		tr, err := transform.Build(colCfg, salt)
		if err != nil {
			return nil, fmt.Errorf("transformer for %s.%s: %w", table, colName, err)
		}
		if tr != nil {
			out[colName] = tr
		}
	}
	return out, nil
}

func tableIncluded(cfg *config.Config, name string) bool {
	if cfg == nil {
		return true
	}
	if len(cfg.IncludeTables) > 0 && !schema.MatchAny(cfg.IncludeTables, name) {
		return false
	}
	return !schema.MatchAny(cfg.ExcludeTables, name)
}
