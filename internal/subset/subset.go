package subset

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/schema"
)

type Selection struct {
	Sets map[string]*PKSet
}

type PKSet struct {
	Cols   []string
	Keys   map[string]struct{}
	Values [][]any
}

func NewPKSet(cols []string) *PKSet {
	return &PKSet{Cols: cols, Keys: map[string]struct{}{}}
}

func (s *PKSet) Add(values []any) bool {
	key := keyFor(values)
	if _, ok := s.Keys[key]; ok {
		return false
	}
	s.Keys[key] = struct{}{}
	row := make([]any, len(values))
	copy(row, values)
	s.Values = append(s.Values, row)
	return true
}

func (s *PKSet) Len() int {
	return len(s.Values)
}

func (s *PKSet) ValuesByColumns(cols []string) ([][]any, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns requested")
	}
	if len(cols) == len(s.Cols) {
		match := true
		for i, c := range cols {
			if s.Cols[i] != c {
				match = false
				break
			}
		}
		if match {
			return s.Values, nil
		}
	}
	idx := make([]int, len(cols))
	colIndex := map[string]int{}
	for i, c := range s.Cols {
		colIndex[c] = i
	}
	for i, c := range cols {
		pos, ok := colIndex[c]
		if !ok {
			return nil, fmt.Errorf("missing column %s", c)
		}
		idx[i] = pos
	}
	out := make([][]any, 0, len(s.Values))
	for _, row := range s.Values {
		vals := make([]any, len(cols))
		for i, pos := range idx {
			vals[i] = row[pos]
		}
		out = append(out, vals)
	}
	return out, nil
}

func BuildSelection(ctx context.Context, db *sql.DB, s *schema.Schema, cfg *config.Config) (*Selection, error) {
	selection := &Selection{Sets: map[string]*PKSet{}}
	if cfg == nil || cfg.Subset == nil {
		return selection, nil
	}
	if len(cfg.Subset.Roots) == 0 {
		return selection, nil
	}
	for _, root := range cfg.Subset.Roots {
		if root.Table == "" {
			continue
		}
		tbl := s.Tables[root.Table]
		if tbl == nil {
			return nil, fmt.Errorf("subset root table not found: %s", root.Table)
		}
		pkCols, useRowID, err := tablePKColumns(tbl)
		if err != nil {
			return nil, err
		}
		set := selection.Sets[root.Table]
		if set == nil {
			set = NewPKSet(pkCols)
			selection.Sets[root.Table] = set
		}
		cols := make([]string, 0, len(pkCols))
		for _, c := range pkCols {
			if c == "rowid" && useRowID {
				cols = append(cols, "rowid")
			} else {
				cols = append(cols, schema.QuoteIdent(c))
			}
		}
		query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ", "), schema.QuoteIdent(root.Table))
		if root.Where != "" {
			query += " WHERE " + root.Where
		}
		if len(pkCols) > 0 {
			query += " ORDER BY " + strings.Join(quotedCols(pkCols, useRowID), ", ")
		}
		if root.Limit > 0 {
			query += fmt.Sprintf(" LIMIT %d", root.Limit)
		}
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("subset root query %s: %w", root.Table, err)
		}
		for rows.Next() {
			vals := make([]any, len(pkCols))
			ptrs := make([]any, len(pkCols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return nil, fmt.Errorf("subset root scan %s: %w", root.Table, err)
			}
			set.Add(vals)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("subset root iterate %s: %w", root.Table, err)
		}
		rows.Close()
	}
	if err := expandSelection(ctx, db, s, selection); err != nil {
		return nil, err
	}
	return selection, nil
}

func expandSelection(ctx context.Context, db *sql.DB, s *schema.Schema, selection *Selection) error {
	fkGroups := map[string][]FKGroup{}
	for name, tbl := range s.Tables {
		fkGroups[name] = groupFKs(tbl)
	}
	tableNames := make([]string, 0, len(s.Tables))
	for name := range s.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	changed := true
	for changed {
		changed = false
		for _, childName := range tableNames {
			groups := fkGroups[childName]
			childSet := selection.Sets[childName]
			childTbl := s.Tables[childName]
			for _, fk := range groups {
				parentTbl := s.Tables[fk.RefTable]
				if parentTbl == nil {
					continue
				}
				parentSet := selection.Sets[fk.RefTable]
				if childSet != nil && childSet.Len() > 0 {
					refVals, err := selectFKValues(ctx, db, childTbl, fk, childSet)
					if err != nil {
						return err
					}
					if len(refVals) > 0 {
						added, err := addParentKeys(ctx, db, parentTbl, fk, refVals, selection)
						if err != nil {
							return err
						}
						if added {
							changed = true
						}
					}
				}
				if parentSet != nil && parentSet.Len() > 0 {
					added, err := addChildKeys(ctx, db, childTbl, fk, parentSet, selection)
					if err != nil {
						return err
					}
					if added {
						changed = true
					}
				}
			}
		}
	}
	return nil
}

type FKGroup struct {
	RefTable string
	FromCols []string
	ToCols   []string
}

func groupFKs(tbl *schema.Table) []FKGroup {
	byID := map[int]*FKGroup{}
	order := make([]int, 0)
	for _, fk := range tbl.ForeignKeys {
		group, ok := byID[fk.ID]
		if !ok {
			group = &FKGroup{RefTable: fk.Table}
			byID[fk.ID] = group
			order = append(order, fk.ID)
		}
		group.FromCols = append(group.FromCols, fk.From)
		group.ToCols = append(group.ToCols, fk.To)
	}
	sort.Ints(order)
	var out []FKGroup
	for _, id := range order {
		out = append(out, *byID[id])
	}
	return out
}

func selectFKValues(ctx context.Context, db *sql.DB, childTbl *schema.Table, fk FKGroup, childSet *PKSet) ([][]any, error) {
	pkCols, useRowID, err := tablePKColumns(childTbl)
	if err != nil {
		return nil, err
	}
	childPKVals, err := childSet.ValuesByColumns(pkCols)
	if err != nil {
		return nil, err
	}
	if len(childPKVals) == 0 {
		return nil, nil
	}
	var results [][]any
	chunks := chunkValues(childPKVals, 500)
	for _, chunk := range chunks {
		whereIn, args := buildTupleIn(pkCols, chunk, useRowID)
		query := fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s", strings.Join(quotedCols(fk.FromCols, false), ", "), schema.QuoteIdent(childTbl.Name), whereIn)
		query += notNullClause(fk.FromCols)
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("subset select fk values %s: %w", childTbl.Name, err)
		}
		for rows.Next() {
			vals := make([]any, len(fk.FromCols))
			ptrs := make([]any, len(fk.FromCols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return nil, fmt.Errorf("subset scan fk values %s: %w", childTbl.Name, err)
			}
			results = append(results, vals)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("subset iterate fk values %s: %w", childTbl.Name, err)
		}
		rows.Close()
	}
	return results, nil
}

func addParentKeys(ctx context.Context, db *sql.DB, parentTbl *schema.Table, fk FKGroup, refVals [][]any, sel *Selection) (bool, error) {
	if len(refVals) == 0 {
		return false, nil
	}
	parentSet := sel.Sets[fk.RefTable]
	if parentSet == nil {
		pkCols, _, err := tablePKColumns(parentTbl)
		if err != nil {
			return false, err
		}
		parentSet = NewPKSet(pkCols)
		sel.Sets[fk.RefTable] = parentSet
	}
	if sameColumnOrder(parentSet.Cols, fk.ToCols) {
		added := false
		for _, row := range refVals {
			if parentSet.Add(row) {
				added = true
			}
		}
		return added, nil
	}
	return selectParentPKs(ctx, db, parentTbl, fk, refVals, parentSet)
}

func selectParentPKs(ctx context.Context, db *sql.DB, parentTbl *schema.Table, fk FKGroup, refVals [][]any, parentSet *PKSet) (bool, error) {
	pkCols, useRowID, err := tablePKColumns(parentTbl)
	if err != nil {
		return false, err
	}
	chunks := chunkValues(refVals, 500)
	added := false
	for _, chunk := range chunks {
		whereIn, args := buildTupleIn(fk.ToCols, chunk, false)
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(quotedCols(pkCols, useRowID), ", "), schema.QuoteIdent(parentTbl.Name), whereIn)
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return false, fmt.Errorf("subset select parent %s: %w", parentTbl.Name, err)
		}
		for rows.Next() {
			vals := make([]any, len(pkCols))
			ptrs := make([]any, len(pkCols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return false, fmt.Errorf("subset scan parent %s: %w", parentTbl.Name, err)
			}
			if parentSet.Add(vals) {
				added = true
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return false, fmt.Errorf("subset iterate parent %s: %w", parentTbl.Name, err)
		}
		rows.Close()
	}
	return added, nil
}

func addChildKeys(ctx context.Context, db *sql.DB, childTbl *schema.Table, fk FKGroup, parentSet *PKSet, sel *Selection) (bool, error) {
	childSet := sel.Sets[childTbl.Name]
	pkCols, useRowID, err := tablePKColumns(childTbl)
	if err != nil {
		return false, err
	}
	if childSet == nil {
		childSet = NewPKSet(pkCols)
		sel.Sets[childTbl.Name] = childSet
	}
	parentVals, err := parentSet.ValuesByColumns(fk.ToCols)
	if err != nil {
		return false, err
	}
	if len(parentVals) == 0 {
		return false, nil
	}
	chunks := chunkValues(parentVals, 500)
	added := false
	for _, chunk := range chunks {
		whereIn, args := buildTupleIn(fk.FromCols, chunk, false)
		query := fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s", strings.Join(quotedCols(pkCols, useRowID), ", "), schema.QuoteIdent(childTbl.Name), whereIn)
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return false, fmt.Errorf("subset select child %s: %w", childTbl.Name, err)
		}
		for rows.Next() {
			vals := make([]any, len(pkCols))
			ptrs := make([]any, len(pkCols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return false, fmt.Errorf("subset scan child %s: %w", childTbl.Name, err)
			}
			if childSet.Add(vals) {
				added = true
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return false, fmt.Errorf("subset iterate child %s: %w", childTbl.Name, err)
		}
		rows.Close()
	}
	return added, nil
}

func tablePKColumns(tbl *schema.Table) ([]string, bool, error) {
	if len(tbl.PrimaryKeys) > 0 {
		return tbl.PrimaryKeys, false, nil
	}
	if tbl.WithoutRowID {
		return nil, false, fmt.Errorf("table %s has no primary key", tbl.Name)
	}
	return []string{"rowid"}, true, nil
}

func quotedCols(cols []string, useRowID bool) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if c == "rowid" && useRowID {
			out = append(out, "rowid")
			continue
		}
		out = append(out, schema.QuoteIdent(c))
	}
	return out
}

func buildTupleIn(cols []string, values [][]any, useRowID bool) (string, []any) {
	if len(cols) == 1 {
		place := make([]string, 0, len(values))
		args := make([]any, 0, len(values))
		col := cols[0]
		if col == "rowid" && useRowID {
			col = "rowid"
		} else {
			col = schema.QuoteIdent(col)
		}
		for _, v := range values {
			place = append(place, "?")
			args = append(args, v[0])
		}
		return fmt.Sprintf("%s IN (%s)", col, strings.Join(place, ", ")), args
	}
	var builder strings.Builder
	builder.WriteString("(")
	builder.WriteString(strings.Join(quotedCols(cols, useRowID), ", "))
	builder.WriteString(") IN (")
	args := make([]any, 0, len(values)*len(cols))
	for i, row := range values {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString("(")
		for j := range cols {
			if j > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString("?")
			args = append(args, row[j])
		}
		builder.WriteString(")")
	}
	builder.WriteString(")")
	return builder.String(), args
}

func notNullClause(cols []string) string {
	clauses := make([]string, 0, len(cols))
	for _, c := range cols {
		clauses = append(clauses, fmt.Sprintf("%s IS NOT NULL", schema.QuoteIdent(c)))
	}
	return " AND " + strings.Join(clauses, " AND ")
}

func chunkValues(values [][]any, size int) [][][]any {
	if size <= 0 || len(values) <= size {
		return [][][]any{values}
	}
	var chunks [][][]any
	for i := 0; i < len(values); i += size {
		end := i + size
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[i:end])
	}
	return chunks
}

func keyFor(values []any) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, "|")
}

func sameColumnOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
