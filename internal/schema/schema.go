package schema

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"sort"
	"strings"
)

type Schema struct {
	Tables   map[string]*Table
	Views    []SQLItem
	Indexes  []SQLItem
	Triggers []SQLItem
}

type SQLItem struct {
	Name string
	SQL  string
	Type string
}

type Table struct {
	Name         string
	SQL          string
	Columns      []Column
	PrimaryKeys  []string
	ForeignKeys  []ForeignKey
	WithoutRowID bool
}

type Column struct {
	Name       string
	Type       string
	NotNull    bool
	DefaultSQL *string
	PK         bool
}

type ForeignKey struct {
	ID       int
	Seq      int
	Table    string
	From     string
	To       string
	OnUpdate string
	OnDelete string
}

func Load(ctx context.Context, db *sql.DB) (*Schema, error) {
	s := &Schema{Tables: map[string]*Table{}}
	rows, err := db.QueryContext(ctx, `SELECT name, type, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("sqlite_master: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, typ string
		var sqlText sql.NullString
		if err := rows.Scan(&name, &typ, &sqlText); err != nil {
			return nil, fmt.Errorf("scan sqlite_master: %w", err)
		}
		item := SQLItem{Name: name, SQL: sqlText.String, Type: typ}
		switch typ {
		case "table":
			if !sqlText.Valid {
				continue
			}
			tbl := &Table{Name: name, SQL: sqlText.String}
			tbl.WithoutRowID = strings.Contains(strings.ToUpper(sqlText.String), "WITHOUT ROWID")
			cols, pkCols, err := loadTableInfo(ctx, db, name)
			if err != nil {
				return nil, err
			}
			fks, err := loadForeignKeys(ctx, db, name)
			if err != nil {
				return nil, err
			}
			tbl.Columns = cols
			tbl.PrimaryKeys = pkCols
			tbl.ForeignKeys = fks
			s.Tables[name] = tbl
		case "index":
			s.Indexes = append(s.Indexes, item)
		case "trigger":
			s.Triggers = append(s.Triggers, item)
		case "view":
			s.Views = append(s.Views, item)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sqlite_master: %w", err)
	}
	return s, nil
}

func loadTableInfo(ctx context.Context, db *sql.DB, table string) ([]Column, []string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", QuoteIdent(table)))
	if err != nil {
		return nil, nil, fmt.Errorf("table_info %s: %w", table, err)
	}
	defer rows.Close()
	var cols []Column
	pkMap := map[int]string{}
	for rows.Next() {
		var cid int
		var name, colType string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk); err != nil {
			return nil, nil, fmt.Errorf("scan table_info %s: %w", table, err)
		}
		col := Column{Name: name, Type: colType, NotNull: notnull == 1, PK: pk > 0}
		if dflt.Valid {
			col.DefaultSQL = &dflt.String
		}
		cols = append(cols, col)
		if pk > 0 {
			pkMap[pk] = name
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate table_info %s: %w", table, err)
	}
	var pkCols []string
	if len(pkMap) > 0 {
		keys := make([]int, 0, len(pkMap))
		for k := range pkMap {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, k := range keys {
			pkCols = append(pkCols, pkMap[k])
		}
	}
	return cols, pkCols, nil
}

func loadForeignKeys(ctx context.Context, db *sql.DB, table string) ([]ForeignKey, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA foreign_key_list(%s)", QuoteIdent(table)))
	if err != nil {
		return nil, fmt.Errorf("foreign_key_list %s: %w", table, err)
	}
	defer rows.Close()
	var fks []ForeignKey
	for rows.Next() {
		var fk ForeignKey
		var id, seq int
		if err := rows.Scan(&id, &seq, &fk.Table, &fk.From, &fk.To, &fk.OnUpdate, &fk.OnDelete, new(string)); err != nil {
			return nil, fmt.Errorf("scan foreign_key_list %s: %w", table, err)
		}
		fk.ID = id
		fk.Seq = seq
		fks = append(fks, fk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate foreign_key_list %s: %w", table, err)
	}
	return fks, nil
}

func QuoteIdent(name string) string {
	escaped := strings.ReplaceAll(name, "\"", "\"\"")
	return "\"" + escaped + "\""
}

func MatchAny(patterns []string, name string) bool {
	if len(patterns) == 0 {
		return false
	}
	for _, p := range patterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}

func TableOrder(s *Schema) []string {
	graph := map[string][]string{}
	indeg := map[string]int{}
	for name := range s.Tables {
		indeg[name] = 0
	}
	for name, tbl := range s.Tables {
		for _, fk := range tbl.ForeignKeys {
			if _, ok := s.Tables[fk.Table]; !ok {
				continue
			}
			graph[fk.Table] = append(graph[fk.Table], name)
			indeg[name]++
		}
	}
	var queue []string
	for name, deg := range indeg {
		if deg == 0 {
			queue = append(queue, name)
		}
	}
	sort.Strings(queue)
	var order []string
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)
		for _, dep := range graph[n] {
			indeg[dep]--
			if indeg[dep] == 0 {
				queue = append(queue, dep)
			}
		}
		sort.Strings(queue)
	}
	if len(order) != len(s.Tables) {
		var missing []string
		for name := range s.Tables {
			found := false
			for _, o := range order {
				if o == name {
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, name)
			}
		}
		sort.Strings(missing)
		order = append(order, missing...)
	}
	return order
}
