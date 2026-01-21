package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: export_csv <db.sqlite> <table> <output.csv>")
		os.Exit(1)
	}
	dbPath := os.Args[1]
	table := os.Args[2]
	outPath := os.Args[3]

	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", dbPath))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	file, err := os.Create(outPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	w := csv.NewWriter(file)
	if err := w.Write(cols); err != nil {
		panic(err)
	}
	values := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			panic(err)
		}
		row := make([]string, len(cols))
		for i, v := range values {
			row[i] = fmt.Sprint(v)
		}
		if err := w.Write(row); err != nil {
			panic(err)
		}
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		panic(err)
	}
	fmt.Println("csv written to", outPath)
}
