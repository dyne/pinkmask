package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: make_demo_db <output.sqlite>")
		os.Exit(1)
	}
	path := os.Args[1]
	_ = os.Remove(path)
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_busy_timeout=5000", path))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		panic(err)
	}
	stmts := []string{
		`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			full_name TEXT NOT NULL,
			phone TEXT,
			address TEXT,
			ssn TEXT,
			country TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			shipping_address TEXT,
			created_at TEXT NOT NULL,
			FOREIGN KEY(user_id) REFERENCES users(id)
		)`,
		`CREATE TABLE order_items (
			id INTEGER PRIMARY KEY,
			order_id INTEGER NOT NULL,
			sku TEXT NOT NULL,
			qty INTEGER NOT NULL,
			price_cents INTEGER NOT NULL,
			FOREIGN KEY(order_id) REFERENCES orders(id)
		)`,
		`CREATE TABLE addresses (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			label TEXT NOT NULL,
			street TEXT NOT NULL,
			city TEXT NOT NULL,
			state TEXT NOT NULL,
			postal_code TEXT NOT NULL,
			FOREIGN KEY(user_id) REFERENCES users(id)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			panic(err)
		}
	}

	rng := rand.New(rand.NewSource(42))
	countries := []string{"US", "CA", "GB"}
	statuses := []string{"pending", "shipped", "cancelled"}
	for u := 1; u <= 120; u++ {
		country := countries[rng.Intn(len(countries))]
		created := time.Now().AddDate(0, 0, -rng.Intn(365)).Format("2006-01-02")
		_, err := db.Exec(`INSERT INTO users (id, email, full_name, phone, address, ssn, country, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			u, fmt.Sprintf("user%d@example.com", u), fmt.Sprintf("User %d", u), fmt.Sprintf("555-01%03d", u), fmt.Sprintf("%d Main St", u), fmt.Sprintf("000-00-%04d", u), country, created)
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(`INSERT INTO addresses (user_id, label, street, city, state, postal_code) VALUES (?, ?, ?, ?, ?, ?)`,
			u, "home", fmt.Sprintf("%d Main St", u), "Springfield", "CA", fmt.Sprintf("9%04d", u))
		if err != nil {
			panic(err)
		}
		orders := rng.Intn(4)
		for o := 0; o < orders; o++ {
			orderID := u*100 + o
			createdAt := time.Now().AddDate(0, 0, -rng.Intn(100)).Format(time.RFC3339)
			status := statuses[rng.Intn(len(statuses))]
			_, err := db.Exec(`INSERT INTO orders (id, user_id, status, shipping_address, created_at) VALUES (?, ?, ?, ?, ?)`,
				orderID, u, status, fmt.Sprintf("%d Market St", orderID), createdAt)
			if err != nil {
				panic(err)
			}
			items := rng.Intn(3) + 1
			for i := 0; i < items; i++ {
				itemID := orderID*10 + i
				_, err := db.Exec(`INSERT INTO order_items (id, order_id, sku, qty, price_cents) VALUES (?, ?, ?, ?, ?)`,
					itemID, orderID, fmt.Sprintf("SKU-%04d", itemID), rng.Intn(3)+1, rng.Intn(10000)+500)
				if err != nil {
					panic(err)
				}
			}
		}
	}
	fmt.Println("demo db created at", path)
}
