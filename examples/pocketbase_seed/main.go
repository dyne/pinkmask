package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"
)

type collection struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type seeder struct {
	base  string
	token string
	http  *http.Client
}

func main() {
	url := flag.String("url", "http://pb-source:8090", "PocketBase URL")
	email := flag.String("email", "admin@example.com", "superuser email")
	password := flag.String("password", "pinkmask-admin-password", "superuser password")
	flag.Parse()

	s := &seeder{base: strings.TrimRight(*url, "/"), http: &http.Client{Timeout: 15 * time.Second}}
	ctx := context.Background()
	if err := s.wait(ctx); err != nil {
		fatal(err)
	}
	if err := s.login(ctx, *email, *password); err != nil {
		fatal(err)
	}
	if err := s.seed(ctx); err != nil {
		fatal(err)
	}
	fmt.Println("PocketBase fixture is ready")
}

func (s *seeder) wait(ctx context.Context) error {
	for range 60 {
		resp, err := s.http.Get(s.base + "/api/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 400 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("PocketBase did not become healthy at %s", s.base)
}

func (s *seeder) login(ctx context.Context, email, password string) error {
	body, _ := json.Marshal(map[string]string{"identity": email, "password": password})
	for _, path := range []string{"/api/collections/_superusers/auth-with-password", "/api/admins/auth-with-password"} {
		data, err := s.request(ctx, http.MethodPost, path, "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		var result struct {
			Token string `json:"token"`
		}
		if json.Unmarshal(data, &result) == nil && result.Token != "" {
			s.token = result.Token
			return nil
		}
	}
	return fmt.Errorf("superuser login failed")
}

func (s *seeder) seed(ctx context.Context) error {
	collections, err := s.collections(ctx)
	if err != nil {
		return err
	}
	byName := make(map[string]collection, len(collections))
	for _, c := range collections {
		byName[c.Name] = c
	}

	customers, err := s.ensureCollection(ctx, byName, collectionDefinition{
		Name: "customers",
		Type: "base",
		Fields: []map[string]any{
			{"name": "full_name", "type": "text", "required": true},
			{"name": "email", "type": "email", "required": true, "unique": true},
			{"name": "phone", "type": "text"},
			{"name": "address", "type": "text"},
			{"name": "ssn", "type": "text"},
			{"name": "avatar", "type": "file", "options": map[string]any{"maxSelect": 1, "maxSize": 5242880, "mimeTypes": []string{"image/jpeg"}}},
		},
	})
	if err != nil {
		return err
	}
	if _, err := s.ensureCollection(ctx, byName, collectionDefinition{
		Name: "orders",
		Type: "base",
		Fields: []map[string]any{
			{"name": "customer", "type": "relation", "collectionId": customers.ID, "maxSelect": 1, "options": map[string]any{"collectionId": customers.ID, "maxSelect": 1}},
			{"name": "shipping_address", "type": "text"},
			{"name": "status", "type": "select", "maxSelect": 1, "values": []string{"pending", "shipped", "cancelled"}, "options": map[string]any{"maxSelect": 1, "values": []string{"pending", "shipped", "cancelled"}}},
			{"name": "sku", "type": "text"},
		},
	}); err != nil {
		return err
	}
	if _, err := s.ensureCollection(ctx, byName, collectionDefinition{
		Name: "members",
		Type: "auth",
		Fields: []map[string]any{
			{"name": "display_name", "type": "text"},
			{"name": "phone", "type": "text"},
		},
	}); err != nil {
		return err
	}

	customersExist, err := s.hasRecords(ctx, "customers")
	if err != nil {
		return err
	}
	if !customersExist {
		if err := s.createMultipart(ctx, "customers", map[string]string{
			"id": "cust00000000001", "full_name": "Alice Example", "email": "alice@example.test",
			"phone": "+1 415 555 0101", "address": "1 Market Street, San Francisco", "ssn": "111-22-3333",
		}, "avatar", "alice.jpg", []byte("fixture placeholder image")); err != nil {
			return err
		}
		for _, record := range []map[string]any{
			{"id": "cust00000000002", "full_name": "Bob Example", "email": "bob@example.test", "phone": "+1 212 555 0102", "address": "2 Madison Avenue, New York", "ssn": "222-33-4444"},
			{"id": "cust00000000003", "full_name": "Carol Example", "email": "carol@example.test", "phone": "+44 20 7946 0103", "address": "3 King Street, London", "ssn": "333-44-5555"},
		} {
			if err := s.create(ctx, "customers", record); err != nil {
				return err
			}
		}
	}
	ordersExist, err := s.hasRecords(ctx, "orders")
	if err != nil {
		return err
	}
	if !ordersExist {
		for _, record := range []map[string]any{
			{"id": "ordr00000000001", "customer": "cust00000000001", "shipping_address": "1 Market Street, San Francisco", "status": "pending", "sku": "SKU-RED-001"},
			{"id": "ordr00000000002", "customer": "cust00000000002", "shipping_address": "2 Madison Avenue, New York", "status": "shipped", "sku": "SKU-BLU-002"},
			{"id": "ordr00000000003", "customer": "cust00000000001", "shipping_address": "1 Market Street, San Francisco", "status": "cancelled", "sku": "SKU-GRN-003"},
		} {
			if err := s.create(ctx, "orders", record); err != nil {
				return err
			}
		}
	}
	membersExist, err := s.hasRecords(ctx, "members")
	if err != nil {
		return err
	}
	if !membersExist {
		for _, record := range []map[string]any{
			{"id": "member000000001", "email": "member.one@example.test", "password": "SourcePassword123!", "passwordConfirm": "SourcePassword123!", "display_name": "Member One", "phone": "+1 202 555 0104"},
			{"id": "member000000002", "email": "member.two@example.test", "password": "SourcePassword123!", "passwordConfirm": "SourcePassword123!", "display_name": "Member Two", "phone": "+1 202 555 0105"},
		} {
			if err := s.create(ctx, "members", record); err != nil {
				return err
			}
		}
	}
	return nil
}

type collectionDefinition struct {
	Name   string
	Type   string
	Fields []map[string]any
}

func (s *seeder) ensureCollection(ctx context.Context, existing map[string]collection, definition collectionDefinition) (collection, error) {
	if c, ok := existing[definition.Name]; ok {
		return c, nil
	}
	payload := map[string]any{"name": definition.Name, "type": definition.Type, "fields": definition.Fields}
	data, err := s.jsonRequest(ctx, http.MethodPost, "/api/collections", payload)
	if err != nil {
		return collection{}, fmt.Errorf("create collection %s: %w", definition.Name, err)
	}
	var c collection
	if err := json.Unmarshal(data, &c); err != nil {
		return collection{}, err
	}
	if c.ID == "" {
		return collection{}, fmt.Errorf("collection %s response has no ID", definition.Name)
	}
	existing[definition.Name] = c
	return c, nil
}

func (s *seeder) collections(ctx context.Context) ([]collection, error) {
	data, err := s.request(ctx, http.MethodGet, "/api/collections?perPage=200", "", nil)
	if err != nil {
		return nil, err
	}
	var response struct {
		Items []collection `json:"items"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func (s *seeder) hasRecords(ctx context.Context, name string) (bool, error) {
	data, err := s.request(ctx, http.MethodGet, "/api/collections/"+name+"/records?perPage=1", "", nil)
	if err != nil {
		return false, err
	}
	var response struct {
		TotalItems int `json:"totalItems"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return false, err
	}
	return response.TotalItems > 0, nil
}

func (s *seeder) create(ctx context.Context, collection string, record map[string]any) error {
	_, err := s.jsonRequest(ctx, http.MethodPost, "/api/collections/"+collection+"/records", record)
	return err
}

func (s *seeder) createMultipart(ctx context.Context, collection string, fields map[string]string, fileField, fileName string, content []byte) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			return err
		}
	}
	file, err := writer.CreateFormFile(fileField, fileName)
	if err != nil {
		return err
	}
	if _, err := file.Write(content); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	_, err = s.request(ctx, http.MethodPost, "/api/collections/"+collection+"/records", writer.FormDataContentType(), &body)
	return err
}

func (s *seeder) jsonRequest(ctx context.Context, method, path string, value any) ([]byte, error) {
	body, _ := json.Marshal(value)
	return s.request(ctx, method, path, "application/json", bytes.NewReader(body))
}

func (s *seeder) request(ctx context.Context, method, path, contentType string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if s.token != "" {
		req.Header.Set("Authorization", s.token)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, string(data))
	}
	return data, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
