// Package pb provides a minimal PocketBase superuser API client used by the
// pinkmask PocketBase-to-PocketBase masking flow.
package pb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

// Client talks to one PocketBase instance as a superuser.
type Client struct {
	BaseURL string
	HTTP    *http.Client
	Token   string
	legacy  bool // true when only the pre-0.23 /api/admins endpoints work
}

// New creates a client for the given PocketBase base URL.
func New(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 60 * time.Second},
	}
}

// Login authenticates as a superuser (PocketBase >= 0.23) and falls back to
// the legacy admin endpoint for older versions.
func (c *Client) Login(ctx context.Context, identity, password string) error {
	endpoints := []string{
		"/api/collections/_superusers/auth-with-password",
		"/api/admins/auth-with-password",
	}
	var lastErr error
	for _, ep := range endpoints {
		body, _ := json.Marshal(map[string]string{"identity": identity, "password": password})
		data, err := c.raw(ctx, http.MethodPost, ep, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		var out struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal(data, &out); err != nil || out.Token == "" {
			lastErr = fmt.Errorf("auth response missing token: %w", err)
			continue
		}
		c.Token = out.Token
		c.legacy = ep == endpoints[1]
		return nil
	}
	return fmt.Errorf("superuser login failed for %s: %w", c.BaseURL, lastErr)
}

func (c *Client) do(ctx context.Context, method, path, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) raw(ctx context.Context, method, path, contentType string, body io.Reader) ([]byte, error) {
	resp, err := c.do(ctx, method, path, contentType, body)
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

// Collection is a subset of the PocketBase collection JSON we care about.
type Collection struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	System     bool            `json:"system"`
	FieldsJSON json.RawMessage `json:"fields,omitempty"` // PocketBase >= 0.23
	Schema     json.RawMessage `json:"schema,omitempty"` // PocketBase <= 0.22
	Raw        map[string]any  `json:"-"`
}

// Field is one collection field definition (works for both "fields" and "schema").
type Field struct {
	Name   string         `json:"name"`
	Type   string         `json:"type"`
	System bool           `json:"system"`
	Raw    map[string]any `json:"-"`
}

// Fields returns the field definitions regardless of API version.
func (col *Collection) Fields() ([]Field, error) {
	src := col.FieldsJSON
	if len(src) == 0 {
		src = col.Schema
	}
	if len(src) == 0 {
		return nil, nil
	}
	var rawFields []json.RawMessage
	if err := json.Unmarshal(src, &rawFields); err != nil {
		return nil, fmt.Errorf("parse fields of %s: %w", col.Name, err)
	}
	fields := make([]Field, 0, len(rawFields))
	for _, raw := range rawFields {
		var field Field
		if err := json.Unmarshal(raw, &field); err != nil {
			return nil, fmt.Errorf("parse field of %s: %w", col.Name, err)
		}
		field.Raw = map[string]any{}
		if err := json.Unmarshal(raw, &field.Raw); err != nil {
			return nil, fmt.Errorf("parse field metadata of %s: %w", col.Name, err)
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// ListCollections fetches all collections (superuser only).
func (c *Client) ListCollections(ctx context.Context) ([]*Collection, error) {
	var all []*Collection
	for page := 1; ; page++ {
		data, err := c.raw(ctx, http.MethodGet,
			fmt.Sprintf("/api/collections?page=%d&perPage=200", page), "", nil)
		if err != nil {
			// Older versions require admin scope for listing too; retry legacy path.
			data, err = c.raw(ctx, http.MethodGet,
				fmt.Sprintf("/api/admins/collections?page=%d&perPage=200", page), "", nil)
			if err != nil {
				return nil, err
			}
		}
		var out struct {
			Items      []json.RawMessage `json:"items"`
			TotalPages int               `json:"totalPages"`
		}
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, err
		}
		for _, rawItem := range out.Items {
			var col Collection
			if err := json.Unmarshal(rawItem, &col); err != nil {
				return nil, err
			}
			col.Raw = map[string]any{}
			if err := json.Unmarshal(rawItem, &col.Raw); err != nil {
				return nil, err
			}
			all = append(all, &col)
		}
		if page >= out.TotalPages || len(out.Items) == 0 {
			break
		}
	}
	return all, nil
}

// CreateCollection creates a collection and rewrites relation targets from
// source collection IDs to destination collection IDs.
func (c *Client) CreateCollection(ctx context.Context, src *Collection, relationIDs map[string]string) (*Collection, error) {
	payload := map[string]any{}
	for _, k := range []string{"name", "type", "fields", "schema", "indexes",
		"listRule", "viewRule", "createRule", "updateRule", "deleteRule"} {
		if v, ok := src.Raw[k]; ok && v != nil {
			payload[k] = v
		}
	}
	for _, key := range []string{"fields", "schema"} {
		if fields, ok := payload[key].([]any); ok {
			clean := make([]any, 0, len(fields))
			for _, f := range fields {
				fm, ok := f.(map[string]any)
				if ok && fm["system"] == true {
					continue
				}
				if ok && fm["type"] == "relation" {
					if sourceID, ok := fm["collectionId"].(string); ok {
						if destinationID := relationIDs[sourceID]; destinationID != "" {
							fm["collectionId"] = destinationID
						}
					}
					if options, ok := fm["options"].(map[string]any); ok {
						if sourceID, ok := options["collectionId"].(string); ok {
							if destinationID := relationIDs[sourceID]; destinationID != "" {
								options["collectionId"] = destinationID
							}
						}
					}
				}
				clean = append(clean, f)
			}
			payload[key] = clean
		}
	}
	body, _ := json.Marshal(payload)
	data, err := c.raw(ctx, http.MethodPost, "/api/collections", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	var created Collection
	if err := json.Unmarshal(data, &created); err != nil {
		return nil, fmt.Errorf("parse created collection %s: %w", src.Name, err)
	}
	if created.ID == "" {
		return nil, fmt.Errorf("created collection %s has no ID", src.Name)
	}
	return &created, nil
}

// RecordPage is one page of records.
type RecordPage struct {
	Items      []map[string]any `json:"items"`
	TotalPages int              `json:"totalPages"`
}

// ListRecordsPage fetches one page of records for a collection.
func (c *Client) ListRecordsPage(ctx context.Context, collection string, page int) (*RecordPage, error) {
	data, err := c.raw(ctx, http.MethodGet,
		fmt.Sprintf("/api/collections/%s/records?page=%d&perPage=500", collection, page), "", nil)
	if err != nil {
		return nil, err
	}
	out := &RecordPage{}
	if err := json.Unmarshal(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// CreateRecord creates a record via JSON. Returns the error message text.
func (c *Client) CreateRecord(ctx context.Context, collection string, record map[string]any) error {
	body, _ := json.Marshal(record)
	_, err := c.raw(ctx, http.MethodPost,
		"/api/collections/"+collection+"/records", "application/json", bytes.NewReader(body))
	return err
}

// CreateRecordMultipart creates a record, uploading placeholder files for the
// given file field names. For file fields the record value should already
// contain the placeholder filename to submit.
func (c *Client) CreateRecordMultipart(ctx context.Context, collection string, record map[string]any, fileFields map[string][]string) error {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for k, v := range record {
		if files, ok := fileFields[k]; ok {
			for _, name := range files {
				fw, err := w.CreateFormFile(k, name)
				if err != nil {
					return err
				}
				if _, err := fw.Write([]byte("This file was masked by pinkmask.")); err != nil {
					return err
				}
			}
			continue
		}
		if v == nil {
			continue
		}
		s := marshalValue(v)
		if err := w.WriteField(k, s); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	_, err := c.raw(ctx, http.MethodPost,
		"/api/collections/"+collection+"/records", w.FormDataContentType(), &buf)
	return err
}

// BatchRequest is one sub-request of the /api/batch endpoint.
type BatchRequest struct {
	Method string         `json:"method"`
	URL    string         `json:"url"`
	Body   map[string]any `json:"body"`
}

// Batch creates many records in one round-trip (PocketBase >= 0.23).
// Returns an error if the endpoint is unavailable so the caller can fall back.
func (c *Client) Batch(ctx context.Context, collection string, records []map[string]any) error {
	reqs := make([]BatchRequest, 0, len(records))
	for _, r := range records {
		reqs = append(reqs, BatchRequest{
			Method: "POST",
			URL:    "/api/collections/" + collection + "/records",
			Body:   r,
		})
	}
	body, _ := json.Marshal(map[string]any{"requests": reqs})
	_, err := c.raw(ctx, http.MethodPost, "/api/batch", "application/json", bytes.NewReader(body))
	return err
}

func marshalValue(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		return trimFloat(t)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func trimFloat(f float64) string {
	s := fmt.Sprintf("%f", f)
	s = strings.TrimRight(s, "0")
	return strings.TrimRight(s, ".")
}
