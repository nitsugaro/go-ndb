package test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nitsugaro/go-ndb"
)

type ClientArr struct {
	ID           uint           `json:"id"`
	Name         string         `json:"name"`
	GrantTypes   []int16        `json:"grant_types"`
	RedirectUris []string       `json:"redirect_uris"`
	Meta         map[string]any `json:"meta"`
	CreatedAt    time.Time      `json:"created_at"`
}

var clientsArrTable = ndb.NewSchema("clients_arr_test").
	Comment("Test arrays + jsonb support").
	NewField("id").Type(ndb.FIELD_BIG_SERIAL).PK().DoneField().
	NewField("name").Type(ndb.FIELD_VARCHAR).Max(100).Unique().DoneField().
	NewField("grant_types").Type(ndb.SchemaFieldType(ndb.FIELD_SMALL_INT_ARRAY)).DoneField().
	NewField("redirect_uris").Type(ndb.SchemaFieldType(ndb.FIELD_TEXT_ARRAY)).Nullable().DoneField().
	NewField("meta").Type(ndb.FIELD_JSONB).Nullable().DoneField().
	NewField("created_at").Type(ndb.FIELD_TIMESTAMP).Default("now()").DoneField()

func TestArrayAndJSONBFields(t *testing.T) {
	var (
		appA, appB ClientArr
	)

	must(t, "01_schema_reset", func(t *testing.T) {
		bridge.DeleteSchema(clientsArrTable.GetName())
		if err := bridge.CreateSchema(clientsArrTable); err != nil {
			t.Fatalf("create_schema_clients_arr: %v", err)
		}
	})

	must(t, "02_insert_appA_arrays_and_jsonb", func(t *testing.T) {
		meta := json.RawMessage(`{
			"tier": "gold",
			"enabled": true,
			"limits": {"rpm": 1200, "burst": 50},
			"tags": ["a","b"]
		}`)

		payload := ndb.M{
			"name":          "appA",
			"grant_types":   []int16{0, 4, 6},
			"redirect_uris": []string{"https://a.test/cb", "https://b.test/cb"},
			"meta":          meta,
		}

		q := ndb.NewCreateQuery(clientsArrTable.GetName()).
			Payload(payload).
			Fields("id", "name", "grant_types", "redirect_uris", "meta", "created_at")

		if err := bridge.CreateOneB(q, &appA); err != nil {
			t.Fatalf("insert_appA_error: %v", err)
		}

		if appA.ID == 0 {
			t.Fatalf("appA_invalid_id")
		}
		if appA.Name != "appA" {
			t.Fatalf("appA_name_mismatch expected=%q actual=%q", "appA", appA.Name)
		}
		if !eqI16(appA.GrantTypes, []int16{0, 4, 6}) {
			t.Fatalf("appA_grant_types_mismatch expected=%v actual=%v", []int16{0, 4, 6}, appA.GrantTypes)
		}
		if !eqS(appA.RedirectUris, []string{"https://a.test/cb", "https://b.test/cb"}) {
			t.Fatalf("appA_redirect_uris_mismatch expected=%v actual=%v", []string{"https://a.test/cb", "https://b.test/cb"}, appA.RedirectUris)
		}

		if mustStrM(appA.Meta, "tier") != "gold" {
			t.Fatalf("appA_meta_tier_mismatch actual=%v", appA.Meta["tier"])
		}
		if mustBoolM(appA.Meta, "enabled") != true {
			t.Fatalf("appA_meta_enabled_mismatch actual=%v", appA.Meta["enabled"])
		}

		vinfo(t, "appA=%+v", appA)
	})

	must(t, "03_insert_appB_empty_array_null_array_jsonb_string", func(t *testing.T) {
		meta := `{"tier":"free","enabled":false,"limits":{"rpm":10}}`

		payload := ndb.M{
			"name":          "appB",
			"grant_types":   []int16{},
			"redirect_uris": nil,
			"meta":          meta,
		}

		q := ndb.NewCreateQuery(clientsArrTable.GetName()).
			Payload(payload).
			Fields("id", "name", "grant_types", "redirect_uris", "meta", "created_at")

		if err := bridge.CreateOneB(q, &appB); err != nil {
			t.Fatalf("insert_appB_error: %v", err)
		}

		if appB.ID == 0 {
			t.Fatalf("appB_invalid_id")
		}
		if appB.Name != "appB" {
			t.Fatalf("appB_name_mismatch expected=%q actual=%q", "appB", appB.Name)
		}
		if len(appB.GrantTypes) != 0 {
			t.Fatalf("appB_grant_types_expected_empty actual=%v", appB.GrantTypes)
		}
		if len(appB.RedirectUris) != 0 {
			t.Fatalf("appB_redirect_uris_expected_null_or_empty actual=%v", appB.RedirectUris)
		}

		m := appB.Meta
		if mustStrM(m, "tier") != "free" {
			t.Fatalf("appB_meta_tier_mismatch actual=%v", m["tier"])
		}

		vinfo(t, "appB=%+v", appB)
	})

	must(t, "04_read_by_id_in_operator", func(t *testing.T) {
		read := ndb.NewReadQuery(clientsArrTable.GetName()).
			Where(ndb.M{
				"id": ndb.M{
					"in": []any{appA.ID, appB.ID},
				},
			}).
			Order(ndb.Fs("clients_arr_test.id", "ASC")).
			Fields("meta", "id", "grant_types")

		var rows []ClientArr
		if err := bridge.ReadB(read, &rows); err != nil {
			t.Fatalf("read_in_error: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("read_in_count_mismatch expected=2 actual=%d rows=%v", len(rows), rows)
		}

		if rows[0].ID != appA.ID {
			t.Fatalf("read_in_appA_id_mismatch expected=%d actual=%d", appA.ID, rows[0].ID)
		}
		if !eqI16(rows[0].GrantTypes, appA.GrantTypes) {
			t.Fatalf("read_in_appA_grant_types_mismatch expected=%v actual=%v", appA.GrantTypes, rows[0].GrantTypes)
		}
		if mustStrM(rows[0].Meta, "tier") != "gold" {
			fmt.Println(rows[0].Meta)
			t.Fatalf("read_in_appA_meta_mismatch")
		}

		if rows[1].ID != appB.ID {
			t.Fatalf("read_in_appB_id_mismatch expected=%d actual=%d", appB.ID, rows[1].ID)
		}
		if len(rows[1].GrantTypes) != 0 {
			t.Fatalf("read_in_appB_grant_types_expected_empty actual=%v", rows[1].GrantTypes)
		}
		if mustStrM(rows[1].Meta, "tier") != "free" {
			t.Fatalf("read_in_appB_meta_mismatch")
		}

		vinfo(t, "read_in_rows=%+v", rows)
	})

	must(t, "05_read_by_name_not_in_operator", func(t *testing.T) {
		read := ndb.NewReadQuery(clientsArrTable.GetName()).
			Where(ndb.M{
				"name": ndb.M{
					"not_in": []any{"appB"},
				},
			})

		var rows []ClientArr
		if err := bridge.ReadB(read, &rows); err != nil {
			t.Fatalf("read_not_in_error: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("read_not_in_count_mismatch expected=1 actual=%d rows=%v", len(rows), rows)
		}
		if rows[0].Name != "appA" {
			t.Fatalf("read_not_in_expected_appA actual=%q", rows[0].Name)
		}
	})

	must(t, "06_insert_string_array_with_comma_and_quotes", func(t *testing.T) {
		payload := ndb.M{
			"name":        "appC",
			"grant_types": []int16{1},
			"redirect_uris": []string{
				`https://c.test/cb?x=1,2`,
				`https://c.test/cb?msg="hola"`,
				`https://c.test/cb?path=a\bb`,
			},
			"meta": json.RawMessage(`{"tier":"pro","note":"comma, quote and slash"}`),
		}

		var appC ClientArr
		q := ndb.NewCreateQuery(clientsArrTable.GetName()).
			Payload(payload).
			Fields("id", "name", "grant_types", "redirect_uris", "meta", "created_at")

		if err := bridge.CreateOneB(q, &appC); err != nil {
			t.Fatalf("insert_appC_error: %v", err)
		}

		if appC.Name != "appC" {
			t.Fatalf("appC_name_mismatch expected=%q actual=%q", "appC", appC.Name)
		}
		if !eqI16(appC.GrantTypes, []int16{1}) {
			t.Fatalf("appC_grant_types_mismatch expected=%v actual=%v", []int16{1}, appC.GrantTypes)
		}
		if !eqS(appC.RedirectUris, []string{
			`https://c.test/cb?x=1,2`,
			`https://c.test/cb?msg="hola"`,
			`https://c.test/cb?path=a\bb`,
		}) {
			t.Fatalf("appC_redirect_uris_mismatch expected=%v actual=%v", payload["redirect_uris"], appC.RedirectUris)
		}
		if mustStrM(appC.Meta, "tier") != "pro" {
			t.Fatalf("appC_meta_tier_mismatch")
		}

		vinfo(t, "appC=%+v", appC)
	})

	must(t, "07_jsonb_query_by_subfield_should_fail_in_builder", func(t *testing.T) {
		read := ndb.NewReadQuery(clientsArrTable.GetName()).
			Where(ndb.M{"meta->>'tier'": "gold"})

		var rows []ClientArr
		err := bridge.ReadB(read, &rows)
		if err == nil {
			t.Fatalf("expected_error_querying_json_subfield_but_got_none rows=%v", rows)
		}

		vinfo(t, "jsonb_subfield_query_expected_error=%v", err)
	})
}

func eqI16(a, b []int16) bool {
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

func eqS(a, b []string) bool {
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

func mustMetaMap(t *testing.T, raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("meta_unmarshal_error: %v raw=%s", err, string(raw))
	}
	return m
}

func mustStrM(m map[string]any, k string) string {
	v, ok := m[k]
	if !ok || v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

func mustBoolM(m map[string]any, k string) bool {
	v, ok := m[k]
	if !ok || v == nil {
		return false
	}
	b, _ := v.(bool)
	return b
}
