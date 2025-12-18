package test

import (
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/nitsugaro/go-ndb"
)

func benchResetClientsArr(b *testing.B) ([]uint, func()) {
	bridge.DeleteSchema(clientsArrTable.GetName())
	if err := bridge.CreateSchema(clientsArrTable); err != nil {
		b.Fatalf("create_schema_clients_arr: %v", err)
	}

	cleanup := func() {
		bridge.DeleteSchema(clientsArrTable.GetName())
	}

	metaA := json.RawMessage(`{"tier":"gold","enabled":true,"limits":{"rpm":1200,"burst":50},"tags":["a","b"]}`)
	metaB := json.RawMessage(`{"tier":"free","enabled":false,"limits":{"rpm":10}}`)

	var ids []uint

	seed := func(name string, grants []int16, uris []string, meta any) uint {
		payload := ndb.M{
			"name":          name,
			"grant_types":   grants,
			"redirect_uris": uris,
			"meta":          meta,
		}

		q := ndb.NewCreateQuery(clientsArrTable.GetName()).
			Payload(payload).
			Fields("id", "name", "grant_types", "redirect_uris", "created_at")

		var out ClientArr
		if err := bridge.CreateOneB(q, &out); err != nil {
			b.Fatalf("seed_insert_error name=%s: %v", name, err)
		}
		return out.ID
	}

	ids = append(ids, seed("seed_appA", []int16{0, 4, 6}, []string{"https://a.test/cb", "https://b.test/cb"}, metaA))
	ids = append(ids, seed("seed_appB", []int16{}, []string{}, metaB))

	for i := 0; i < 128; i++ {
		name := "seed_" + strconv.Itoa(i)
		grants := []int16{0, 4, 6}
		uris := []string{"https://x.test/cb", "https://y.test/cb"}
		ids = append(ids, seed(name, grants, uris, metaA))
	}

	return ids, cleanup
}

func BenchmarkCreateOneB_ClientArr_NoReturnMeta(b *testing.B) {
	_, cleanup := benchResetClientsArr(b)
	defer cleanup()

	meta := json.RawMessage(`{"tier":"gold","enabled":true,"limits":{"rpm":1200,"burst":50},"tags":["a","b"]}`)

	payload := ndb.M{
		"name":          "",
		"grant_types":   []int16{0, 4, 6},
		"redirect_uris": []string{"https://a.test/cb", "https://b.test/cb"},
		"meta":          meta,
	}

	q := ndb.NewCreateQuery(clientsArrTable.GetName()).
		Payload(payload).
		Fields("id", "name", "grant_types", "redirect_uris", "created_at")

	var n uint64
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := atomic.AddUint64(&n, 1)
		payload["name"] = "bench_app_" + strconv.FormatUint(id, 10)

		var out ClientArr
		if err := bridge.CreateOneB(q, &out); err != nil {
			b.Fatalf("insert_error: %v", err)
		}
		if out.ID == 0 {
			b.Fatalf("invalid_id")
		}
	}
}

func BenchmarkCreateOne_ClientArr_Map(b *testing.B) {
	_, cleanup := benchResetClientsArr(b)
	defer cleanup()

	meta := json.RawMessage(`{"tier":"gold","enabled":true,"limits":{"rpm":1200,"burst":50},"tags":["a","b"]}`)

	payload := ndb.M{
		"name":          "",
		"grant_types":   []int16{0, 4, 6},
		"redirect_uris": []string{"https://a.test/cb", "https://b.test/cb"},
		"meta":          meta,
	}

	q := ndb.NewCreateQuery(clientsArrTable.GetName()).
		Payload(payload).
		Fields("id", "name", "grant_types", "redirect_uris", "created_at")

	var n uint64
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := atomic.AddUint64(&n, 1)
		payload["name"] = "bench_app_m_" + strconv.FormatUint(id, 10)

		row, err := bridge.CreateOne(q)
		if err != nil {
			b.Fatalf("insert_error: %v", err)
		}
		if row["id"] == nil {
			b.Fatalf("invalid_id")
		}
	}
}

func BenchmarkReadB_ClientArr_In_NoMeta(b *testing.B) {
	ids, cleanup := benchResetClientsArr(b)
	defer cleanup()

	in := make([]any, 0, len(ids))
	for _, id := range ids {
		in = append(in, id)
	}

	read := ndb.NewReadQuery(clientsArrTable.GetName()).
		Fields("id", "name", "grant_types", "redirect_uris", "created_at").
		Where(ndb.M{"id": ndb.M{"in": in}}).
		Order(ndb.Fs("clients_arr_test.id", "ASC")).
		Limit(1000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var rows []ClientArr
		if err := bridge.ReadB(read, &rows); err != nil {
			b.Fatalf("read_error: %v", err)
		}

		if len(rows) != len(ids) {
			b.Fatalf("count_mismatch expected=%d actual=%d", len(ids), len(rows))
		}
	}
}

func BenchmarkRead_ClientArr_Map_NoMeta(b *testing.B) {
	ids, cleanup := benchResetClientsArr(b)
	defer cleanup()

	in := make([]any, 0, len(ids))
	for _, id := range ids {
		in = append(in, id)
	}

	read := ndb.NewReadQuery(clientsArrTable.GetName()).
		Fields("id", "name", "grant_types", "redirect_uris", "created_at").
		Where(ndb.M{"id": ndb.M{"in": in}}).
		Order(ndb.Fs("clients_arr_test.id", "ASC")).
		Limit(10000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows, err := bridge.Read(read)
		if err != nil {
			b.Fatalf("read_error: %v", err)
		}
		if len(rows) != len(ids) {
			b.Fatalf("count_mismatch expected=%d actual=%d", len(ids), len(rows))
		}
	}
}

func BenchmarkReadOneB_ClientArr_NoMeta(b *testing.B) {
	ids, cleanup := benchResetClientsArr(b)
	defer cleanup()

	read := ndb.NewReadQuery(clientsArrTable.GetName()).
		Fields("id", "name", "grant_types", "redirect_uris", "created_at").
		Where(ndb.M{"id": ids[0]}).
		Limit(1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var out ClientArr
		if err := bridge.ReadOneB(read, &out); err != nil {
			b.Fatalf("readone_error: %v", err)
		}
		if out.ID == 0 {
			b.Fatalf("invalid_id")
		}
	}
}

func BenchmarkReadOne_ClientArr_Map_NoMeta(b *testing.B) {
	ids, cleanup := benchResetClientsArr(b)
	defer cleanup()

	read := ndb.NewReadQuery(clientsArrTable.GetName()).
		Fields("id", "name", "grant_types", "redirect_uris", "created_at").
		Where(ndb.M{"id": ids[0]}).
		Limit(1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		row, err := bridge.ReadOne(read)
		if err != nil {
			b.Fatalf("readone_error: %v", err)
		}
		if row == nil || row["id"] == nil {
			b.Fatalf("invalid_row")
		}
	}
}

func BenchmarkNormalize_Only(b *testing.B) {
	rawJSON := []byte(`{"tier":"gold","enabled":true,"limits":{"rpm":1200,"burst":50},"tags":["a","b"]}`)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var v any
		_ = json.Unmarshal(rawJSON, &v)
	}
}
