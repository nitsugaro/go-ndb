package test

import (
	"database/sql"
	"log"
	"testing"
	"time"

	_ "github.com/lib/pq"
	goconf "github.com/nitsugaro/go-conf"
	"github.com/nitsugaro/go-ndb"
	"github.com/nitsugaro/go-nstore"
)

func must(t *testing.T, name string, fn func(t *testing.T)) {
	t.Helper()
	if !t.Run(name, fn) {
		t.FailNow() // corta todo el TestUser en el primer fallo
	}
}

func vinfo(t *testing.T, format string, args ...any) {
	t.Helper()
	if testing.Verbose() {
		t.Logf(format, args...)
	}
}

var bridge *ndb.DBBridge

func TestMain(m *testing.M) {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/test?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(time.Minute * 5)

	goconf.LoadConfig()

	storage, err := nstore.New[*ndb.Schema](goconf.GetOpField("ndb.schema.folder", "schemas"))
	if err != nil {
		panic(err)
	}

	storage.LoadFromDisk()

	bridge = ndb.NewBridge(&ndb.NBridge{DB: db, SchemaPrefix: "ndb_", SchemaStorage: storage})
	/*
		bridge.AddMiddleware(ndb.QueryLoggingMiddleware, false)
	*/
	m.Run()
}
