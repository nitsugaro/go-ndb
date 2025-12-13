package ndb

import (
	"database/sql"
	"fmt"

	"github.com/nitsugaro/go-nstore"
	goutils "github.com/nitsugaro/go-utils"
)

type DBFuncs interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
	Begin() (*sql.Tx, error)
}

type M = map[string]any

type DBBridge struct {
	schemaPrefix  string
	db            *sql.DB
	trx           *sql.Tx
	prevValidate  []QueryMiddleware
	postValidate  []QueryMiddleware
	schemaStorage *nstore.NStorage[*Schema]
}

func (dbb *DBBridge) GetSchemas(query ...nstore.ConditionalFunc[*Schema]) []*Schema {
	if len(query) == 0 {
		return dbb.schemaStorage.ListOfCache()
	}

	result, _ := dbb.schemaStorage.Query(query[0], len(dbb.schemaStorage.IDs()))
	return goutils.Filter(result, func(s *Schema, _ int) bool { return s != nil })
}

func (dbb *DBBridge) GetSchemaByName(name string) (*Schema, bool) {
	result, total := dbb.schemaStorage.Query(func(t *Schema) bool { return t.PName == name }, 1)
	if total == 1 {
		return result[0], true
	}

	return nil, false
}

func (dbb *DBBridge) CreateSchema(schema *Schema) error {
	_, err := dbb.ExecuteQuery(dbb.generateCreateSchemaSQL(schema))
	if err != nil {
		return err
	}

	dbb.schemaStorage.Save(schema)

	return nil
}

func (dbb *DBBridge) ModifySchema(schemaName string, fields []*AlterField) error {
	sql, newSchema, err := dbb.generateAlterSchemaSQL(schemaName, fields)

	if err != nil {
		return err
	}

	if _, err := dbb.ExecuteQuery(sql); err != nil {
		return err
	}

	return dbb.schemaStorage.Save(newSchema)
}

func (dbb *DBBridge) DeleteSchema(name string) error {
	schema, ok := dbb.GetSchemaByName(name)
	if !ok {
		return fmt.Errorf("schema '%s' not found", name)
	}

	_, err := dbb.ExecuteQuery(dbb.generateDropSchemaSql(name))
	if err != nil {
		return err
	}

	return dbb.schemaStorage.Delete(schema.ID)
}

func (dbb *DBBridge) GetSchemaPrefix() string {
	return dbb.schemaPrefix
}

type NBridge struct {
	DB                      *sql.DB
	SchemaPrefix            string
	SchemaStorage           *nstore.NStorage[*Schema]
	trx                     *sql.Tx
	prevValidatemiddlewares []QueryMiddleware
	postValidatemiddlewares []QueryMiddleware
}

func NewBridge(nbrigde *NBridge) *DBBridge {
	brigde := &DBBridge{
		db:            nbrigde.DB,
		trx:           nbrigde.trx,
		schemaPrefix:  nbrigde.SchemaPrefix,
		schemaStorage: nbrigde.SchemaStorage,
		prevValidate:  nbrigde.prevValidatemiddlewares,
		postValidate:  nbrigde.postValidatemiddlewares,
	}

	if brigde.prevValidate == nil {
		brigde.prevValidate = []QueryMiddleware{}
	}

	if brigde.postValidate == nil {
		brigde.postValidate = []QueryMiddleware{}
	}

	return brigde
}
