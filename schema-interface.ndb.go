package ndb

import (
	"github.com/nitsugaro/go-nstore"
	goutils "github.com/nitsugaro/go-utils"
)

type ForeignKeyRule string

const (
	NoAction   ForeignKeyRule = "NO ACTION"
	Restrict   ForeignKeyRule = "RESTRICT"
	Cascade    ForeignKeyRule = "CASCADE"
	SetNull    ForeignKeyRule = "SET NULL"
	SetDefault ForeignKeyRule = "SET DEFAULT"
)

type FieldType string

const (
	FieldSmallInt    FieldType = "smallint"
	FieldSmallSerial FieldType = "smallserial"
	FieldInt         FieldType = "int"
	FieldBigInt      FieldType = "bigint"
	FieldSerial      FieldType = "serial"
	FieldBigSerial   FieldType = "bigserial"
	FieldVarchar     FieldType = "varchar"
	FieldText        FieldType = "text"
	FieldUUID        FieldType = "uuid"
	FieldBoolean     FieldType = "boolean"
	FieldTimestamp   FieldType = "timestamp"
	FieldJSONB       FieldType = "jsonb"
	FieldFloat       FieldType = "float"
	FieldDouble      FieldType = "double precision"
)

type ForeignKey struct {
	Schema   string         `json:"schema"`
	Column   string         `json:"column"`
	OnDelete ForeignKeyRule `json:"on_delete,omitempty"`
	OnUpdate ForeignKeyRule `json:"on_update,omitempty"`
}

type Field struct {
	Name       string      `json:"name"`
	Type       FieldType   `json:"type"`
	Max        *int        `json:"max,omitempty"`
	Min        *int        `json:"min,omitempty"`
	Nullable   bool        `json:"nullable"`
	Unique     bool        `json:"unique,omitempty"`
	Default    *string     `json:"default,omitempty"`
	PrimaryKey bool        `json:"primary_key,omitempty"`
	ForeignKey *ForeignKey `json:"foreign_key,omitempty"`
	EnumValues []string    `json:"enum_values,omitempty"`
	Pattern    *string     `json:"pattern,omitempty"`
	Comment    string      `json:"comment,omitempty"`
	Metadata   M           `json:"metadata,omitempty"`
}

type Schema struct {
	*nstore.Metadata
	Name                string     `json:"name"`
	Comment             string     `json:"comment,omitempty"`
	Fields              []Field    `json:"fields"`
	Extensions          []string   `json:"extensions,omitempty"`
	Indexes             [][]string `json:"indexes,omitempty"`
	UniqueIndexes       [][]string `json:"unique_indexes,omitempty"`
	CompositePrimaryKey []string   `json:"composite_primary_key,omitempty"`
	CompositeUniqueKeys [][]string `json:"composite_unique_keys,omitempty"`
}

func (s *Schema) GetField(name string) *Field {
	for _, f := range s.Fields {
		if f.Name == name {
			return &f
		}
	}

	return nil
}

func (s *Schema) AddField(f *Field) bool {
	if s.GetField(f.Name) != nil {
		return false
	}

	s.Fields = append(s.Fields, *f)

	return true
}

func (s *Schema) UpdateField(name string, field *Field) bool {
	for i, f := range s.Fields {
		if f.Name == name {
			s.Fields[i] = *field
			return true
		}
	}

	return false
}

func (s *Schema) RemoveField(name string) bool {
	length := len(s.Fields)
	newFields := goutils.Filter(s.Fields, func(f Field, _ int) bool { return f.Name != name })

	if length == len(newFields) {
		return false
	}

	s.Fields = newFields

	return true
}

func (f *Field) GetMetadata() goutils.TreeMapImpl {
	return goutils.NewTreeMap(f.Metadata)
}
