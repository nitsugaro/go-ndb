package ndb

import (
	"fmt"

	"github.com/nitsugaro/go-nstore"
	goutils "github.com/nitsugaro/go-utils"
)

type Schema struct {
	*nstore.Metadata
	PName                string         `json:"name"`
	PComment             string         `json:"comment,omitempty"`
	PFields              []*SchemaField `json:"fields"`
	PExtensions          []string       `json:"extensions,omitempty"`
	PIndexes             [][]string     `json:"indexes,omitempty"`
	PUniqueIndexes       [][]string     `json:"unique_indexes,omitempty"`
	PCompositePrimaryKey []string       `json:"composite_primary_key,omitempty"`
	PCompositeUniqueKeys [][]string     `json:"composite_unique_keys,omitempty"`
	PMetadata            M              `json:"metadata,omitempty"`
	err                  error          `json:"-"`
}

func (s *Schema) GetName() string {
	return s.PName
}

func NewSchema(name string) *Schema {
	return &Schema{PName: name, PFields: []*SchemaField{}, PMetadata: M{}}
}

func (s *Schema) NewField(name string) *SchemaField {
	f := &SchemaField{PName: name, s: s}
	s.AddField(f)
	return f
}

func (s *Schema) Name(name string) *Schema {
	s.PName = name

	return s
}

func (s *Schema) Comment(comment string) *Schema {
	s.PComment = comment

	return s
}

func (s *Schema) Extension(extensions ...string) *Schema {
	s.PExtensions = extensions

	return s
}

func (s *Schema) Indexes(indexes ...string) *Schema {
	if s.PIndexes == nil {
		s.PIndexes = [][]string{}
	}

	s.PIndexes = append(s.PIndexes, indexes)

	return s
}

func (s *Schema) CompositePK(compositePK ...string) *Schema {
	s.PCompositePrimaryKey = compositePK

	return s
}

func (s *Schema) UniqueIndex(uniqueIndexes ...string) *Schema {
	if s.PUniqueIndexes == nil {
		s.PUniqueIndexes = [][]string{}
	}

	s.PUniqueIndexes = append(s.PUniqueIndexes, uniqueIndexes)

	return s
}

func (s *Schema) CompositeUniqueIndex(compositeUniqueIndex ...[]string) *Schema {
	s.PCompositeUniqueKeys = compositeUniqueIndex

	return s
}

func (s *Schema) GetField(name string) *SchemaField {
	for _, f := range s.PFields {
		if f.PName == name {
			return f
		}
	}

	return nil
}

func (s *Schema) AddField(f *SchemaField) *Schema {
	if s.GetField(f.PName) != nil {
		s.err = fmt.Errorf("field '%s': already exists and cannot be added", f.PName)
		return s
	}

	s.PFields = append(s.PFields, f)
	s.err = nil

	return s
}

func (s *Schema) UpdateField(name string, field *SchemaField) *Schema {
	for i, f := range s.PFields {
		if f.PName == name {
			s.PFields[i] = field
			s.err = nil
			return s
		}
	}

	s.err = fmt.Errorf("field '%s': cannot be updated", name)

	return s
}

func (s *Schema) RemoveField(name string) *Schema {
	length := len(s.PFields)
	newFields := goutils.Filter(s.PFields, func(f *SchemaField, _ int) bool { return f.PName != name })

	if length == len(newFields) {
		s.err = fmt.Errorf("field '%s': cannot be removed", name)
		return s
	}

	s.PFields = newFields
	s.err = nil
	return s
}

func (f *Schema) AddMetadata(key string, val any) *Schema {
	f.PMetadata[key] = val
	return f
}

func (f *Schema) GetSchemaMetadata() M {
	return f.PMetadata
}
