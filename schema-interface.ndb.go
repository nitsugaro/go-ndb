package ndb

import (
	"fmt"
	"slices"

	"github.com/nitsugaro/go-nstore"
	goutils "github.com/nitsugaro/go-utils"
)

type SchemaGroup struct {
	DisplayName string   `json:"display_name"`
	Fields      []string `json:"fields"`
}

type RESTSchema struct {
	PMethods           []string `json:"methods,omitempty"`
	PAllowedJoinTables []string `json:"allowed_join_tables,omitempty"`
	PAllowedFields     []string `json:"allowed_fields,omitempty"`
	s                  *Schema
}

type RESTCollectionSchema = RESTSchema

type RESTResourceSchema struct {
	*RESTSchema
	IDField string `json:"id_field"`
}

type Schema struct {
	*nstore.Metadata
	PName                string                `json:"name"`
	PComment             string                `json:"comment,omitempty"`
	PFields              []*SchemaField        `json:"fields,omitempty"`
	PExtensions          []string              `json:"extensions,omitempty"`
	PIndexes             [][]string            `json:"indexes,omitempty"`
	PUniqueIndexes       [][]string            `json:"unique_indexes,omitempty"`
	PCompositePrimaryKey []string              `json:"composite_primary_key,omitempty"`
	PCompositeUniqueKeys [][]string            `json:"composite_unique_keys,omitempty"`
	PMetadata            M                     `json:"metadata,omitempty"`
	PGroups              []*SchemaGroup        `json:"groups,omitempty"`
	PRestCollection      *RESTCollectionSchema `json:"rest_collection,omitempty"`
	PRestResource        *RESTResourceSchema   `json:"rest_resource,omitempty"`
	err                  error                 `json:"-"`
}

func (s *Schema) NewGroup(name string) *Schema {
	s.PGroups = append(s.PGroups, &SchemaGroup{DisplayName: name, Fields: []string{}})
	return s
}

func (s *Schema) GetName() string {
	return s.PName
}

func NewSchema(name string) *Schema {
	return &Schema{PName: name, PFields: []*SchemaField{}, PGroups: []*SchemaGroup{}, PMetadata: M{}}
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

func (s *Schema) EnableRESTCollection(methods ...string) *RESTCollectionSchema {
	s.PRestCollection = &RESTCollectionSchema{
		PMethods: methods,
		s:        s,
	}

	return s.PRestCollection
}

func (rc *RESTCollectionSchema) AllowedFields(fields ...string) *RESTCollectionSchema {
	rc.PAllowedFields = fields
	return rc
}

func (rc *RESTCollectionSchema) AllowedJoins(joins ...string) *RESTCollectionSchema {
	rc.PAllowedJoinTables = joins
	return rc
}

func (rc *RESTCollectionSchema) DoneRESTCollection() *Schema {
	return rc.s
}

func (s *Schema) EnableRESTResource(IDField string, methods ...string) *RESTResourceSchema {
	s.PRestResource = &RESTResourceSchema{
		IDField: IDField,
		RESTSchema: &RESTSchema{
			PMethods: methods,
			s:        s,
		},
	}

	return s.PRestResource
}

func (rc *RESTResourceSchema) AllowedFields(fields ...string) *RESTResourceSchema {
	rc.PAllowedFields = fields
	return rc
}

func (rc *RESTResourceSchema) AllowedJoins(joins ...string) *RESTResourceSchema {
	rc.PAllowedJoinTables = joins
	return rc
}

func (s *Schema) GetRESTCollecton() *RESTCollectionSchema {
	return s.PRestCollection
}

func (s *Schema) GetRESTResource() *RESTResourceSchema {
	return s.PRestResource
}

func (rc *RESTResourceSchema) DoneRESTResource() *Schema {
	return rc.s
}

func (f *Schema) isRESTMetohdSupported(restSchema *RESTSchema, method string) bool {
	if restSchema == nil {
		return false
	}

	return slices.Contains(restSchema.PMethods, method)
}

func (f *Schema) IsRESTCollectionMethodSupported(method string) bool {
	return f.isRESTMetohdSupported(f.PRestCollection, method)
}

func (f *Schema) IsRESTResourceMethodSupported(method string) bool {
	return f.isRESTMetohdSupported(f.PRestResource.RESTSchema, method)
}

func (f *Schema) GetRESTResourceIDField() string {
	return f.PRestResource.IDField
}
