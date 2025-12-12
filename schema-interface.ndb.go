package ndb

import (
	"errors"
	"fmt"

	"github.com/nitsugaro/go-nstore"
	goutils "github.com/nitsugaro/go-utils"
)

type ForeignKey struct {
	PSchema   string         `json:"schema"`
	PColumn   string         `json:"column"`
	POnDelete ForeignKeyRule `json:"on_delete,omitempty"`
	POnUpdate ForeignKeyRule `json:"on_update,omitempty"`
	f         *SchemaField
}

func (fk *ForeignKey) OnUpdate(OnUpdate ForeignKeyRule) *ForeignKey {
	fk.POnUpdate = OnUpdate
	return fk
}

func (fk *ForeignKey) OnDelete(onDelete ForeignKeyRule) *ForeignKey {
	fk.POnDelete = onDelete
	return fk
}

func (fk *ForeignKey) DoneFK() *SchemaField {
	return fk.f
}

func (fk *ForeignKey) Validate() error {
	if len(fk.PSchema) == 0 || len(fk.PColumn) == 0 {
		return errors.New("schema & column fields are required")
	}

	if err := IsSQLName(fk.PSchema); err != nil {
		return err
	}

	if err := IsSQLName(fk.PColumn); err != nil {
		return err
	}

	if fk.POnDelete != "" && goutils.All(foreignKeyRules, func(fkr ForeignKeyRule, _ int) bool { return fkr != fk.POnDelete }) {
		return fmt.Errorf("invalid on delete clausure: %s", fk.POnDelete)
	}

	if fk.POnUpdate != "" && goutils.All(foreignKeyRules, func(fkr ForeignKeyRule, _ int) bool { return fkr != fk.POnUpdate }) {
		return fmt.Errorf("invalid on update clausure: %s", fk.POnUpdate)
	}

	return nil
}

type SchemaField struct {
	PName       string          `json:"name"`
	PType       SchemaFieldType `json:"type"`
	PMax        *int            `json:"max,omitempty"`
	PMin        *int            `json:"min,omitempty"`
	PNullable   bool            `json:"nullable"`
	PUnique     bool            `json:"unique,omitempty"`
	PDefault    *string         `json:"default,omitempty"`
	PPrimaryKey bool            `json:"primary_key,omitempty"`
	PForeignKey *ForeignKey     `json:"foreign_key,omitempty"`
	PEnumValues []string        `json:"enum_values,omitempty"`
	PPattern    *string         `json:"pattern,omitempty"`
	PComment    string          `json:"comment,omitempty"`
	PMetadata   M               `json:"metadata,omitempty"`
	s           *Schema
}

func (f *SchemaField) Type(typ SchemaFieldType) *SchemaField {
	f.PType = typ
	return f
}

func (f *SchemaField) Max(max int) *SchemaField {
	f.PMax = &max
	return f
}

func (f *SchemaField) Min(min int) *SchemaField {
	f.PMin = &min
	return f
}

func (f *SchemaField) Nullable() *SchemaField {
	f.PNullable = true
	return f
}

func (f *SchemaField) Unique() *SchemaField {
	f.PUnique = true
	return f
}

func (f *SchemaField) PK() *SchemaField {
	f.PPrimaryKey = true
	return f
}

func (f *SchemaField) Pattern(pattern string) *SchemaField {
	f.PPattern = &pattern
	return f
}

func (f *SchemaField) Enum(values ...string) *SchemaField {
	f.PEnumValues = values
	return f
}

func (f *SchemaField) NewFK(schema string, column string) *ForeignKey {
	f.PForeignKey = &ForeignKey{f: f, PSchema: schema, PColumn: column}

	return f.PForeignKey
}

func (f *SchemaField) Default(defaultValue string) *SchemaField {
	f.PDefault = &defaultValue
	return f
}

func (f *SchemaField) DoneField() *Schema {
	return f.s
}

func (f *SchemaField) Metadata(m M) *SchemaField {
	f.PMetadata = m
	return f
}

func (f *SchemaField) GetMetadata() goutils.TreeMapImpl {
	return goutils.NewTreeMap(f.PMetadata)
}

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
	PMetadata            M              `json:"composite_unique_keys,omitempty"`
	err                  error          `json:"-"`
}

func (s *Schema) GetName() string {
	return s.PName
}

func NewSchema(name string) *Schema {
	return &Schema{PName: name, PFields: []*SchemaField{}}
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
