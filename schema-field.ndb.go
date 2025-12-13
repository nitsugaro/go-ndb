package ndb

import goutils "github.com/nitsugaro/go-utils"

type SchemaField struct {
	PName        string          `json:"name"`
	PDisplayName string          `json:"display_name"`
	PType        SchemaFieldType `json:"type"`
	PMax         *int            `json:"max,omitempty"`
	PMin         *int            `json:"min,omitempty"`
	PNullable    bool            `json:"nullable"`
	PUnique      bool            `json:"unique,omitempty"`
	PDefault     *string         `json:"default,omitempty"`
	PPrimaryKey  bool            `json:"primary_key,omitempty"`
	PForeignKey  *ForeignKey     `json:"foreign_key,omitempty"`
	PEnumValues  []string        `json:"enum_values,omitempty"`
	PPattern     *string         `json:"pattern,omitempty"`
	PComment     string          `json:"comment,omitempty"`
	PMetadata    M               `json:"metadata,omitempty"`
	s            *Schema
}

func (f *SchemaField) GetName() string {
	return f.PName
}

func (f *SchemaField) GetDisplayName() string {
	return f.PDisplayName
}

func (f *SchemaField) DisplayName(displayName string) *SchemaField {
	f.PDisplayName = displayName
	return f
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
