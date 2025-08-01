package ndb

import (
	"fmt"
	"strings"
)

func Ptr[T any](v T) *T {
	return &v
}

func (d *DBBridge) generateCreateSchemaSQL(t *Schema) string {
	fullTableName := "\"" + d.schemaPrefix + t.Name + "\""
	var sb strings.Builder

	// 1. Extensions
	for _, ext := range t.Extensions {
		sb.WriteString(ext)
		sb.WriteString(";\n")
	}

	// 2. Table Comments
	if t.Comment != "" {
		sb.WriteString(fmt.Sprintf("-- %s\n", t.Comment))
	}

	// 3. CREATE TABLE
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", fullTableName))
	for i, f := range t.Fields {
		line := fmt.Sprintf("    %s %s", f.Name, string(f.Type))

		if f.Type == FieldVarchar && f.Max != nil {
			line = fmt.Sprintf("    %s %s(%d)", f.Name, f.Type, *f.Max)
		}

		if f.PrimaryKey {
			line += " PRIMARY KEY"
		}
		if f.Unique {
			line += " UNIQUE"
		}
		if !f.Nullable {
			line += " NOT NULL"
		}
		if f.Default != nil {
			line += fmt.Sprintf(" DEFAULT %s", *f.Default)
		}
		if f.ForeignKey != nil {
			line += fmt.Sprintf(" REFERENCES \"%s\"(%s)", d.schemaPrefix+f.ForeignKey.Schema, f.ForeignKey.Column)
			if f.ForeignKey.OnDelete != "" {
				line += " ON DELETE " + string(f.ForeignKey.OnDelete)
			}
			if f.ForeignKey.OnUpdate != "" {
				line += " ON UPDATE " + string(f.ForeignKey.OnUpdate)
			}
		}

		if len(f.EnumValues) > 0 {
			quoted := "'" + strings.Join(f.EnumValues, "', '") + "'"
			line += fmt.Sprintf(" CHECK (%s IN (%s))", f.Name, quoted)
		}
		if i < len(t.Fields)-1 {
			line += ","
		}
		line += "\n"
		sb.WriteString(line)
	}
	sb.WriteString(");\n\n")

	// 4. column comment
	for _, f := range t.Fields {
		if f.Comment != "" {
			sb.WriteString(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';\n", fullTableName, f.Name, f.Comment))
		}
	}

	// 5. table comment
	if t.Comment != "" {
		sb.WriteString(fmt.Sprintf("COMMENT ON TABLE %s IS '%s';\n", fullTableName, t.Comment))
	}

	// 6. index
	for _, idx := range t.Indexes {
		indexName := fmt.Sprintf("idx_%s_%s", t.Name, strings.Join(idx, "_"))
		sb.WriteString(fmt.Sprintf("CREATE INDEX %s ON %s (%s);\n", indexName, fullTableName, strings.Join(idx, ", ")))
	}

	// 7. unique index
	for _, uidx := range t.UniqueIndexes {
		indexName := fmt.Sprintf("uniq_%s_%s", t.Name, strings.Join(uidx, "_"))
		sb.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);\n", indexName, fullTableName, strings.Join(uidx, ", ")))
	}

	// 8. primary compose key
	if len(t.CompositePrimaryKey) > 0 {
		sb.WriteString(fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s);\n", fullTableName, strings.Join(t.CompositePrimaryKey, ", ")))
	}

	// 9. unique compose keys
	for _, uc := range t.CompositeUniqueKeys {
		indexName := fmt.Sprintf("uniq_%s_%s", t.Name, strings.Join(uc, "_"))
		sb.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);\n", indexName, fullTableName, strings.Join(uc, ", ")))
	}

	return sb.String()
}

func (d *DBBridge) generateDropSchemaSql(name string) string {
	return fmt.Sprintf("DROP TABLE \"%s\"", d.schemaPrefix+name)
}
