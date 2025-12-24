package ndb

import (
	"fmt"
	"strings"
)

func Ptr[T any](v T) *T {
	return &v
}

func (d *DBBridge) generateCreateSchemaSQL(t *Schema) string {
	fullTableName := "\"" + d.schemaPrefix + t.PName + "\""
	var sb strings.Builder

	// 1. Extensions
	for _, ext := range t.PExtensions {
		sb.WriteString(ext)
		sb.WriteString(";\n")
	}

	// 2. Table Comments
	if t.PComment != "" {
		sb.WriteString(fmt.Sprintf("-- %s\n", t.PComment))
	}

	// 3. CREATE TABLE
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", fullTableName))
	for i, f := range t.PFields {
		line := fmt.Sprintf("    %s %s", f.PName, string(f.PType))

		if f.PType == FIELD_VARCHAR && f.PMax != nil {
			line = fmt.Sprintf("    %s %s(%d)", f.PName, f.PType, *f.PMax)
		}

		if f.PPrimaryKey {
			line += " PRIMARY KEY"
		}
		if f.PUnique {
			line += " UNIQUE"
		}
		if !f.PNullable {
			line += " NOT NULL"
		}
		if f.PDefault != nil {
			line += fmt.Sprintf(" DEFAULT %s", *f.PDefault)
		}
		if f.PForeignKey != nil {
			line += fmt.Sprintf(" REFERENCES \"%s\"(%s)", d.schemaPrefix+f.PForeignKey.PSchema, f.PForeignKey.PColumn)
			if f.PForeignKey.POnDelete != "" {
				line += " ON DELETE " + string(f.PForeignKey.POnDelete)
			}
			if f.PForeignKey.POnUpdate != "" {
				line += " ON UPDATE " + string(f.PForeignKey.POnUpdate)
			}
		}

		if len(f.PEnumValues) > 0 {
			if strings.Contains(string(f.PType), "[]") {
				line += fmt.Sprintf(" CHECK (%s <@ ARRAY[%s]::%s)", f.PName, strings.Join(f.PEnumValues, ", "), f.PType)
			} else {
				line += fmt.Sprintf(" CHECK (%s IN (%s))", f.PName, strings.Join(f.PEnumValues, ", "))
			}
		}
		if i < len(t.PFields)-1 {
			line += ","
		}
		line += "\n"
		sb.WriteString(line)
	}
	sb.WriteString(");\n\n")

	// 4. column comment
	for _, f := range t.PFields {
		if f.PComment != "" {
			sb.WriteString(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';\n", fullTableName, f.PName, f.PComment))
		}
	}

	// 5. table comment
	if t.PComment != "" {
		sb.WriteString(fmt.Sprintf("COMMENT ON TABLE %s IS '%s';\n", fullTableName, t.PComment))
	}

	// 6. index
	for _, idx := range t.PIndexes {
		indexName := fmt.Sprintf("idx_%s_%s", t.PName, strings.Join(idx, "_"))
		sb.WriteString(fmt.Sprintf("CREATE INDEX %s ON %s (%s);\n", indexName, fullTableName, strings.Join(idx, ", ")))
	}

	// 7. unique index
	for _, uidx := range t.PUniqueIndexes {
		indexName := fmt.Sprintf("uniq_%s_%s", t.PName, strings.Join(uidx, "_"))
		sb.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);\n", indexName, fullTableName, strings.Join(uidx, ", ")))
	}

	// 8. primary compose key
	if len(t.PCompositePrimaryKey) > 0 {
		sb.WriteString(fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s);\n", fullTableName, strings.Join(t.PCompositePrimaryKey, ", ")))
	}

	// 9. unique compose keys
	for _, uc := range t.PCompositeUniqueKeys {
		indexName := fmt.Sprintf("uniq_%s_%s", t.PName, strings.Join(uc, "_"))
		sb.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);\n", indexName, fullTableName, strings.Join(uc, ", ")))
	}

	return sb.String()
}

func (d *DBBridge) generateDropSchemaSql(name string) string {
	return fmt.Sprintf("DROP TABLE \"%s\"", d.schemaPrefix+name)
}
