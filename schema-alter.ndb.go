package ndb

import (
	"fmt"
	"strings"
)

type AlterAction string

const (
	AddColumn   AlterAction = "add"
	AlterColumn AlterAction = "alter"
	DropColumn  AlterAction = "drop"
)

type AlterOptions struct {
	NewName   *string `json:"new_name"`
	OldUnique *bool
	IndexName *string
}

type AlterField struct {
	Field        *Field        `json:"field"`
	AlterAction  AlterAction   `json:"alter_action"`
	AlterOptions *AlterOptions `json:"alter_options"`
}

func genIndexName(schemaName, col string) string {
	return fmt.Sprintf("uniq_%s_%s", schemaName, col)
}

func (dbb *DBBridge) generateAlterSchemaSQL(schemaName string, fields []*AlterField) (string, *Schema, error) {
	fullTableName := fmt.Sprintf("\"%s%s\"", dbb.schemaPrefix, schemaName)
	var sql strings.Builder

	schema, ok := dbb.GetSchema(schemaName)
	if !ok {
		return "", nil, ErrSchemaKeyNotFound
	}

	newSchema := Ptr(*schema)
	for _, field := range fields {
		f := field.Field
		action := field.AlterAction
		opts := field.AlterOptions

		var sb strings.Builder
		switch action {
		case AddColumn:
			sb.WriteString(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", fullTableName, f.Name, string(f.Type)))
			if f.Type == FieldVarchar && f.Max != nil {
				sb.Reset()
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s(%d)", fullTableName, f.Name, f.Type, *f.Max))
			}
			if !f.Nullable {
				sb.WriteString(" NOT NULL")
			}
			if f.Default != nil {
				sb.WriteString(fmt.Sprintf(" DEFAULT %s", *f.Default))
			}
			sb.WriteString(";\n")

			if f.Unique {
				colName := f.Name
				indexName := genIndexName(schemaName, colName)
				if opts != nil && opts.IndexName != nil {
					indexName = *opts.IndexName
				}
				sb.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);\n", indexName, fullTableName, colName))
				newSchema.Indexes = append(schema.Indexes, []string{f.Name})
			}

			if !newSchema.AddField(f) {
				return "", nil, fmt.Errorf("field '%s': already exists and cannot be added", f.Name)
			}
		case AlterColumn:
			sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", fullTableName, f.Name, string(f.Type)))
			if f.Type == FieldVarchar && f.Max != nil {
				sb.Reset()
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s(%d)", fullTableName, f.Name, f.Type, *f.Max))
			}
			sb.WriteString(";\n")

			// Nullable
			if f.Nullable {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;\n", fullTableName, f.Name))
			} else {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;\n", fullTableName, f.Name))
			}

			// Default
			if f.Default != nil {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;\n", fullTableName, f.Name, *f.Default))
			} else {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;\n", fullTableName, f.Name))
			}

			// Rename columna
			if opts != nil && opts.NewName != nil && *opts.NewName != f.Name {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;\n", fullTableName, f.Name, *opts.NewName))
			}

			oldCol := f.Name
			newCol := f.Name

			if opts != nil && opts.NewName != nil {
				newCol = *opts.NewName
			}

			// Unique
			if opts != nil && opts.OldUnique != nil {
				oldUnique := *opts.OldUnique
				newUnique := f.Unique

				oldIndexName := genIndexName(schemaName, oldCol)
				newIndexName := genIndexName(schemaName, newCol)

				if opts.IndexName != nil {
					oldIndexName = *opts.IndexName
					newIndexName = *opts.IndexName
				}

				// DROP old unique index
				if oldUnique && !newUnique {
					sb.WriteString(fmt.Sprintf("DROP INDEX IF EXISTS %s;\n", oldIndexName))
				}

				// CREATE new unique index
				if !oldUnique && newUnique {
					sb.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);\n", newIndexName, fullTableName, newCol))
				}

				// Rename index if unique stayed but name changed
				if oldUnique && newUnique && oldCol != newCol && oldIndexName != newIndexName {
					sb.WriteString(fmt.Sprintf("ALTER INDEX %s RENAME TO %s;\n", oldIndexName, newIndexName))
				}

				newSchema.Indexes = append(schema.Indexes, []string{newCol})
			}

			f.Name = newCol
			if !newSchema.UpdateField(oldCol, f) {
				return "", nil, fmt.Errorf("field '%s': cannot be updated", f.Name)
			}
		case DropColumn:
			if !newSchema.RemoveField(f.Name) {
				return "", nil, fmt.Errorf("field '%s': cannot be removed", f.Name)
			}

			sb.WriteString(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", fullTableName, f.Name))
		}

		sql.WriteString(sb.String())
		sql.WriteString("\n\n")
	}

	return sql.String(), newSchema, nil
}
