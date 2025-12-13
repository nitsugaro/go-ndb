package ndb

import (
	"fmt"
	"strings"

	goutils "github.com/nitsugaro/go-utils"
)

type AlterOptions struct {
	NewName   *string `json:"new_name"`
	OldUnique *bool   `json:"old_unique"`
	IndexName *string `json:"index_name"`
}

type AlterField struct {
	Field        *SchemaField  `json:"field"`
	AlterAction  AlterAction   `json:"alter_action"`
	AlterOptions *AlterOptions `json:"alter_options"`
}

func genIndexName(schemaName, col string) string {
	return fmt.Sprintf("uniq_%s_%s", schemaName, col)
}

func (dbb *DBBridge) generateAlterSchemaSQL(schemaName string, fields []*AlterField) (string, *Schema, error) {
	fullTableName := fmt.Sprintf("\"%s%s\"", dbb.schemaPrefix, schemaName)
	var sql strings.Builder

	schema, ok := dbb.GetSchemaByName(schemaName)
	if !ok {
		return "", nil, ErrSchemaKeyNotFound
	}

	newSchema := Ptr(*schema)
	newSchema.PFields = goutils.Map(newSchema.PFields, func(f *SchemaField, _ int) *SchemaField { return Ptr(*f) })
	for _, field := range fields {
		f := field.Field
		action := field.AlterAction
		opts := field.AlterOptions

		var sb strings.Builder
		switch action {
		case ADD_COLUMN:
			sb.WriteString(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", fullTableName, f.PName, string(f.PType)))
			if f.PType == FIELD_VARCHAR && f.PMax != nil {
				sb.Reset()
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s(%d)", fullTableName, f.PName, f.PType, *f.PMax))
			}
			if !f.PNullable {
				sb.WriteString(" NOT NULL")
			}
			if f.PDefault != nil {
				sb.WriteString(fmt.Sprintf(" DEFAULT %s", *f.PDefault))
			}
			sb.WriteString(";\n")

			if f.PUnique {
				colName := f.PName
				indexName := genIndexName(schemaName, colName)
				if opts != nil && opts.IndexName != nil {
					indexName = *opts.IndexName
				}
				sb.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s);\n", indexName, fullTableName, colName))
				newSchema.PIndexes = append(schema.PIndexes, []string{f.PName})
			}

			if newSchema.AddField(f).err != nil {
				return "", nil, newSchema.err
			}
		case ALTER_COLUMN:
			sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", fullTableName, f.PName, string(f.PType)))
			if f.PType == FIELD_VARCHAR && f.PMax != nil {
				sb.Reset()
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s(%d)", fullTableName, f.PName, f.PType, *f.PMax))
			}
			sb.WriteString(";\n")

			// Nullable
			if f.PNullable {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;\n", fullTableName, f.PName))
			} else {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;\n", fullTableName, f.PName))
			}

			// Default
			if f.PDefault != nil {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;\n", fullTableName, f.PName, *f.PDefault))
			} else {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;\n", fullTableName, f.PName))
			}

			// Rename columna
			if opts != nil && opts.NewName != nil && *opts.NewName != f.PName {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;\n", fullTableName, f.PName, *opts.NewName))
			}

			oldCol := f.PName
			newCol := f.PName

			if opts != nil && opts.NewName != nil {
				newCol = *opts.NewName
			}

			// Unique
			if opts != nil && opts.OldUnique != nil {
				oldUnique := *opts.OldUnique
				newUnique := f.PUnique

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

				newSchema.PIndexes = append(schema.PIndexes, []string{newCol})
			}

			f.PName = newCol
			if newSchema.UpdateField(oldCol, f).err != nil {
				return "", nil, newSchema.err
			}
		case DROP_COLUMN:
			if newSchema.RemoveField(f.PName).err != nil {
				return "", nil, newSchema.err
			}

			sb.WriteString(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", fullTableName, f.PName))
		}

		sql.WriteString(sb.String())
		sql.WriteString("\n\n")
	}

	return sql.String(), newSchema, nil
}
