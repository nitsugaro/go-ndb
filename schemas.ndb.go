package ndb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"slices"

	"github.com/nitsugaro/go-ndb/cache"
)

func InEnum(val string, enum []string) bool {
	return slices.Contains(enum, val)
}

func validateString(val string, f *SchemaField) error {
	if f.PType != FIELD_TEXT && f.PType != FIELD_VARCHAR && f.PType != FIELD_TIMESTAMP && f.PType != FIELD_JSONB && f.PType != FIELD_UUID {
		return fmt.Errorf("field '%s': must be %s type", f.PName, f.PType)
	}

	if f.PMax != nil && len(val) > *f.PMax {
		return fmt.Errorf("field '%s': string max length is '%v'", f.PName, *f.PMax)
	}

	if f.PMin != nil && len(val) < *f.PMin {
		return fmt.Errorf("field '%s': string min length is '%v'", f.PName, *f.PMin)
	}

	if f.PPattern != nil {
		regex, err := cache.GetRegexp(*f.PPattern)
		if err != nil {
			return fmt.Errorf("field '%s': cannot apply regex '%s'", f.PName, *f.PPattern)
		}

		if !regex.MatchString(val) {
			return fmt.Errorf("field '%s': invalid regex value %s", f.PName, *f.PPattern)
		}
	}

	if f.PEnumValues != nil && !InEnum(val, f.PEnumValues) {
		return fmt.Errorf("field '%s': must be one of these values [%s]", f.PName, strings.Join(f.PEnumValues, ", "))
	}

	if f.PType == FIELD_TIMESTAMP {
		_, err := time.Parse(val, "2006-01-02T15:04:39.013Z")
		if err != nil {
			return fmt.Errorf("field '%s': invalid timestamp %s", f.PName, err.Error())
		}
	}

	return nil
}

func validateInt(val int64, f *SchemaField) error {
	if f.PType != FIELD_BIG_INT && f.PType != FIELD_BIG_SERIAL && f.PType != FIELD_SMALL_INT && f.PType != FIELD_SMALL_SERIAL && f.PType != FIELD_INT {
		return fmt.Errorf("field '%s': must be %s type", f.PName, f.PType)
	}

	if f.PMax != nil && int(val) > *f.PMax {
		return fmt.Errorf("field '%s': max is '%v'", f.PName, *f.PMax)
	}

	if f.PMin != nil && int(val) < *f.PMin {
		return fmt.Errorf("field '%s': min is '%v'", f.PName, *f.PMin)
	}

	if f.PEnumValues != nil && !InEnum(strconv.Itoa(int(val)), f.PEnumValues) {
		return fmt.Errorf("field '%s': must be one of these values [%s]", f.PName, strings.Join(f.PEnumValues, ", "))
	}

	return nil
}

func validateFloat(val float64, f *SchemaField) error {
	if f.PType != FIELD_DOUBLE && f.PType != FIELD_FLOAT {
		return fmt.Errorf("field '%s': must be %s type", f.PName, f.PType)
	}

	if f.PMax != nil && val > float64(*f.PMax) {
		return fmt.Errorf("field '%s': max is '%v'", f.PName, *f.PMax)
	}

	if f.PMin != nil && val < float64(*f.PMin) {
		return fmt.Errorf("field '%s': min is '%v'", f.PName, *f.PMin)
	}

	return nil
}

func (dbb *DBBridge) ValidateSchema(name string, queryType QueryType, data M) error {
	if dbb.schemaStorage == nil {
		return nil
	}

	schema, ok := dbb.GetSchemaByName(name)
	if !ok {
		return ErrSchemaKeyNotFound
	}

	for _, f := range schema.PFields {
		val, ok := data[f.PName]
		if ((ok && queryType == UPDATE) || queryType == CREATE) && val == nil && !f.PNullable && f.PDefault == nil && f.PType != FIELD_BIG_SERIAL && f.PType != FIELD_SMALL_SERIAL {
			return fmt.Errorf("field '%s': is required", f.PName)
		}

		var err error
		if val != nil {
			switch val := val.(type) {
			case bool:
				if f.PType != FIELD_BOOLEAN {
					return fmt.Errorf("field '%s': must be %s type", f.PName, f.PType)
				}
			case int64:
				err = validateInt(val, f)
			case int:
				err = validateInt(int64(val), f)
			case uint:
				err = validateInt(int64(val), f)
			case float64:
				err = validateFloat(val, f)
			case float32:
				err = validateFloat(float64(val), f)
			case string:
				err = validateString(val, f)
			default:
				return fmt.Errorf("field '%s': unsopported type only accept: [bool, int, uint, int64, float32, float64, string]", f.PName)
			}
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DBBridge) GetTables() ([]M, error) {
	return d.ExecuteQuery(
		"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename LIKE '" + d.schemaPrefix + "%';",
	)
}
