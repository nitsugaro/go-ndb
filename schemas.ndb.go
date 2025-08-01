package ndb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nitsugaro/go-ndb/cache"
)

func InEnum(val string, enum []string) bool {
	for _, v := range enum {
		if v == val {
			return true
		}
	}
	return false
}

func validateString(val string, f *Field) error {
	if f.Type != FieldText && f.Type != FieldVarchar && f.Type != FieldTimestamp && f.Type != FieldJSONB && f.Type != FieldUUID {
		return fmt.Errorf("field '%s': must be %s type", f.Name, f.Type)
	}

	if f.Max != nil && len(val) > *f.Max {
		return fmt.Errorf("field '%s': string max length is '%v'", f.Name, *f.Max)
	}

	if f.Min != nil && len(val) < *f.Min {
		return fmt.Errorf("field '%s': string min length is '%v'", f.Name, *f.Min)
	}

	if f.Pattern != nil {
		regex, err := cache.GetRegexp(*f.Pattern)
		if err != nil {
			return fmt.Errorf("field '%s': cannot apply regex '%s'", f.Name, *f.Pattern)
		}

		if !regex.MatchString(val) {
			return fmt.Errorf("field '%s': invalid regex value %s", f.Name, *f.Pattern)
		}
	}

	if f.EnumValues != nil && !InEnum(val, f.EnumValues) {
		return fmt.Errorf("field '%s': must be one of these values [%s]", f.Name, strings.Join(f.EnumValues, ", "))
	}

	if f.Type == FieldTimestamp {
		_, err := time.Parse(val, "2006-01-02T15:04:39.013Z")
		if err != nil {
			return fmt.Errorf("field '%s': invalid timestamp %s", f.Name, err.Error())
		}
	}

	return nil
}

func validateInt(val int64, f *Field) error {
	if f.Type != FieldBigInt && f.Type != FieldBigSerial && f.Type != FieldSmallInt && f.Type != FieldSmallSerial && f.Type != FieldInt {
		return fmt.Errorf("field '%s': must be %s type", f.Name, f.Type)
	}

	if f.Max != nil && int(val) > *f.Max {
		return fmt.Errorf("field '%s': max is '%v'", f.Name, *f.Max)
	}

	if f.Min != nil && int(val) < *f.Min {
		return fmt.Errorf("field '%s': min is '%v'", f.Name, *f.Min)
	}

	if f.EnumValues != nil && !InEnum(strconv.Itoa(int(val)), f.EnumValues) {
		return fmt.Errorf("field '%s': must be one of these values [%s]", f.Name, strings.Join(f.EnumValues, ", "))
	}

	return nil
}

func validateFloat(val float64, f *Field) error {
	if f.Type != FieldDouble && f.Type != FieldFloat {
		return fmt.Errorf("field '%s': must be %s type", f.Name, f.Type)
	}

	if f.Max != nil && val > float64(*f.Max) {
		return fmt.Errorf("field '%s': max is '%v'", f.Name, *f.Max)
	}

	if f.Min != nil && val < float64(*f.Min) {
		return fmt.Errorf("field '%s': min is '%v'", f.Name, *f.Min)
	}

	return nil
}

func (dbb *DBBridge) ValidateSchema(name string, data M) error {
	if dbb.schemaStorage == nil {
		return nil
	}

	schema, ok := dbb.GetSchema(name)
	if !ok {
		return ErrSchemaKeyNotFound
	}

	for _, f := range schema.Fields {
		val := data[f.Name]
		if val == nil && !f.Nullable && f.Default == nil && f.Type != FieldBigSerial && f.Type != FieldSmallSerial {
			return fmt.Errorf("field '%s': is required", f.Name)
		}

		var err error
		if val != nil {
			switch val := val.(type) {
			case bool:
				if f.Type != FieldBoolean {
					return fmt.Errorf("field '%s': must be %s type", f.Name, f.Type)
				}
			case int64:
				err = validateInt(val, &f)
			case int:
				err = validateInt(int64(val), &f)
			case float64:
				err = validateFloat(val, &f)
			case float32:
				err = validateFloat(float64(val), &f)
			case string:
				err = validateString(val, &f)
			default:
				return fmt.Errorf("field '%s': unsopported type only accept: [bool, int, int64, float32, float64, string]", f.Name)
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
		"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename LIKE '" + d.schemaPrefix + "%'",
	)
}
