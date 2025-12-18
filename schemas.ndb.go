// file: ndb/validate.go
package ndb

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"slices"

	"github.com/fatih/color"
	"github.com/lib/pq"
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

	// FIX: time.Parse(layout, value) (antes estaba al revés)
	if f.PType == FIELD_TIMESTAMP {
		// Aceptá RFC3339Nano y RFC3339 (tu validateTime coercer ya lo hace)
		if _, err := time.Parse(time.RFC3339Nano, val); err != nil {
			if _, err2 := time.Parse(time.RFC3339, val); err2 != nil {
				return fmt.Errorf("field '%s': invalid timestamp %s", f.PName, err2.Error())
			}
		}
	}

	return nil
}

func validateInt(val int, f *SchemaField) error {
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

// ---- Array type lookup ----

var arrayBaseType = map[SchemaFieldType]SchemaFieldType{
	FIELD_SMALL_INT_ARRAY: FIELD_SMALL_INT,
	FIELD_INT_ARRAY:       FIELD_INT,
	FIELD_BIG_INT_ARRAY:   FIELD_BIG_INT,
	FIELD_UUID_ARRAY:      FIELD_UUID,
	FIELD_TEXT_ARRAY:      FIELD_TEXT,
	FIELD_BOOLEAN_ARRAY:   FIELD_BOOLEAN,
	FIELD_TIMESTAMP_ARRAY: FIELD_TIMESTAMP,
	FIELD_JSONB_ARRAY:     FIELD_JSONB,
	FIELD_FLOAT_ARRAY:     FIELD_FLOAT,
	FIELD_DOUBLE_ARRAY:    FIELD_DOUBLE,
}

func arrayBase(t SchemaFieldType) (SchemaFieldType, bool) {
	bt, ok := arrayBaseType[t]
	return bt, ok
}

// ---- Type groups ----

func isIntType(t SchemaFieldType) bool {
	switch t {
	case FIELD_SMALL_INT, FIELD_INT, FIELD_BIG_INT,
		FIELD_SMALL_SERIAL, FIELD_SERIAL, FIELD_BIG_SERIAL:
		return true
	default:
		return false
	}
}

func isFloatType(t SchemaFieldType) bool {
	switch t {
	case FIELD_FLOAT, FIELD_DOUBLE:
		return true
	default:
		return false
	}
}

func isStringType(t SchemaFieldType) bool {
	switch t {
	case FIELD_VARCHAR, FIELD_TEXT, FIELD_UUID:
		return true
	default:
		return false
	}
}

// ---- Coercions (parse a lo que DEBERÍA ser) ----

func coerceBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case string:
		s := strings.TrimSpace(strings.ToLower(x))
		if s == "true" || s == "1" || s == "yes" {
			return true, true
		}
		if s == "false" || s == "0" || s == "no" {
			return false, true
		}
		return false, false
	case []byte:
		return coerceBool(string(x))
	default:
		return false, false
	}
}

func coerceInt(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int8:
		return int(x), true
	case int16:
		return int(x), true
	case int32:
		return int(x), true
	case int64:
		return int(x), true
	case uint:
		return int(x), true
	case uint8:
		return int(x), true
	case uint16:
		return int(x), true
	case uint32:
		return int(x), true
	case uint64:
		return int(x), true
	case float32:
		f := float64(x)
		if f == float64(int(f)) {
			return int(f), true
		}
		return 0, false
	case float64:
		if x == float64(int(x)) {
			return int(x), true
		}
		return 0, false
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return 0, false
		}
		return int(n), true
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return int(n), true
	case []byte:
		return coerceInt(string(x))
	default:
		return 0, false
	}
}

func coerceFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int16:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	case []byte:
		return coerceFloat64(string(x))
	default:
		return 0, false
	}
}

func coerceString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	default:
		return "", false
	}
}

func coerceTime(v any) (time.Time, bool) {
	switch x := v.(type) {
	case time.Time:
		return x, true
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return time.Time{}, false
		}
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t, true
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t, true
		}
		if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
			return t, true
		}
		if t, err := time.Parse("2006-01-02", s); err == nil {
			return t, true
		}
		return time.Time{}, false
	case []byte:
		return coerceTime(string(x))
	default:
		return time.Time{}, false
	}
}

func isValidJSONBytes(b []byte) bool {
	b = []byte(strings.TrimSpace(string(b)))
	return len(b) > 0 && json.Valid(b)
}

func validateJSONB(v any) bool {
	switch x := v.(type) {
	case map[string]any, []any:
		return true
	case json.RawMessage:
		return isValidJSONBytes(x)
	case []byte:
		return isValidJSONBytes(x)
	case string:
		return isValidJSONBytes([]byte(x))
	default:
		return false
	}
}

func validateScalarAndCoerce(val any, f *SchemaField) (any, error) {
	t := f.PType

	if t == FIELD_JSONB {
		if !validateJSONB(val) {
			return nil, fmt.Errorf("must be %s type", t)
		}
		return val, nil
	}

	if t == FIELD_TIMESTAMP {
		tv, ok := coerceTime(val)
		if !ok {
			return nil, fmt.Errorf("must be %s type", t)
		}
		return tv, nil
	}

	if t == FIELD_BOOLEAN {
		b, ok := coerceBool(val)
		if !ok {
			return nil, fmt.Errorf("must be %s type", t)
		}
		return b, nil
	}

	if isIntType(t) {
		i, ok := coerceInt(val)
		if !ok {
			return nil, fmt.Errorf("must be %s type", t)
		}
		if err := validateInt(i, f); err != nil {
			return nil, err
		}
		return i, nil
	}

	if isFloatType(t) {
		fv, ok := coerceFloat64(val)
		if !ok {
			return nil, fmt.Errorf("must be %s type", t)
		}
		if err := validateFloat(fv, f); err != nil {
			return nil, err
		}
		return fv, nil
	}

	if isStringType(t) {
		s, ok := coerceString(val)
		if !ok {
			return nil, fmt.Errorf("must be %s type", t)
		}
		if err := validateString(s, f); err != nil {
			return nil, err
		}
		return s, nil
	}

	return nil, fmt.Errorf("unsupported schema field type %s", t)
}

// ---- Arrays: lo importante ----
// database/sql NO acepta []int16 / []string como args.
// Se normaliza a pq.Array([]int64{...}) o pq.Array([]string{...})

func toInt64Slice(v any, field string) ([]int64, error) {
	switch x := v.(type) {
	case []int64:
		return x, nil
	case []int:
		out := make([]int64, len(x))
		for i := range x {
			out[i] = int64(x[i])
		}
		return out, nil
	case []int16:
		out := make([]int64, len(x))
		for i := range x {
			out[i] = int64(x[i])
		}
		return out, nil
	case []int32:
		out := make([]int64, len(x))
		for i := range x {
			out[i] = int64(x[i])
		}
		return out, nil
	case []uint:
		out := make([]int64, len(x))
		for i := range x {
			out[i] = int64(x[i])
		}
		return out, nil
	case []any:
		out := make([]int64, len(x))
		for i := range x {
			n, ok := coerceInt(x[i])
			if !ok {
				return nil, fmt.Errorf("field '%s'[%d]: must be int element", field, i)
			}
			out[i] = int64(n)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("field '%s': must be int array", field)
	}
}

func toStringSlice(v any, field string) ([]string, error) {
	switch x := v.(type) {
	case []string:
		return x, nil
	case []any:
		out := make([]string, len(x))
		for i := range x {
			s, ok := coerceString(x[i])
			if !ok {
				return nil, fmt.Errorf("field '%s'[%d]: must be string element", field, i)
			}
			out[i] = s
		}
		return out, nil
	default:
		return nil, fmt.Errorf("field '%s': must be string array", field)
	}
}

func validateAndCoerceArrayForSQL(val any, base SchemaFieldType, f *SchemaField) (any, error) {
	if _, ok := val.(driver.Valuer); ok {
		return val, nil
	}

	switch base {

	case FIELD_SMALL_INT, FIELD_INT, FIELD_BIG_INT:
		arr, err := toInt64Slice(val, f.PName)
		if err != nil {
			return nil, err
		}
		ef := *f
		ef.PType = base
		for i := range arr {
			if err := validateInt(int(arr[i]), &ef); err != nil {
				return nil, fmt.Errorf("field '%s'[%d]: %w", f.PName, i, err)
			}
		}
		return pq.Array(arr), nil

	case FIELD_TEXT, FIELD_VARCHAR, FIELD_UUID:
		arr, err := toStringSlice(val, f.PName)
		if err != nil {
			return nil, err
		}
		ef := *f
		ef.PType = base
		for i := range arr {
			if err := validateString(arr[i], &ef); err != nil {
				return nil, fmt.Errorf("field '%s'[%d]: %w", f.PName, i, err)
			}
		}
		return pq.Array(arr), nil

	default:
		return nil, fmt.Errorf("field '%s': unsupported array base type %s", f.PName, base)
	}
}

// ---- ValidateSchema (completo) ----

func (dbb *DBBridge) ValidateSchema(name string, queryType QueryType, data M) error {
	if dbb.schemaStorage == nil {
		return nil
	}

	schema, ok := dbb.GetSchemaByName(name)
	if !ok {
		return ErrSchemaKeyNotFound
	}

	for _, f := range schema.PFields {
		val, has := data[f.PName]

		if ((has && queryType == UPDATE) || queryType == CREATE) &&
			val == nil &&
			!f.PNullable &&
			f.PDefault == nil &&
			f.PType != FIELD_BIG_SERIAL &&
			f.PType != FIELD_SMALL_SERIAL {
			return fmt.Errorf("field '%s': is required", f.PName)
		}

		if !has || val == nil {
			continue
		}

		// ARRAY: valida y NORMALIZA a pq.Array(...) (driver friendly)
		if bt, isArr := arrayBase(f.PType); isArr {
			coercedArr, err := validateAndCoerceArrayForSQL(val, bt, f)
			if err != nil {
				return err
			}
			data[f.PName] = coercedArr
			continue
		}

		// SCALAR: coerce + validate
		coerced, err := validateScalarAndCoerce(val, f)
		if err != nil {
			return fmt.Errorf("field '%s': %w", f.PName, err)
		}
		data[f.PName] = coerced
	}

	return nil
}

// keep (solo para logs si ya lo usás acá)
func debugQuery(s string) {
	if logEnabled {
		color.Yellow(s)
	}
}

func (d *DBBridge) GetTables() ([]M, error) {
	return d.ExecuteQuery(
		"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename LIKE '" + d.schemaPrefix + "%';",
	)
}
