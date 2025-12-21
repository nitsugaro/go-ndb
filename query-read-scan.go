package ndb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/lib/pq"
)

func scanIntoMaps(rows *sql.Rows, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer to a slice")
	}
	sliceVal := rv.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to a slice")
	}

	cols, _ := rows.Columns()

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		m := make(map[string]any, len(cols))
		for i, col := range cols {
			v := vals[i]
			switch x := v.(type) {
			case []byte:
				s := string(x)
				if len(s) > 0 && (s[0] == '{' || s[0] == '[') && json.Valid(x) {
					var out any
					if err := json.Unmarshal(x, &out); err == nil {
						m[col] = out
						continue
					}
				}
				m[col] = s
			default:
				m[col] = v
			}
		}

		sliceVal.Set(reflect.Append(sliceVal, reflect.ValueOf(m)))
	}

	return rows.Err()
}

func scanOneIntoMap(rows *sql.Rows, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer to a map")
	}
	mVal := rv.Elem()
	if mVal.Kind() != reflect.Map {
		return fmt.Errorf("dest must be pointer to map")
	}

	cols, _ := rows.Columns()

	if !rows.Next() {
		return rows.Err()
	}

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	if err := rows.Scan(ptrs...); err != nil {
		return err
	}

	if mVal.IsNil() {
		mVal.Set(reflect.MakeMap(mVal.Type()))
	}

	for i, col := range cols {
		v := vals[i]
		var out any
		switch x := v.(type) {
		case []byte:
			s := string(x)
			if len(s) > 0 && (s[0] == '{' || s[0] == '[') && json.Valid(x) {
				if err := json.Unmarshal(x, &out); err == nil {
					mVal.SetMapIndex(reflect.ValueOf(col), reflect.ValueOf(out))
					continue
				}
			}
			out = s
		default:
			out = v
		}
		mVal.SetMapIndex(reflect.ValueOf(col), reflect.ValueOf(out))
	}

	return nil
}

func scanIntoStructs(rows *sql.Rows, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer to a slice")
	}
	sliceVal := rv.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to a slice")
	}

	cols, _ := rows.Columns()
	elemType := sliceVal.Type().Elem()

	fieldIndex := makeFieldIndex(elemType)

	ptrs, setters := makeScanPlan(cols, elemType, fieldIndex)

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		itemVal := reflect.New(elemType).Elem()
		for i := range setters {
			if setters[i] != nil {
				if err := setters[i](itemVal); err != nil {
					return err
				}
			}
		}

		sliceVal.Set(reflect.Append(sliceVal, itemVal))
	}

	return rows.Err()
}

func scanOneIntoStruct(rows *sql.Rows, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("dest must be pointer to struct")
	}
	elemPtr := rv.Elem()
	if elemPtr.Kind() == reflect.Ptr {
		if elemPtr.IsNil() {
			elemPtr.Set(reflect.New(elemPtr.Type().Elem()))
		}
		elemPtr = elemPtr.Elem()
	}

	if elemPtr.Kind() != reflect.Struct {
		return fmt.Errorf("ReadOneB target must be struct pointer")
	}

	cols, _ := rows.Columns()
	fieldIndex := makeFieldIndex(elemPtr.Type())
	ptrs, setters := makeScanPlan(cols, elemPtr.Type(), fieldIndex)

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := rows.Scan(ptrs...); err != nil {
		return err
	}

	for i := range setters {
		if setters[i] != nil {
			if err := setters[i](elemPtr); err != nil {
				return err
			}
		}
	}

	return nil
}

func makeFieldIndex(t reflect.Type) map[string]int {
	idx := make(map[string]int, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		tag := f.Tag.Get("json")
		if tag != "" && tag != "-" {
			name := strings.Split(tag, ",")[0]
			if name != "" {
				idx[name] = i
			}
		}
		idx[strings.ToLower(f.Name)] = i
	}
	return idx
}

type setter func(dst reflect.Value) error

func makeScanPlan(cols []string, structType reflect.Type, idx map[string]int) ([]any, []setter) {
	ptrs := make([]any, len(cols))
	setters := make([]setter, len(cols))

	for i, col := range cols {
		fieldPos, ok := idx[col]
		if !ok {
			var sink any
			ptrs[i] = &sink
			setters[i] = nil
			continue
		}

		field := structType.Field(fieldPos)
		fieldType := field.Type

		switch {
		case fieldType == reflect.TypeOf(time.Time{}):
			v := new(sql.NullTime)
			ptrs[i] = v
			setters[i] = func(dst reflect.Value) error {
				if v.Valid {
					dst.Field(fieldPos).Set(reflect.ValueOf(v.Time))
				}
				return nil
			}

		case fieldType.Kind() == reflect.String:
			v := new(sql.NullString)
			ptrs[i] = v
			setters[i] = func(dst reflect.Value) error {
				if v.Valid {
					dst.Field(fieldPos).SetString(v.String)
				}
				return nil
			}

		case fieldType.Kind() == reflect.Bool:
			v := new(sql.NullBool)
			ptrs[i] = v
			setters[i] = func(dst reflect.Value) error {
				if v.Valid {
					dst.Field(fieldPos).SetBool(v.Bool)
				}
				return nil
			}

		case fieldType.Kind() == reflect.Float32 || fieldType.Kind() == reflect.Float64:
			v := new(sql.NullFloat64)
			ptrs[i] = v
			setters[i] = func(dst reflect.Value) error {
				if v.Valid {
					dst.Field(fieldPos).SetFloat(v.Float64)
				}
				return nil
			}

		case fieldType.Kind() >= reflect.Int && fieldType.Kind() <= reflect.Int64:
			v := new(sql.NullInt64)
			ptrs[i] = v
			setters[i] = func(dst reflect.Value) error {
				if v.Valid {
					dst.Field(fieldPos).SetInt(v.Int64)
				}
				return nil
			}

		case fieldType.Kind() >= reflect.Uint && fieldType.Kind() <= reflect.Uint64:
			v := new(sql.NullInt64)
			ptrs[i] = v
			setters[i] = func(dst reflect.Value) error {
				if v.Valid {
					dst.Field(fieldPos).SetUint(uint64(v.Int64))
				}
				return nil
			}

		case fieldType.Kind() == reflect.Slice && fieldType.Elem().Kind() == reflect.String:
			a := new(pq.StringArray)
			ptrs[i] = a
			setters[i] = func(dst reflect.Value) error {
				if *a != nil {
					dst.Field(fieldPos).Set(reflect.ValueOf([]string(*a)))
				}
				return nil
			}

		case fieldType.Kind() == reflect.Slice && fieldType.Elem().Kind() == reflect.Int16:
			a := new(pq.Int64Array)
			ptrs[i] = a
			setters[i] = func(dst reflect.Value) error {
				if *a != nil {
					int64Array := []int64(*a)
					out := make([]int16, len(int64Array))
					for j := range int64Array {
						out[j] = int16(int64Array[j])
					}
					dst.Field(fieldPos).Set(reflect.ValueOf(out))
				}
				return nil
			}

		case fieldType.Kind() == reflect.Map && fieldType.Key().Kind() == reflect.String && fieldType.Elem().Kind() == reflect.Interface:
			v := new([]byte)
			ptrs[i] = v
			setters[i] = func(dst reflect.Value) error {
				if len(*v) > 0 {
					var m map[string]any
					_ = json.Unmarshal(*v, &m)
					dst.Field(fieldPos).Set(reflect.ValueOf(m))
				}
				return nil
			}

		default:
			var sink any
			ptrs[i] = &sink
			setters[i] = nil
		}
	}

	return ptrs, setters
}
