package ndb

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
)

func (b *DBBridge) ExecuteQuery(query string, args ...any) ([]M, error) {
	var rows *sql.Rows
	var err error
	if b.db != nil {
		rows, err = b.db.Query(query, args...)
	} else {
		rows, err = b.trx.Query(query, args...)
	}

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	cols, _ := rows.Columns()
	result := []M{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := M{}
		for i, col := range cols {
			v := reflect.ValueOf(vals[i])
			if v.Kind() == reflect.Slice {
				row[col] = fmt.Sprintf("%s", vals[i])
			} else {
				row[col] = vals[i]
			}
		}
		result = append(result, row)
	}

	return result, nil
}

/*
Returns response as bytes of a JSON structure. If 'arrayValue' is set to 'true', then the JSON will an array.
*/
func (b *DBBridge) ExecuteQueryBytes(query string, arrayValue bool, args ...any) ([]byte, error) {
	var rows *sql.Rows
	var err error
	if b.db != nil {
		rows, err = b.db.Query(query, args...)
	} else {
		rows, err = b.trx.Query(query, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer

	if arrayValue {
		out.WriteByte('[')
	}

	firstRow := true

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		if !firstRow {
			out.WriteByte(',')
		}
		firstRow = false

		out.WriteByte('{')
		for i, col := range cols {
			if i > 0 {
				out.WriteByte(',')
			}

			kb, _ := json.Marshal(col)
			out.Write(kb)
			out.WriteByte(':')

			vb, err := marshalSQLValue(vals[i])
			if err != nil {
				return nil, fmt.Errorf("marshal col=%s: %w", col, err)
			}
			out.Write(vb)
		}
		out.WriteByte('}')
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if arrayValue {
		out.WriteByte(']')
	}
	return out.Bytes(), nil
}

func marshalSQLValue(v any) ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}

	switch x := v.(type) {
	case []byte:
		return json.Marshal(string(x))
	case sql.RawBytes:
		return json.Marshal(string(x))
	default:
		return json.Marshal(x)
	}
}
