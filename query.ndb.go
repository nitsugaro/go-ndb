package ndb

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

type colKind uint8

const (
	kindAny colKind = iota
	kindJSONBytes
	kindI64
	kindF64
	kindStr
	kindBool
	kindTime
	kindNumBytes
	kindArrI64
	kindArrStr
	kindArrBool
	kindArrF64
)

type colPlan struct {
	kind  colKind
	ptr   any
	key   []byte
	dbTyp string
}

func (b *DBBridge) queryRows(query string, args ...any) (*sql.Rows, error) {
	if b.db != nil {
		return b.db.Query(query, args...)
	}
	return b.trx.Query(query, args...)
}

func (b *DBBridge) ExecuteQuery(query string, args ...any) ([]M, error) {
	rows, err := b.queryRows(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	plans := make([]colPlan, len(cols))
	ptrs := make([]any, len(cols))

	for i := range cols {
		t := normalizeDBType(ct[i].DatabaseTypeName())
		p := makeColPlanForMap(t)
		p.dbTyp = t
		plans[i] = p
		ptrs[i] = p.ptr
	}

	out := make([]M, 0, 8)

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		row := M{}
		for i := range cols {
			v, err := readPlannedValueForMap(plans[i])
			if err != nil {
				return nil, fmt.Errorf("col=%s type=%s: %w", cols[i], plans[i].dbTyp, err)
			}
			row[cols[i]] = v
		}
		out = append(out, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (b *DBBridge) ExecuteQueryBytes(query string, arrayValue bool, args ...any) ([]byte, error) {
	rows, err := b.queryRows(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	plans := make([]colPlan, len(cols))
	ptrs := make([]any, len(cols))

	for i := range cols {
		kb, _ := json.Marshal(cols[i])
		key := append(kb, ':')

		t := normalizeDBType(ct[i].DatabaseTypeName())
		p := makeColPlanForJSON(t)
		p.key = key
		p.dbTyp = t
		plans[i] = p
		ptrs[i] = p.ptr
	}

	var out bytes.Buffer
	if arrayValue {
		out.WriteByte('[')
	}

	firstRow := true

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		if !firstRow {
			out.WriteByte(',')
		}
		firstRow = false

		out.WriteByte('{')
		for i := range plans {
			if i > 0 {
				out.WriteByte(',')
			}
			out.Write(plans[i].key)

			vb, err := readPlannedValueForJSON(plans[i])
			if err != nil {
				return nil, fmt.Errorf("col=%s type=%s: %w", cols[i], plans[i].dbTyp, err)
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

func normalizeDBType(t string) string {
	t = strings.ToUpper(strings.TrimSpace(t))
	if strings.HasSuffix(t, "[]") {
		return t
	}
	if strings.HasPrefix(t, "_") {
		return t
	}
	return t
}

func makeColPlanForJSON(t string) colPlan {
	if isJSONTypeName(t) {
		v := new([]byte)
		return colPlan{kind: kindJSONBytes, ptr: v}
	}

	if isArrayTypeName(t) {
		base := arrayBaseName(t)
		switch base {
		case "INT2", "INT4", "INT8":
			a := &pq.Int64Array{}
			return colPlan{kind: kindArrI64, ptr: a}
		case "TEXT", "VARCHAR", "UUID":
			a := &pq.StringArray{}
			return colPlan{kind: kindArrStr, ptr: a}
		case "BOOL":
			a := &pq.BoolArray{}
			return colPlan{kind: kindArrBool, ptr: a}
		case "FLOAT4", "FLOAT8", "NUMERIC":
			a := &pq.Float64Array{}
			return colPlan{kind: kindArrF64, ptr: a}
		default:
			a := &pq.StringArray{}
			return colPlan{kind: kindArrStr, ptr: a}
		}
	}

	switch t {
	case "INT2", "INT4", "INT8":
		v := &sql.NullInt64{}
		return colPlan{kind: kindI64, ptr: v}
	case "FLOAT4", "FLOAT8":
		v := &sql.NullFloat64{}
		return colPlan{kind: kindF64, ptr: v}
	case "NUMERIC":
		v := new([]byte)
		return colPlan{kind: kindNumBytes, ptr: v}
	case "TEXT", "VARCHAR", "UUID":
		v := &sql.NullString{}
		return colPlan{kind: kindStr, ptr: v}
	case "BOOL":
		v := &sql.NullBool{}
		return colPlan{kind: kindBool, ptr: v}
	case "TIMESTAMP", "TIMESTAMPTZ", "DATE":
		v := &sql.NullTime{}
		return colPlan{kind: kindTime, ptr: v}
	default:
		v := new(any)
		return colPlan{kind: kindAny, ptr: v}
	}
}

func makeColPlanForMap(t string) colPlan {
	return makeColPlanForJSON(t)
}

func readPlannedValueForJSON(p colPlan) ([]byte, error) {
	switch p.kind {
	case kindJSONBytes:
		b := *(p.ptr.(*[]byte))
		if len(b) == 0 || !json.Valid(b) {
			return []byte("null"), nil
		}
		return b, nil

	case kindI64:
		v := *(p.ptr.(*sql.NullInt64))
		if !v.Valid {
			return []byte("null"), nil
		}
		return strconv.AppendInt(nil, v.Int64, 10), nil

	case kindF64:
		v := *(p.ptr.(*sql.NullFloat64))
		if !v.Valid {
			return []byte("null"), nil
		}
		return []byte(strconv.FormatFloat(v.Float64, 'f', -1, 64)), nil

	case kindNumBytes:
		b := *(p.ptr.(*[]byte))
		if len(b) == 0 {
			return []byte("null"), nil
		}
		s := strings.TrimSpace(string(b))
		if s == "" {
			return []byte("null"), nil
		}
		return []byte(s), nil

	case kindStr:
		v := *(p.ptr.(*sql.NullString))
		if !v.Valid {
			return []byte("null"), nil
		}
		return json.Marshal(v.String)

	case kindBool:
		v := *(p.ptr.(*sql.NullBool))
		if !v.Valid {
			return []byte("null"), nil
		}
		if v.Bool {
			return []byte("true"), nil
		}
		return []byte("false"), nil

	case kindTime:
		v := *(p.ptr.(*sql.NullTime))
		if !v.Valid {
			return []byte("null"), nil
		}
		return json.Marshal(v.Time)

	case kindArrI64:
		a := *(p.ptr.(*pq.Int64Array))
		if a == nil {
			return []byte("null"), nil
		}
		return json.Marshal([]int64(a))

	case kindArrStr:
		a := *(p.ptr.(*pq.StringArray))
		if a == nil {
			return []byte("null"), nil
		}
		return json.Marshal([]string(a))

	case kindArrBool:
		a := *(p.ptr.(*pq.BoolArray))
		if a == nil {
			return []byte("null"), nil
		}
		return json.Marshal([]bool(a))

	case kindArrF64:
		a := *(p.ptr.(*pq.Float64Array))
		if a == nil {
			return []byte("null"), nil
		}
		return json.Marshal([]float64(a))

	default:
		v := *(p.ptr.(*any))
		if v == nil {
			return []byte("null"), nil
		}
		if bb, ok := v.([]byte); ok {
			return json.Marshal(string(bb))
		}
		return json.Marshal(v)
	}
}

func readPlannedValueForMap(p colPlan) (any, error) {
	switch p.kind {
	case kindJSONBytes:
		b := *(p.ptr.(*[]byte))
		if len(b) == 0 || !json.Valid(b) {
			return nil, nil
		}
		var out any
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, err
		}
		return out, nil

	case kindI64:
		v := *(p.ptr.(*sql.NullInt64))
		if !v.Valid {
			return nil, nil
		}
		return v.Int64, nil

	case kindF64:
		v := *(p.ptr.(*sql.NullFloat64))
		if !v.Valid {
			return nil, nil
		}
		return v.Float64, nil

	case kindNumBytes:
		b := *(p.ptr.(*[]byte))
		if len(b) == 0 {
			return nil, nil
		}
		s := strings.TrimSpace(string(b))
		if s == "" {
			return nil, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return s, nil
		}
		return f, nil

	case kindStr:
		v := *(p.ptr.(*sql.NullString))
		if !v.Valid {
			return nil, nil
		}
		return v.String, nil

	case kindBool:
		v := *(p.ptr.(*sql.NullBool))
		if !v.Valid {
			return nil, nil
		}
		return v.Bool, nil

	case kindTime:
		v := *(p.ptr.(*sql.NullTime))
		if !v.Valid {
			return nil, nil
		}
		return v.Time, nil

	case kindArrI64:
		a := *(p.ptr.(*pq.Int64Array))
		if a == nil {
			return nil, nil
		}
		return []int64(a), nil

	case kindArrStr:
		a := *(p.ptr.(*pq.StringArray))
		if a == nil {
			return nil, nil
		}
		return []string(a), nil

	case kindArrBool:
		a := *(p.ptr.(*pq.BoolArray))
		if a == nil {
			return nil, nil
		}
		return []bool(a), nil

	case kindArrF64:
		a := *(p.ptr.(*pq.Float64Array))
		if a == nil {
			return nil, nil
		}
		return []float64(a), nil

	default:
		v := *(p.ptr.(*any))
		if v == nil {
			return nil, nil
		}
		if bb, ok := v.([]byte); ok {
			return string(bb), nil
		}
		return v, nil
	}
}

func isJSONTypeName(t string) bool {
	t = strings.ToUpper(strings.TrimSpace(t))
	return t == "JSON" || t == "JSONB"
}

func isArrayTypeName(t string) bool {
	t = strings.ToUpper(strings.TrimSpace(t))
	return strings.HasPrefix(t, "_") || strings.HasSuffix(t, "[]")
}

func arrayBaseName(t string) string {
	t = strings.ToUpper(strings.TrimSpace(t))
	if strings.HasPrefix(t, "_") {
		t = t[1:]
	}
	if strings.HasSuffix(t, "[]") {
		t = strings.TrimSuffix(t, "[]")
	}
	return t
}
