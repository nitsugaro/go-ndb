package ndb

import (
	"strings"

	goutils "github.com/nitsugaro/go-utils"
)

type Operation int

const (
	DISTINCT Operation = iota
	COUNT
	SUM
	AVG
	MIN
	MAX
	AS
	LOWER
	UPPER

	STRING_AGG
	ARRAY_AGG
	BOOL_AND
	BOOL_OR
	JSON_AGG
	JSONB_AGG

	COALESCE
	NULLIF

	TRIM
	LENGTH
	SUBSTR

	ROUND
	FLOOR
	CEIL
	ABS

	NOW
	CURRENT_DATE
	DATE_TRUNC
)

var funcs = [256](func(name string, v ...string) string){
	DISTINCT:   func(name string, v ...string) string { return "DISTINCT " + name },
	COUNT:      func(name string, v ...string) string { return "COUNT(" + name + ")" },
	SUM:        func(name string, v ...string) string { return "SUM(" + name + ")" },
	AVG:        func(name string, v ...string) string { return "AVG(" + name + ")" },
	MIN:        func(name string, v ...string) string { return "MIN(" + name + ")" },
	MAX:        func(name string, v ...string) string { return "MAX(" + name + ")" },
	AS:         func(name string, v ...string) string { return name + " AS " + v[0] },
	LOWER:      func(name string, v ...string) string { return "LOWER(" + name + ")" },
	UPPER:      func(name string, v ...string) string { return "UPPER(" + name + ")" },
	STRING_AGG: func(name string, v ...string) string { return "STRING_AGG(" + name + ", " + v[0] + ")" },
	ARRAY_AGG:  func(name string, v ...string) string { return "ARRAY_AGG(" + name + ")" },
	BOOL_AND:   func(name string, v ...string) string { return "BOOL_AND(" + name + ")" },
	BOOL_OR:    func(name string, v ...string) string { return "BOOL_OR(" + name + ")" },
	JSON_AGG:   func(name string, v ...string) string { return "JSON_AGG(" + name + ")" },
	JSONB_AGG:  func(name string, v ...string) string { return "JSONB_AGG(" + name + ")" },
	COALESCE:   func(name string, v ...string) string { return "COALESCE(" + strings.Join(v, ",") + ")" },
	NULLIF:     func(name string, v ...string) string { return "NULLIF(" + name + "," + v[0] + ")" },
	TRIM:       func(name string, v ...string) string { return "TRIM(" + name + ")" },
	LENGTH:     func(name string, v ...string) string { return "LENGTH(" + name + ")" },
	SUBSTR: func(name string, v ...string) string { // col, from, [len]
		if len(v) == 1 {
			return "SUBSTRING(" + name + " FROM " + v[0] + ")"
		}
		return "SUBSTRING(" + name + " FROM " + v[0] + " FOR " + v[1] + ")"
	},
	ROUND: func(name string, v ...string) string { // n, [decimals]
		if len(v) == 1 {
			return "ROUND(" + name + "," + v[0] + ")"
		}

		return "ROUND(" + name + ")"
	},
	FLOOR:        func(name string, v ...string) string { return "FLOOR(" + name + ")" },
	CEIL:         func(name string, v ...string) string { return "CEIL(" + name + ")" },
	ABS:          func(name string, v ...string) string { return "ABS(" + name + ")" },
	NOW:          func(name string, v ...string) string { return "NOW()" },
	CURRENT_DATE: func(name string, v ...string) string { return "CURRENT_DATE" },
	DATE_TRUNC:   func(name string, v ...string) string { return "DATE_TRUNC(" + name + "," + v[0] + ")" }, // ('day', ts)
}

func (sf *SQLField) DoneField() *Query {
	return sf.q
}

func Fg(fields ...*SQLField) []*SQLField {
	return fields
}

func F(name string) *SQLField {
	return &SQLField{Name: name, Operators: []*SQLOperation{}}
}

func Fs(names ...string) []*SQLField {
	return goutils.Map(names, func(name string, _ int) *SQLField { return &SQLField{Name: name, Operators: []*SQLOperation{}} })
}

func (f *SQLField) GerForQuery(schemaPrefix string) (string, error) {
	name, err := FormatSQLField(schemaPrefix, f.Name)
	if err != nil {
		return "", err
	}

	goutils.ForEach(f.Operators, func(op *SQLOperation, _ int) {
		if funcs[op.Op] != nil {
			name = funcs[op.Op](name, op.Args...)
		}
	})

	return name, nil
}

func (f *SQLField) addFunc(op Operation, args ...string) *SQLField {
	f.Operators = append(f.Operators, &SQLOperation{Op: op, Args: args})
	return f
}

func (f *SQLField) Distinct() *SQLField       { return f.addFunc(DISTINCT) }
func (f *SQLField) Count() *SQLField          { return f.addFunc(COUNT) }
func (f *SQLField) Sum() *SQLField            { return f.addFunc(SUM) }
func (f *SQLField) Avg() *SQLField            { return f.addFunc(AVG) }
func (f *SQLField) Min() *SQLField            { return f.addFunc(MIN) }
func (f *SQLField) Max() *SQLField            { return f.addFunc(MAX) }
func (f *SQLField) Lower() *SQLField          { return f.addFunc(LOWER) }
func (f *SQLField) Upper() *SQLField          { return f.addFunc(UPPER) }
func (f *SQLField) StringAgg() *SQLField      { return f.addFunc(STRING_AGG) }
func (f *SQLField) ArrayAgg() *SQLField       { return f.addFunc(ARRAY_AGG) }
func (f *SQLField) BoolAnd() *SQLField        { return f.addFunc(BOOL_AND) }
func (f *SQLField) BoolOr() *SQLField         { return f.addFunc(BOOL_OR) }
func (f *SQLField) JsonAgg() *SQLField        { return f.addFunc(JSON_AGG) }
func (f *SQLField) JsonbAgg() *SQLField       { return f.addFunc(JSONB_AGG) }
func (f *SQLField) Coalesce() *SQLField       { return f.addFunc(COALESCE) }
func (f *SQLField) NullIf() *SQLField         { return f.addFunc(NULLIF) }
func (f *SQLField) Trim() *SQLField           { return f.addFunc(TRIM) }
func (f *SQLField) Length() *SQLField         { return f.addFunc(LENGTH) }
func (f *SQLField) Substr() *SQLField         { return f.addFunc(SUBSTR) }
func (f *SQLField) Round() *SQLField          { return f.addFunc(ROUND) }
func (f *SQLField) Floor() *SQLField          { return f.addFunc(FLOOR) }
func (f *SQLField) Ceil() *SQLField           { return f.addFunc(CEIL) }
func (f *SQLField) Abs() *SQLField            { return f.addFunc(ABS) }
func (f *SQLField) Now() *SQLField            { return f.addFunc(NOW) }
func (f *SQLField) CurrentDate() *SQLField    { return f.addFunc(CURRENT_DATE) }
func (f *SQLField) DateTrunc() *SQLField      { return f.addFunc(DATE_TRUNC) }
func (f *SQLField) As(alias string) *SQLField { return f.addFunc(AS, alias) }
