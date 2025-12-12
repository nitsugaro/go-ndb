package ndb

import (
	"encoding/json"
)

type BasicSchema struct {
	Schema string `json:"schema,omitempty"`
}

type SubQuery struct {
	queryName string
	fields    []*SQLField
	*Query
}

type SQLOperation struct {
	Op   Operation `json:"operation,omitempty"`
	Args []string  `json:"args,omitempty"`
}

type SQLField struct {
	Name      string          `json:"name,omitempty"`
	Operators []*SQLOperation `json:"operators,omitempty"`

	q *Query
}

type Query struct {
	*BasicSchema `json:"schema,omitempty"`
	typ          QueryType

	PFields  []*SQLField `json:"fields,omitempty"`
	PWhere   []M         `json:"where,omitempty"`
	PLimit   int         `json:"limit,omitempty"`
	POffset  int         `json:"offset,omitempty"`
	PGroupBy []*SQLField `json:"group_by,omitempty"`
	POrderBy []*SQLField `json:"order_by,omitempty"`
	PJoins   []*Join     `json:"joins,omitempty"`
	RPayload M           `json:"payload,omitempty"`

	subQuery *SubQuery
}

func (q *Query) Clone() *Query {
	return &Query{
		typ:      q.typ,
		PFields:  q.PFields,
		PWhere:   q.PWhere,
		PLimit:   q.PLimit,
		POffset:  q.POffset,
		PGroupBy: q.PGroupBy,
		POrderBy: q.POrderBy,
		PJoins:   q.PJoins,
		RPayload: q.RPayload,
		subQuery: q.subQuery,
	}
}

func (q *Query) Type() QueryType {
	return q.typ
}

func (q *Query) String() string {
	bytes, _ := json.Marshal(q)

	return string(bytes)
}

func (bt *BasicSchema) GetSchema(db *DBBridge) (string, error) {
	if bt.Schema == "" {
		return "", ErrNotFoundTable
	}

	return "\"" + db.GetSchemaPrefix() + bt.Schema + "\"", nil
}

func newQuery(table string, typ QueryType) *Query {
	return &Query{typ: typ, BasicSchema: &BasicSchema{Schema: table}}
}

func NewReadQuery(table string) *Query { return newQuery(table, READ) }

func NewCreateQuery(table string) *Query { return newQuery(table, CREATE) }

func NewUpdateQuery(table string) *Query { return newQuery(table, UPDATE) }

func NewDeleteQuery(table string) *Query { return newQuery(table, DELETE) }

func (dbo *Query) GetSelect(schemaPrefix string) ([]string, error) {
	if len(dbo.PFields) == 0 {
		return []string{"*"}, nil
	}

	return ValidParseSqlFields(schemaPrefix, dbo.PFields)
}
