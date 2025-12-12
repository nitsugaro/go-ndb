package ndb

import goutils "github.com/nitsugaro/go-utils"

func (q *Query) NewField(name string) *SQLField {
	f := &SQLField{q: q, Name: name, Operators: []*SQLOperation{}}
	q.PFields = append(q.PFields, f)
	return f
}

func (q *Query) Fields(fields ...string) *Query {
	q.PFields = goutils.Map(fields, func(f string, _ int) *SQLField { return F(f) })
	return q
}

func (u *Query) Where(conditions ...M) *Query {
	u.PWhere = conditions
	return u
}

func (q *Query) Payload(payload M) *Query {
	q.RPayload = payload
	return q
}

func (q *Query) NewJoin(schema string, typ JoinType) *Join {
	j := &Join{PTyp: typ, q: q, BasicSchema: &BasicSchema{Schema: schema}, POn: []M{}}
	q.PJoins = append(q.PJoins, j)
	return j
}

func (q *Query) Offset(offset int) *Query {
	q.POffset = offset
	return q
}

func (q *Query) Limit(limit int) *Query {
	q.PLimit = limit
	return q
}

func (q *Query) Order(orderSequence []*SQLField) *Query {
	q.POrderBy = orderSequence
	return q
}

func (q *Query) Group(groupSequence []*SQLField) *Query {
	q.PGroupBy = groupSequence
	return q
}

func (q *Query) SubQuery(query *Query, fields []*SQLField) *Query {
	return q.SubQueryName("", query, fields)
}

func (q *Query) SubQueryName(queryName string, query *Query, fields []*SQLField) *Query {
	if query.typ != READ || len(fields) == 0 {
		return q
	}

	q.subQuery = &SubQuery{Query: query, fields: fields, queryName: queryName}

	return q
}

func (q *Query) AddField(field *SQLField) *Query {
	q.PFields = append(q.PFields, field)
	return q
}

func (q *Query) AddJoin(clausureJoin *Join) *Query {
	q.PJoins = append(q.PJoins, clausureJoin)
	return q
}

func (q *Query) AddWhere(condition M) *Query {
	q.PWhere = append(q.PWhere, condition)
	return q
}

func (q *Query) AddPayload(key string, value any) *Query {
	q.RPayload[key] = value
	return q
}

func (q *Query) GetPayload() M {
	return q.RPayload
}

func (q *Query) GetOffset() int { return q.POffset }

func (q *Query) GetLimit() int {
	if q.PLimit == 0 {
		return 100
	}
	return q.PLimit
}
