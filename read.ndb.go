package ndb

import (
	"fmt"
	"strings"
)

type JoinType string

const (
	InnerJoin JoinType = "INNER"
	LeftJoin  JoinType = "LEFT"
	RightJoin JoinType = "RIGHT"
	FullJoin  JoinType = "FULL OUTER"
	CrossJoin JoinType = "CROSS"
)

type Join struct {
	*BasicSchema
	Type JoinType `json:"type"`
	On   []M      `json:"on"`
}

type ReadQuery struct {
	*BasicQuery
	Limit   int      `json:"limit"`
	Offset  int      `json:"offset"`
	OrderBy []string `json:"order_by"`
	Joins   []*Join  `json:"joins"`
}

func (r *ReadQuery) GetLimit() int {
	if r.Limit == 0 {
		return 1000
	}

	return r.Limit
}

// just initialize table field
func NewReadQuery(table string) *ReadQuery {
	return &ReadQuery{BasicQuery: &BasicQuery{BasicSchema: &BasicSchema{Schema: table}}}
}

func (dbb *DBBridge) Read(readQuery *ReadQuery) (any, error) {
	tableName, err := readQuery.GetSchema(dbb)
	if err != nil {
		return nil, err
	}

	if err := dbb.runMiddlewares(readQuery); err != nil {
		return nil, err
	}

	whereClause, args, pos, err := dbb.buildConditionClause(readQuery.Where, 1, "WHERE")
	if err != nil {
		return nil, err
	}

	selectFields, err := readQuery.GetSelect(dbb.schemaPrefix, true)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectFields, ","), tableName)

	// JOINS
	pos += 1 // next arg
	for _, join := range readQuery.Joins {
		joinTable, err := join.GetSchema(dbb)
		if err != nil {
			return nil, err
		}

		joinType := strings.ToUpper(string(join.Type))
		if join.Type != "" && !allowedJoins[joinType] {
			return nil, ErrUnsuporrtedJoinType
		} else {
			joinType = "INNER"
		}

		onClause, onArgs, newPos, err := dbb.buildConditionClause(join.On, pos, "")
		if err != nil {
			return nil, fmt.Errorf("invalid ON clause for join %s: %w", join.Schema, err)
		}
		pos = newPos
		args = append(args, onArgs...)

		query += fmt.Sprintf(" %s JOIN %s ON %s", joinType, joinTable, onClause)
	}

	if whereClause != "" {
		query += " " + whereClause
	}

	if len(readQuery.OrderBy) == 2 && validName.MatchString(readQuery.OrderBy[0]) {
		order := strings.ToUpper(readQuery.OrderBy[1])
		if order == "ASC" || order == "DESC" {
			query += fmt.Sprintf(" ORDER BY %s %s", readQuery.OrderBy[0], order)
		}
	}

	query += fmt.Sprintf(" LIMIT %d", readQuery.GetLimit())

	if readQuery.Offset != 0 {
		query += fmt.Sprintf(" OFFSET %d", readQuery.Offset)
	}

	return dbb.ExecuteQuery(query, args...)
}
