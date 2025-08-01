package ndb

import (
	"fmt"
	"strings"
)

type CreateQuery struct {
	*BasicQuery
	Data M `json:"data"`
}

// just initialize table field
func NewCreateQuery(table string) *CreateQuery {
	return &CreateQuery{BasicQuery: &BasicQuery{BasicSchema: &BasicSchema{Schema: table}}}
}

func (dbb *DBBridge) Create(createQuery *CreateQuery) (any, error) {
	table, err := createQuery.GetSchema(dbb)
	if err != nil {
		return nil, err
	}

	if err := dbb.ValidateSchema(createQuery.Schema, createQuery.Data); err != nil {
		return nil, err
	}

	if len(createQuery.Data) == 0 {
		return nil, ErrEmptyCreateData
	}

	if err := dbb.runMiddlewares(createQuery); err != nil {
		return nil, err
	}

	var keys []string
	var placeholders []string
	var args []any
	pos := 1
	for k, v := range createQuery.Data {
		keys = append(keys, k)
		placeholders = append(placeholders, fmt.Sprintf("$%d", pos))
		args = append(args, v)
		pos++
	}

	selectFields, err := createQuery.GetSelect(dbb.schemaPrefix, true)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
		table,
		strings.Join(keys, ","),
		strings.Join(placeholders, ","),
		strings.Join(selectFields, ","),
	)

	result, err := dbb.ExecuteQuery(query, args...)
	if err != nil {
		return nil, err
	}

	return result[0], nil
}
