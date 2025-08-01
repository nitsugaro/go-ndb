package ndb

import (
	"fmt"
	"strings"
)

type UpdateQuery struct {
	*BasicQuery
	Data M `json:"data"`
}

// just initialize table field
func NewUpdateQuery(table string) *UpdateQuery {
	return &UpdateQuery{BasicQuery: &BasicQuery{BasicSchema: &BasicSchema{Schema: table}}}
}

func (dbb *DBBridge) Update(updateQuery *UpdateQuery) (any, error) {
	tableName, err := updateQuery.GetSchema(dbb)
	if err != nil {
		return nil, err
	}

	if err := dbb.ValidateSchema(updateQuery.Schema, updateQuery.Data); err != nil {
		return nil, err
	}

	if len(updateQuery.Data) == 0 {
		return nil, ErrEmptyUpdateData
	}

	if err := dbb.runMiddlewares(updateQuery); err != nil {
		return nil, err
	}

	var sets []string
	var args []any
	pos := 1
	for k, v := range updateQuery.Data {
		sets = append(sets, fmt.Sprintf("%s = $%d", k, pos))
		args = append(args, v)
		pos++
	}

	whereClause, whereArgs, _, err := dbb.buildConditionClause(updateQuery.Where, pos, "WHERE")
	if err != nil {
		return nil, err
	}

	args = append(args, whereArgs...)
	query := fmt.Sprintf("UPDATE %s SET %s %s", tableName, strings.Join(sets, ","), whereClause)

	fields, err := updateQuery.GetSelect(dbb.schemaPrefix, false)
	if err != nil {
		return nil, err
	}

	if len(fields) > 0 {
		query += fmt.Sprintf(" RETURNING %s", strings.Join(fields, ","))
		return dbb.ExecuteQuery(query, args...)
	}

	res, err := dbb.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return res.RowsAffected()
}
