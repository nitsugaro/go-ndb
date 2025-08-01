package ndb

import (
	"fmt"
)

type DeleteQuery struct {
	*BasicQuery
}

// just initialize table field
func NewDeleteQuery(table string) *DeleteQuery {
	return &DeleteQuery{BasicQuery: &BasicQuery{BasicSchema: &BasicSchema{Schema: table}}}
}

func (dbb *DBBridge) Delete(deleteQuery *DeleteQuery) (any, error) {
	table, err := deleteQuery.GetSchema(dbb)
	if err != nil {
		return nil, err
	}

	if err := dbb.runMiddlewares(deleteQuery); err != nil {
		return nil, err
	}

	whereClause, args, _, err := dbb.buildConditionClause(deleteQuery.Where, 1, "WHERE")
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("DELETE FROM %s %s", table, whereClause)

	selectFields, err := deleteQuery.GetSelect(dbb.schemaPrefix, false)
	if err != nil {
		return nil, err
	}

	if len(selectFields) != 0 {
		return dbb.ExecuteQuery(query, args...)
	}

	res, err := dbb.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return res.RowsAffected()
}
