package ndb

import (
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/fatih/color"
)

func (dbb *DBBridge) BuildDeleteQuery(deleteQuery *Query, returning bool) (string, []any, error) {
	if deleteQuery.typ != DELETE {
		return "", nil, ErrInvalidQueryType
	}

	schema, err := deleteQuery.GetSchema(dbb)
	if err != nil {
		return "", nil, err
	}

	if err := dbb.runPrevValidateMiddlewares(deleteQuery); err != nil {
		return "", nil, err
	}

	var (
		query = &strings.Builder{}
		args  []any
		pos   = 1
	)

	query.WriteString("DELETE FROM ")
	query.WriteString(schema)

	if deleteQuery.subQuery != nil {
		subSQL, subArgs, err := dbb.BuildReadQuery(deleteQuery.subQuery.Query)
		if err != nil {
			return "", nil, err
		}

		args = append(args, subArgs...)
		pos = len(args) + 1

		alias := deleteQuery.subQuery.queryName
		if alias == "" {
			alias = "sq"
		}

		query.WriteString(" USING (")
		query.WriteString(subSQL)
		query.WriteString(") AS ")
		query.WriteString(dbb.schemaPrefix + alias)
	}

	whereArgs, _, err := dbb.buildConditionClauseB(query, deleteQuery.PWhere, pos, "WHERE")
	if err != nil {
		return "", nil, err
	}
	args = append(args, whereArgs...)

	if returning {
		selectFields, err := deleteQuery.GetFormattedFields(dbb.schemaPrefix)
		if err != nil {
			return "", nil, err
		}

		query.WriteString(" RETURNING ")
		query.WriteString(strings.Join(selectFields, ","))
	}

	queryString := query.String()
	if logEnabled {
		color.Red(queryString)
	}

	return queryString, args, nil
}

func (dbb *DBBridge) DeleteWithFields(Query *Query) ([]M, error) {
	query, args, err := dbb.BuildDeleteQuery(Query, true)
	if err != nil {
		return nil, err
	}

	return dbb.ExecuteQuery(query, args...)
}

func (dbb *DBBridge) DeleteOneWithFields(Query *Query) (M, error) {
	query, args, err := dbb.BuildDeleteQuery(Query, true)
	if err != nil {
		return nil, err
	}

	if result, err := dbb.ExecuteQuery(query, args...); err != nil || len(result) == 0 {
		return nil, err
	} else {
		return result[0], nil
	}
}

func (dbb *DBBridge) DeleteWithFieldsB(Query *Query, v any) error {
	query, args, err := dbb.BuildDeleteQuery(Query, true)
	if err != nil {
		return err
	}

	if bytes, err := dbb.ExecuteQueryBytes(query, true, args...); err == nil {
		return json.Unmarshal(bytes, v)
	} else {
		return err
	}
}

func (dbb *DBBridge) DeleteOneWithFieldsB(Query *Query, v any) error {
	query, args, err := dbb.BuildDeleteQuery(Query, true)
	if err != nil {
		return err
	}

	if bytes, err := dbb.ExecuteQueryBytes(query, false, args...); err == nil {
		return json.Unmarshal(bytes, v)
	} else {
		return err
	}
}

func (dbb *DBBridge) DeleteWithRowsAffected(Query *Query) (int64, error) {
	query, args, err := dbb.BuildDeleteQuery(Query, false)
	if err != nil {
		return 0, err
	}

	var res sql.Result
	if dbb.db != nil {
		res, err = dbb.db.Exec(query, args...)
	} else {
		res, err = dbb.trx.Exec(query, args...)
	}

	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}
