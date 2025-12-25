package ndb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fatih/color"
)

func (dbb *DBBridge) BuildUpdateQuery(updateQuery *Query, returning bool) (string, []any, error) {
	if updateQuery.typ != UPDATE {
		return "", nil, ErrInvalidQueryType
	}

	tableName, err := updateQuery.GetSchema(dbb)
	if err != nil {
		return "", nil, err
	}

	if updateQuery.RPayload != nil {
		if err := dbb.runPrevValidateMiddlewares(updateQuery); err != nil {
			return "", nil, err
		}

		if err := dbb.ValidateSchema(updateQuery.PSchema, UPDATE, updateQuery.RPayload); err != nil {
			return "", nil, err
		}

		if err := dbb.runPostValidateMiddlewares(updateQuery); err != nil {
			return "", nil, err
		}

		var (
			sets []string
			args []any
			pos  = 1
		)

		for k, v := range updateQuery.RPayload {
			sets = append(sets, fmt.Sprintf("%s = $%d", k, pos))
			args = append(args, v)
			pos++
		}

		query := &strings.Builder{}
		query.WriteString("UPDATE ")
		query.WriteString(tableName)
		query.WriteString(" SET ")
		query.WriteString(strings.Join(sets, ","))
		query.WriteByte(' ')

		whereArgs, _, err := dbb.buildConditionClauseB(query, updateQuery.PWhere, pos, "WHERE")
		if err != nil {
			return "", nil, err
		}

		args = append(args, whereArgs...)

		if returning {
			fields, err := updateQuery.GetFormattedFields(dbb.schemaPrefix)
			if err != nil {
				return "", nil, err
			}

			query.WriteString(" RETURNING ")
			query.WriteString(strings.Join(fields, ","))
		}

		queryStr := query.String()

		if logEnabled {
			color.Magenta(queryStr)
		}

		return queryStr, args, nil
	}

	if updateQuery.subQuery != nil {
		subSQL, subArgs, err := dbb.BuildReadQuery(updateQuery.subQuery.Query)
		if err != nil {
			return "", nil, err
		}

		cols, err := ValidParseSqlFields(dbb.schemaPrefix, updateQuery.subQuery.fields)
		if err != nil {
			return "", nil, err
		}
		if len(cols) == 0 {
			return "", nil, ErrEmptyUpdateData
		}

		args := make([]any, 0, len(subArgs)+8)
		args = append(args, subArgs...)
		pos := len(args) + 1

		query := &strings.Builder{}
		query.WriteString("UPDATE ")
		query.WriteString(tableName)
		query.WriteString(" SET (")
		query.WriteString(strings.Join(cols, ","))
		query.WriteString(") = (")
		query.WriteString(subSQL)
		query.WriteString(") ")

		whereArgs, _, err := dbb.buildConditionClauseB(query, updateQuery.PWhere, pos, "WHERE")
		if err != nil {
			return "", nil, err
		}
		args = append(args, whereArgs...)

		if returning {
			fields, err := updateQuery.GetFormattedFields(dbb.schemaPrefix)
			if err != nil {
				return "", nil, err
			}

			query.WriteString(" RETURNING ")
			query.WriteString(strings.Join(fields, ","))
		}

		queryStr := query.String()

		if logEnabled {
			color.Magenta(queryStr)
		}

		return queryStr, args, nil
	}

	return "", nil, ErrEmptyUpdateData
}

func (dbb *DBBridge) UpdateWithFields(updateQuery *Query) ([]M, error) {
	query, args, err := dbb.BuildUpdateQuery(updateQuery, true)
	if err != nil {
		return nil, err
	}

	return dbb.ExecuteQuery(query, args...)
}

func (dbb *DBBridge) UpdateOneWithFields(updateQuery *Query) (M, error) {
	query, args, err := dbb.BuildUpdateQuery(updateQuery, true)
	if err != nil {
		return nil, err
	}

	if result, err := dbb.ExecuteQuery(query, args...); err != nil {
		return nil, err
	} else if len(result) == 0 {
		return nil, ErrNotFoundRecord
	} else {
		return result[0], nil
	}
}

func (dbb *DBBridge) UpdateWithFieldsB(updateQuery *Query, v any) error {
	query, args, err := dbb.BuildUpdateQuery(updateQuery, true)
	if err != nil {
		return err
	}

	if bytes, err := dbb.ExecuteQueryBytes(query, true, args...); err == nil {
		return json.Unmarshal(bytes, v)
	} else {
		return err
	}
}

func (dbb *DBBridge) UpdateOneWithFieldsB(updateQuery *Query, v any) error {
	query, args, err := dbb.BuildUpdateQuery(updateQuery, true)
	if err != nil {
		return err
	}

	if bytes, err := dbb.ExecuteQueryBytes(query, false, args...); err == nil {
		return json.Unmarshal(bytes, v)
	} else {
		return err
	}
}

func (dbb *DBBridge) UpdateWithRowsAffected(updateQuery *Query) (int64, error) {
	query, args, err := dbb.BuildUpdateQuery(updateQuery, false)
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
