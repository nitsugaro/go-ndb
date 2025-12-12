package ndb

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/fatih/color"
)

func (dbb *DBBridge) BuildReadQuery(readQuery *Query) (string, []any, error) {
	if readQuery.typ != READ {
		return "", nil, ErrInvalidQueryType
	}

	tableName, err := readQuery.GetSchema(dbb)
	if err != nil {
		return "", nil, err
	}

	if err := dbb.runMiddlewares(readQuery); err != nil {
		return "", nil, err
	}

	selectFields, err := readQuery.GetSelect(dbb.schemaPrefix)
	if err != nil {
		return "", nil, err
	}

	var (
		query = &strings.Builder{}
		args  []any
		pos   int = 1
	)

	query.WriteString("SELECT ")
	query.WriteString(strings.Join(selectFields, ","))
	query.WriteString(" FROM ")

	if readQuery.subQuery != nil {
		subSQL, subArgs, err := dbb.BuildReadQuery(readQuery.subQuery.Query)
		if err != nil {
			return "", nil, err
		}

		args = append(args, subArgs...)
		pos = len(args) + 1

		query.WriteByte('(')
		query.WriteString(subSQL)
		query.WriteString(") AS ")

		subQueryName := dbb.schemaPrefix + readQuery.subQuery.queryName
		subQueryName, err = FormatSQLField(dbb.schemaPrefix, subQueryName)
		if err != nil {
			return "", nil, err
		}

		query.WriteString(subQueryName)
	} else {
		query.WriteString(tableName)
	}

	for _, join := range readQuery.PJoins {
		joinTable, err := join.GetSchema(dbb)
		if err != nil {
			return "", nil, err
		}

		joinType := strings.ToUpper(string(join.PTyp))
		if join.PTyp == "" {
			joinType = string(INNER_JOIN)
		} else if !slices.Contains(allowedJoins, joinType) {
			return "", nil, ErrUnsuporrtedJoinType
		}

		query.WriteByte(' ')
		query.WriteString(joinType)
		query.WriteString(" JOIN ")
		query.WriteString(joinTable)
		query.WriteString(" ON")

		onArgs, newPos, err := dbb.buildConditionClauseB(query, join.POn, pos, "")
		if err != nil {
			return "", nil, fmt.Errorf("invalid ON clause for join %s: %w", join.Schema, err)
		}
		pos = newPos
		args = append(args, onArgs...)
	}

	whereArgs, _, err := dbb.buildConditionClauseB(query, readQuery.PWhere, pos, "WHERE")
	if err != nil {
		return "", nil, err
	}

	args = append(args, whereArgs...)

	if len(readQuery.PGroupBy) != 0 {
		if fields, err := ValidParseSqlFields(dbb.schemaPrefix, readQuery.PGroupBy); err != nil {
			return "", nil, err
		} else {
			query.WriteString(" GROUP BY ")
			query.WriteString(strings.Join(fields, ","))
		}
	}

	if len(readQuery.POrderBy) >= 1 {
		if fields, err := ValidParseSqlFields(dbb.schemaPrefix, readQuery.POrderBy[:len(readQuery.POrderBy)-1]); err != nil {
			return "", nil, err
		} else {
			order := strings.ToUpper(readQuery.POrderBy[len(readQuery.POrderBy)-1].Name)
			if order == "ASC" || order == "DESC" {
				query.WriteString(" ORDER BY ")
				query.WriteString(strings.Join(fields, ","))
				query.WriteByte(' ')
				query.WriteString(order)
			}
		}
	}

	query.WriteString(" LIMIT ")
	query.WriteString(strconv.Itoa((readQuery.GetLimit())))

	if readQuery.POffset != 0 {
		query.WriteString(" OFFSET ")
		query.WriteString(strconv.Itoa((readQuery.GetOffset())))
	}

	queryStr := query.String()
	if logEnabled {
		color.Green(queryStr)
	}

	return queryStr, args, nil
}

func (dbb *DBBridge) Read(readQuery *Query) (any, error) {
	if query, args, err := dbb.BuildReadQuery(readQuery); err != nil {
		return nil, err
	} else {
		return dbb.ExecuteQuery(query, args...)
	}
}

func (dbb *DBBridge) ReadB(readQuery *Query, v any) error {
	if query, args, err := dbb.BuildReadQuery(readQuery); err != nil {
		return err
	} else if bytes, err := dbb.ExecuteQueryBytes(query, true, args...); err != nil {
		return err
	} else {
		return json.Unmarshal(bytes, v)
	}
}
