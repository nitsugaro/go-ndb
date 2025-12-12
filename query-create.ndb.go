package ndb

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fatih/color"
)

func (dbb *DBBridge) BuildCreateQuery(createQuery *Query) (string, []any, error) {
	if createQuery.typ != CREATE {
		return "", nil, ErrInvalidQueryType
	}

	table, err := createQuery.GetSchema(dbb)
	if err != nil {
		return "", nil, err
	}

	selectFields, err := createQuery.GetSelect(dbb.schemaPrefix)
	if err != nil {
		return "", nil, err
	}

	var (
		query = &strings.Builder{}
		args  []any
	)

	if createQuery.RPayload != nil {
		if err := dbb.ValidateSchema(createQuery.Schema, CREATE, createQuery.RPayload); err != nil {
			return "", nil, err
		}

		if len(createQuery.RPayload) == 0 {
			return "", nil, ErrEmptyCreateData
		}

		if err := dbb.runMiddlewares(createQuery); err != nil {
			return "", nil, err
		}

		var keys []string
		var placeholders []string
		pos := 1

		for k, v := range createQuery.RPayload {
			if err := IsSQLName(k); err != nil {
				return "", nil, err
			}

			keys = append(keys, k)
			placeholders = append(placeholders, fmt.Sprintf("$%d", pos))
			args = append(args, v)
			pos++
		}

		query.WriteString("INSERT INTO ")
		query.WriteString(table)
		query.WriteString(" (")
		query.WriteString(strings.Join(keys, ","))
		query.WriteString(") VALUES (")
		query.WriteString(strings.Join(placeholders, ","))
		query.WriteString(") RETURNING ")
		query.WriteString(strings.Join(selectFields, ","))
	} else {
		if createQuery.subQuery == nil {
			return "", nil, ErrEmptyCreateData
		}

		subQuery, subArgs, err := dbb.BuildReadQuery(createQuery.subQuery.Query)
		if err != nil {
			return "", nil, err
		}
		args = subArgs

		keys, err := ValidParseSqlFields(dbb.schemaPrefix, createQuery.subQuery.fields)
		if err != nil {
			return "", nil, err
		}

		query.WriteString("INSERT INTO ")
		query.WriteString(table)
		query.WriteString(" (")
		query.WriteString(strings.Join(keys, ","))
		query.WriteString(") (")
		query.WriteString(subQuery)
		query.WriteString(") RETURNING ")
		query.WriteString(strings.Join(selectFields, ","))
	}

	queryStr := query.String()
	if logEnabled {
		color.Yellow(queryStr)
	}

	return queryStr, args, nil
}

func (dbb *DBBridge) Create(createQuery *Query) ([]M, error) {
	query, args, err := dbb.BuildCreateQuery(createQuery)
	if err != nil {
		return nil, err
	}

	result, err := dbb.ExecuteQuery(query, args...)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (dbb *DBBridge) CreateOne(createQuery *Query) (M, error) {
	query, args, err := dbb.BuildCreateQuery(createQuery)
	if err != nil {
		return nil, err
	}

	result, err := dbb.ExecuteQuery(query, args...)
	if err != nil {
		return nil, err
	}

	if len(result) == 1 {
		return result[0], nil
	}

	return nil, ErrEmptyCreateData
}

func (dbb *DBBridge) createB(createQuery *Query, v any, arrayVal bool) error {
	query, args, err := dbb.BuildCreateQuery(createQuery)
	if err != nil {
		return err
	}

	bytes, err := dbb.ExecuteQueryBytes(query, arrayVal, args...)
	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, v)
}

func (dbb *DBBridge) CreateOneB(createQuery *Query, v any) error {
	return dbb.createB(createQuery, v, false)
}

func (dbb *DBBridge) CreateB(createQuery *Query, v any) error {
	return dbb.createB(createQuery, v, true)
}
