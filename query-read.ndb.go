package ndb

import (
	"fmt"
	"reflect"
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

	if err := dbb.runPrevValidateMiddlewares(readQuery); err != nil {
		return "", nil, err
	}

	fields, err := readQuery.GetFormattedFields(dbb.schemaPrefix)
	if err != nil {
		return "", nil, err
	}

	var (
		query = &strings.Builder{}
		args  []any
		pos   int = 1
	)

	query.WriteString("SELECT ")
	query.WriteString(strings.Join(fields, ","))
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
			return "", nil, fmt.Errorf("invalid ON clause for join %s: %w", join.PSchema, err)
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
			order := strings.ToUpper(readQuery.POrderBy[len(readQuery.POrderBy)-1].PName)
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

func (dbb *DBBridge) Read(readQuery *Query) ([]M, error) {
	if query, args, err := dbb.BuildReadQuery(readQuery); err != nil {
		return nil, err
	} else {
		return dbb.ExecuteQuery(query, args...)
	}
}

func (dbb *DBBridge) ReadB(readQuery *Query, dest any) error {
	query, args, err := dbb.BuildReadQuery(readQuery)
	if err != nil {
		return err
	}

	rows, err := dbb.queryRows(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("dest must be non-nil pointer")
	}
	sliceVal := rv.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be pointer to slice")
	}
	elemType := sliceVal.Type().Elem()

	if elemType.Kind() == reflect.Struct {
		return scanIntoStructs(rows, dest)
	}

	if elemType.Kind() == reflect.Map &&
		elemType.Key().Kind() == reflect.String &&
		elemType.Elem().Kind() == reflect.Interface {
		return scanIntoMaps(rows, dest)
	}

	return fmt.Errorf("unsupported ReadB target element type: %s", elemType.Kind().String())
}

func (dbb *DBBridge) ReadOneB(readOneQuery *Query, dest any) error {
	q := readOneQuery.Limit(1)

	query, args, err := dbb.BuildReadQuery(q)
	if err != nil {
		return err
	}

	rows, err := dbb.queryRows(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("dest must be non-nil pointer")
	}
	elem := rv.Elem()

	if elem.Kind() == reflect.Struct {
		return scanOneIntoStruct(rows, dest)
	}

	if elem.Kind() == reflect.Map &&
		elem.Type().Key().Kind() == reflect.String &&
		elem.Type().Elem().Kind() == reflect.Interface {
		return scanOneIntoMap(rows, dest)
	}

	return fmt.Errorf("unsupported ReadOneB target type: %s", elem.Kind().String())
}
