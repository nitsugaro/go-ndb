package ndb

import (
	"errors"
	"fmt"
	"strings"
)

func (dbb *DBBridge) buildConditionClause(clauseArr []M, startPos int, prefix string) (string, []any, int, error) {
	if len(clauseArr) == 0 {
		return "", nil, startPos, nil
	}

	var clauseGroups []string
	var args []any
	pos := startPos
	for _, andGroup := range clauseArr {
		clause, newArgs, newPos, err := dbb.parseAndGroup(andGroup, pos)
		if err != nil {
			return "", nil, pos, err
		}
		clauseGroups = append(clauseGroups, fmt.Sprintf("(%s)", clause))
		args = append(args, newArgs...)
		pos = newPos
	}

	result := strings.Join(clauseGroups, " OR ")
	if prefix != "" {
		result = prefix + " " + result
	}
	return result, args, pos, nil
}

func (dbb *DBBridge) parseAndGroup(group M, startPos int) (string, []any, int, error) {
	var parts []string
	var args []any
	pos := startPos

	for key, val := range group {
		if key == "not" {
			notGroup, ok := val.(M)
			if !ok {
				return "", nil, pos, errors.New("invalid 'not' clause")
			}
			clause, newArgs, newPos, err := dbb.parseAndGroup(notGroup, pos)
			if err != nil {
				return "", nil, pos, err
			}
			parts = append(parts, fmt.Sprintf("NOT (%s)", clause))
			args = append(args, newArgs...)
			pos = newPos
			continue
		}

		switch v := val.(type) {
		case M:
			for op, val2 := range v {
				switch strings.ToLower(op) {
				case "gt":
					parts = append(parts, fmt.Sprintf("%s > $%d", key, pos))
					args = append(args, val2)
					pos++
				case "gte":
					parts = append(parts, fmt.Sprintf("%s >= $%d", key, pos))
					args = append(args, val2)
					pos++
				case "lt":
					parts = append(parts, fmt.Sprintf("%s < $%d", key, pos))
					args = append(args, val2)
					pos++
				case "lte":
					parts = append(parts, fmt.Sprintf("%s <= $%d", key, pos))
					args = append(args, val2)
					pos++
				case "ne":
					parts = append(parts, fmt.Sprintf("%s != $%d", key, pos))
					args = append(args, val2)
					pos++
				case "like":
					parts = append(parts, fmt.Sprintf("%s LIKE $%d", key, pos))
					args = append(args, val2)
					pos++
				case "ilike", "i_like":
					parts = append(parts, fmt.Sprintf("%s ILIKE $%d", key, pos))
					args = append(args, val2)
					pos++
				case "in":
					arr, ok := val2.([]any)
					if !ok || len(arr) == 0 {
						return "", nil, pos, fmt.Errorf("invalid IN clause for %s", key)
					}
					qmarks := make([]string, len(arr))
					for i := range arr {
						qmarks[i] = fmt.Sprintf("$%d", pos)
						args = append(args, arr[i])
						pos++
					}

					sKey, err := ValidParseSqlField(dbb.schemaPrefix, key)
					if err != nil {
						return "", nil, 0, err
					}

					parts = append(parts, fmt.Sprintf("%s IN (%s)", sKey, strings.Join(qmarks, ",")))
				case "notin", "not_in":
					arr, ok := val2.([]any)
					if !ok || len(arr) == 0 {
						return "", nil, pos, fmt.Errorf("invalid NOT IN clause for %s", key)
					}
					qmarks := make([]string, len(arr))
					for i := range arr {
						qmarks[i] = fmt.Sprintf("$%d", pos)
						args = append(args, arr[i])
						pos++
					}

					sKey, err := ValidParseSqlField(dbb.schemaPrefix, key)
					if err != nil {
						return "", nil, 0, err
					}

					parts = append(parts, fmt.Sprintf("%s NOT IN (%s)", sKey, strings.Join(qmarks, ",")))
				case "isnull", "is_null":
					isNull, ok := val2.(bool)
					if !ok {
						return "", nil, pos, fmt.Errorf("isnull operator expects boolean")
					}

					sKey, err := ValidParseSqlField(dbb.schemaPrefix, key)
					if err != nil {
						return "", nil, 0, err
					}

					if isNull {
						parts = append(parts, fmt.Sprintf("%s IS NULL", sKey))
					} else {
						parts = append(parts, fmt.Sprintf("%s IS NOT NULL", sKey))
					}
				case "eq_field", "eqfield":
					sKey, err := ValidParseSqlFields(dbb.schemaPrefix, []string{key, val2.(string)})
					if err != nil {
						return "", nil, 0, err
					}

					parts = append(parts, fmt.Sprintf("%s = %s", sKey[0], sKey[1]))
				default:
					return "", nil, pos, fmt.Errorf(ErrUnsuporrtedQueryOperator.Error(), op)
				}
			}
		default:
			sKey, err := ValidParseSqlField(dbb.schemaPrefix, key)
			if err != nil {
				return "", nil, 0, err
			}

			parts = append(parts, fmt.Sprintf("%s = $%d", sKey, pos))
			args = append(args, val)
			pos++
		}
	}

	return strings.Join(parts, " AND "), args, pos, nil
}
