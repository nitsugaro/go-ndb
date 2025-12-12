package ndb

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func writeDollarPos(b *strings.Builder, pos int) {
	b.WriteByte('$')
	b.WriteString(strconv.Itoa(pos))
}

func (dbb *DBBridge) buildConditionClauseB(b *strings.Builder, clauseArr []M, startPos int, prefix string) ([]any, int, error) {
	if len(clauseArr) == 0 {
		return nil, startPos, nil
	}

	b.WriteByte(' ')

	var args []any
	pos := startPos

	if prefix != "" {
		b.WriteString(prefix)
		b.WriteByte(' ')
	}

	for i, andGroup := range clauseArr {
		if i > 0 {
			b.WriteString(" OR ")
		}
		b.WriteByte('(')

		var err error
		pos, err = dbb.parseAndGroupToBuilder(andGroup, pos, b, &args)
		if err != nil {
			return nil, pos, err
		}

		b.WriteByte(')')
	}

	return args, pos, nil
}

func (dbb *DBBridge) parseAndGroupToBuilder(group M, startPos int, b *strings.Builder, args *[]any) (int, error) {
	pos := startPos
	first := true

	addSep := func() {
		if !first {
			b.WriteString(" AND ")
		}
		first = false
	}

	for key, val := range group {
		if key == "not" {
			notGroup, ok := val.(M)
			if !ok {
				return pos, errors.New("invalid 'not' clause")
			}

			addSep()
			b.WriteString("NOT (")

			var err error
			pos, err = dbb.parseAndGroupToBuilder(notGroup, pos, b, args)
			if err != nil {
				return pos, err
			}

			b.WriteByte(')')
			continue
		}

		switch v := val.(type) {
		case M:
			for op, val2 := range v {
				switch strings.ToLower(op) {
				case "gt":
					addSep()
					b.WriteString(key)
					b.WriteString(" > ")
					writeDollarPos(b, pos)
					*args = append(*args, val2)
					pos++

				case "gte":
					addSep()
					b.WriteString(key)
					b.WriteString(" >= ")
					writeDollarPos(b, pos)
					*args = append(*args, val2)
					pos++

				case "lt":
					addSep()
					b.WriteString(key)
					b.WriteString(" < ")
					writeDollarPos(b, pos)
					*args = append(*args, val2)
					pos++

				case "lte":
					addSep()
					b.WriteString(key)
					b.WriteString(" <= ")
					writeDollarPos(b, pos)
					*args = append(*args, val2)
					pos++

				case "ne":
					addSep()
					b.WriteString(key)
					b.WriteString(" != ")
					writeDollarPos(b, pos)
					*args = append(*args, val2)
					pos++

				case "like":
					addSep()
					b.WriteString(key)
					b.WriteString(" LIKE ")
					writeDollarPos(b, pos)
					*args = append(*args, val2)
					pos++

				case "ilike", "i_like":
					addSep()
					b.WriteString(key)
					b.WriteString(" ILIKE ")
					writeDollarPos(b, pos)
					*args = append(*args, val2)
					pos++

				case "in":
					arr, ok := val2.([]any)
					if !ok || len(arr) == 0 {
						return pos, fmt.Errorf("invalid IN clause for %s", key)
					}

					sKey, err := FormatSQLField(dbb.schemaPrefix, key)
					if err != nil {
						return pos, err
					}

					addSep()
					b.WriteString(sKey)
					b.WriteString(" IN (")

					for i := range arr {
						if i > 0 {
							b.WriteByte(',')
						}
						writeDollarPos(b, pos)
						*args = append(*args, arr[i])
						pos++
					}
					b.WriteByte(')')

				case "notin", "not_in":
					arr, ok := val2.([]any)
					if !ok || len(arr) == 0 {
						return pos, fmt.Errorf("invalid NOT IN clause for %s", key)
					}

					sKey, err := FormatSQLField(dbb.schemaPrefix, key)
					if err != nil {
						return pos, err
					}

					addSep()
					b.WriteString(sKey)
					b.WriteString(" NOT IN (")

					for i := range arr {
						if i > 0 {
							b.WriteByte(',')
						}
						writeDollarPos(b, pos)
						*args = append(*args, arr[i])
						pos++
					}
					b.WriteByte(')')

				case "isnull", "is_null":
					isNull, ok := val2.(bool)
					if !ok {
						return pos, fmt.Errorf("isnull operator expects boolean")
					}

					sKey, err := FormatSQLField(dbb.schemaPrefix, key)
					if err != nil {
						return pos, err
					}

					addSep()
					b.WriteString(sKey)
					if isNull {
						b.WriteString(" IS NULL")
					} else {
						b.WriteString(" IS NOT NULL")
					}

				case "eq_field", "eqf":
					eqField, ok := val2.(string)
					if !ok {
						return pos, fmt.Errorf("eq_field expects string")
					}

					sKey, err := FormatSQLFields(dbb.schemaPrefix, key, eqField)
					if err != nil {
						return pos, err
					}

					addSep()
					b.WriteString(sKey[0])
					b.WriteString(" = ")
					b.WriteString(sKey[1])

				default:
					return pos, fmt.Errorf(ErrUnsuporrtedQueryOperator.Error(), op)
				}
			}

		default:
			sKey, err := FormatSQLField(dbb.schemaPrefix, key)
			if err != nil {
				return pos, err
			}

			addSep()
			b.WriteString(sKey)
			b.WriteString(" = ")
			writeDollarPos(b, pos)
			*args = append(*args, val)
			pos++
		}
	}

	return pos, nil
}
