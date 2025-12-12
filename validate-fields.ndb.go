package ndb

import (
	"fmt"
	"strings"

	goutils "github.com/nitsugaro/go-utils"
)

func FormatSQLFields(schemaPrefix string, fields ...string) ([]string, error) {
	parts := make([]string, len(fields))
	for i := range fields {
		val, err := FormatSQLField(schemaPrefix, fields[i])
		if err != nil {
			return nil, err
		}

		parts[i] = val
	}

	return parts, nil
}

func FormatSQLField(schemaPrefix string, f string) (string, error) {
	parts := strings.Split(f, ":")

	nameParts := strings.Split(parts[0], ".")
	if len(nameParts) > 2 {
		return "", fmt.Errorf("invalid field syntax: '%s'", f)
	}

	name := "*"
	if parts[0] != "*" {
		if err := IsSQLName(parts[0]); err != nil {
			return "", err
		}

		name = strings.Join(goutils.Map(nameParts, func(f string, i int) string {
			if i == 0 && len(nameParts) != 1 {
				return "\"" + schemaPrefix + f + "\""
			} else {
				return "\"" + f + "\""
			}
		}), ".")
	}

	return name, nil
}

func ValidParseSqlFields(schemaPrefix string, fields []*SQLField) ([]string, error) {
	parts := make([]string, len(fields))
	for i := range fields {
		val, err := fields[i].GerForQuery(schemaPrefix)
		if err != nil {
			return nil, err
		}

		parts[i] = val
	}

	return parts, nil
}
