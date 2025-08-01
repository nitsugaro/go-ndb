package ndb

import (
	"fmt"
	"regexp"
	"strings"

	goutils "github.com/nitsugaro/go-utils"
)

var validName = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?|\*|(count|sum|avg|min|max|lower|upper)\((\*|[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?)\))$`)

func ValidSqlField(f string) error {
	if !validName.MatchString(f) {
		return fmt.Errorf("invalid field syntax: %s", f)
	}

	if matches := validName.FindStringSubmatch(f); matches != nil {
		funcName := matches[1]
		if allowedFuncs[funcName] {
			return nil
		}
	} else {
		return fmt.Errorf("invalid field syntax: %s", f)
	}
	return nil
}

func ValidSqlFields(fields []string) error {
	for _, field := range fields {
		if err := ValidSqlField(field); err != nil {
			return err
		}
	}

	return nil
}

func ValidParseSqlField(schemaPrefix, f string) (string, error) {
	parts := strings.Split(f, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("invalid field syntax: '%s'", f)
	}

	return strings.Join(goutils.Map(parts, func(f string, i int) string {
		if i == 0 && len(parts) != 1 {
			return "\"" + schemaPrefix + f + "\""
		} else {
			return "\"" + f + "\""
		}
	}), "."), nil
}

func ValidParseSqlFields(schemaPrefix string, f []string) ([]string, error) {
	parts := make([]string, len(f))
	for i := range f {
		val, err := ValidParseSqlField(schemaPrefix, f[i])
		if err != nil {
			return nil, err
		}

		parts[i] = val
	}

	return parts, nil
}
