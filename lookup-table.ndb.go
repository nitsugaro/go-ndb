package ndb

import (
	"fmt"
	"strings"
)

var validChar [256]bool

func init() {
	for c := 'a'; c <= 'z'; c++ {
		validChar[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		validChar[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		validChar[c] = true
	}
	validChar['_'] = true
	validChar['*'] = true
}

func isIdent(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := range len(s) {
		if !validChar[s[i]] {
			return false
		}
	}
	return true
}

func IsSQLName(name string) error {
	i := strings.IndexByte(name, '.')
	if (i == -1 && isIdent(name)) || (i != -1 && isIdent(name[:i]) && isIdent(name[i+1:])) {
		return nil
	}

	return fmt.Errorf("invalid field syntax: %s", name)
}
