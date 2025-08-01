package ndb

import "errors"

var (
	ErrNotFoundTable            = errors.New("not found table in query config")
	ErrNotFoundJoinTable        = errors.New("required join table in query config")
	ErrConvert                  = errors.New("cannot convert value from sql query")
	ErrUnsuporrtedJoinType      = errors.New("invalid join type")
	ErrUnsuporrtedQueryOperator = errors.New("invalid operator in query %s")
	ErrEmptyUpdateData          = errors.New("update data cannot be empty")
	ErrEmptyCreateData          = errors.New("create data cannot be empty")
	ErrInvalidIntType           = errors.New("invalid int type")
	ErrInvalidFloatType         = errors.New("invalid float type")
	ErrInvalidStrType           = errors.New("invalid str type")
	ErrInvalidBoolType          = errors.New("invalid bool type")
	ErrInvalidListType          = errors.New("invalid list type")
	ErrSchemaKeyNotFound        = errors.New("not found key in schema")
)
