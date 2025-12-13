package ndb

import (
	"fmt"

	"github.com/fatih/color"
	goutils "github.com/nitsugaro/go-utils"
)

type QueryMiddleware = func(query *Query) error

func (d *DBBridge) AddMiddleware(m QueryMiddleware, runPostValidate bool) {
	if runPostValidate {
		d.postValidate = append(d.postValidate, m)
	} else {
		d.prevValidate = append(d.prevValidate, m)
	}
}

func (d *DBBridge) runPrevValidateMiddlewares(query *Query) error {
	for _, m := range d.prevValidate {
		if err := m(query); err != nil {
			return err
		}
	}

	return nil
}

func (d *DBBridge) runPostValidateMiddlewares(query *Query) error {
	for _, m := range d.postValidate {
		if err := m(query); err != nil {
			return err
		}
	}

	return nil
}

//########## DEFAULT MIDDLEWARES ###########

// Logs every query operation with its color
var QueryLoggingMiddleware = func(query *Query) error {
	fmt.Print("\n\n")
	switch query.Type() {
	case READ:
		color.Green(query.String())
	case CREATE:
		color.Yellow(query.String())
	case UPDATE:
		color.Magenta(query.String())
	case DELETE:
		color.Red(query.String())
	}
	return nil
}

// Prints the following format: "[%s] table=%s fields=%d where=%d joins=%d limit=%d offset=%d payload=%d\n"
var QuerySummaryLoggingMiddleware = func(q *Query) error {
	fmt.Printf("[%s] table=%s fields=%d where=%d joins=%d limit=%d offset=%d payload=%d\n",
		q.Type(), q.GetSchemaName(), len(q.GetFields()), len(q.GetWhere()), len(q.GetJoins()), q.GetLimit(), q.GetOffset(), len(q.GetPayload()),
	)
	return nil
}

// Forbids any delete/update operation without a condition where
var MissingWhereMiddleware = func(query *Query) error {
	if t := query.Type(); t != DELETE && t != UPDATE {
		return nil
	}

	if len(query.GetWhere()) == 0 || goutils.All(query.GetWhere(), func(w M, _ int) bool { return len(w) == 0 }) {
		return ErrMissingWhereQuery
	}

	return nil
}

var PayloadNotEmptyMiddleware = func(q *Query) error {
	switch q.Type() {
	case CREATE, UPDATE:
		if len(q.GetPayload()) == 0 {
			return ErrEmptyPayloadQuery
		}
	}

	return nil
}

var TableAllowlistMiddleware = func(allowed map[string]bool) func(*Query) error {
	return func(q *Query) error {
		if q.BasicSchema == nil || !allowed[q.GetSchemaName()] {
			return ErrTableNotAllowedQuery
		}

		return nil
	}
}
