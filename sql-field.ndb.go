package ndb

type SQLOperation struct {
	POp   Operation `json:"operation,omitempty"`
	PArgs []string  `json:"args,omitempty"`
}

func (sq *SQLOperation) GetOp() Operation {
	return sq.POp
}

func (sq *SQLOperation) GetArgs() []string {
	return sq.PArgs
}

type SQLField struct {
	PName      string          `json:"name,omitempty"`
	POperators []*SQLOperation `json:"operators,omitempty"`

	q *Query
}

func (sf *SQLField) GetName() string {
	return sf.PName
}

func (sf *SQLField) GetOperators() []*SQLOperation {
	return sf.POperators
}
