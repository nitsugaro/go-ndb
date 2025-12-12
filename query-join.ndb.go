package ndb

type Join struct {
	*BasicSchema
	PTyp JoinType `json:"type"`
	POn  []M      `json:"on"`

	q *Query
}

func (j *Join) On(condition M) *Join {
	j.POn = append(j.POn, condition)
	return j
}

func (j *Join) DoneJoin() *Query {
	return j.q
}

func NewInnerJoin(schema string, on ...M) *Join {
	return &Join{
		BasicSchema: &BasicSchema{Schema: schema},
		PTyp:        INNER_JOIN,
		POn:         on,
	}
}

func NewLeftJoin(schema string, on ...M) *Join {
	return &Join{
		BasicSchema: &BasicSchema{Schema: schema},
		PTyp:        LEFT_JOIN,
		POn:         on,
	}
}

func NewRightJoin(schema string, on ...M) *Join {
	return &Join{
		BasicSchema: &BasicSchema{Schema: schema},
		PTyp:        RIGHT_JOIN,
		POn:         on,
	}
}

func NewFullJoin(schema string, on ...M) *Join {
	return &Join{
		BasicSchema: &BasicSchema{Schema: schema},
		PTyp:        FULL_JOIN,
		POn:         on,
	}
}

func NewCrossJoin(schema string, on ...M) *Join {
	return &Join{
		BasicSchema: &BasicSchema{Schema: schema},
		PTyp:        CROSS_JOIN,
		POn:         on,
	}
}
