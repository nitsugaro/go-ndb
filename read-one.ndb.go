package ndb

func (b *DBBridge) ReadOne(basicQuery *BasicQuery) (any, error) {
	result, err := b.Read(&ReadQuery{BasicQuery: basicQuery, Limit: 1})
	if err != nil {
		return nil, err
	}

	val, ok := result.([]M)
	if !ok {
		return nil, ErrConvert
	}

	if len(val) == 1 {
		return val[0], nil
	} else {
		return nil, nil
	}
}
