package ndb

import "encoding/json"

func (b *DBBridge) ReadOne(readOneQuery *Query) (any, error) {
	result, err := b.Read(readOneQuery.Limit(1))
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

func (dbb *DBBridge) ReadOneB(readOneQuery *Query, v any) error {
	if query, args, err := dbb.BuildReadQuery(readOneQuery.Limit(1)); err != nil {
		return err
	} else if bytes, err := dbb.ExecuteQueryBytes(query, false, args...); err != nil {
		return err
	} else {
		return json.Unmarshal(bytes, v)
	}
}
