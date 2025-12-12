package ndb

import "encoding/json"

func (b *DBBridge) ReadOne(readOneQuery *Query) (M, error) {
	result, err := b.Read(readOneQuery.Limit(1))
	if err != nil {
		return nil, err
	}

	if len(result) == 1 {
		return result[0], nil
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
