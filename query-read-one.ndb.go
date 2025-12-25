package ndb

func (b *DBBridge) ReadOne(readOneQuery *Query) (M, error) {
	result, err := b.Read(readOneQuery.Limit(1))
	if err != nil {
		return nil, err
	}

	if len(result) == 1 {
		return result[0], nil
	} else {
		return nil, ErrNotFoundRecord
	}
}
