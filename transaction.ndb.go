package ndb

func (dbb *DBBridge) Transaction(tfunc func(bridge *DBBridge) error) error {
	trx, err := dbb.db.Begin()
	if err != nil {
		return err
	}

	tempBridge := NewBridge(&NBridge{trx: trx, middlewares: dbb.middlewares, SchemaPrefix: dbb.schemaPrefix, SchemaStorage: dbb.schemaStorage})
	if err := tfunc(tempBridge); err != nil {
		return tempBridge.trx.Rollback()
	}

	return tempBridge.trx.Commit()
}
