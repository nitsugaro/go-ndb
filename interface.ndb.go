package ndb

type BasicSchema struct {
	Schema string `json:"schema"`
}

func (bt *BasicSchema) GetSchema(db *DBBridge) (string, error) {
	if bt.Schema == "" {
		return "", ErrNotFoundTable
	}

	return "\"" + db.GetSchemaPrefix() + bt.Schema + "\"", nil
}

type BasicQuery struct {
	*BasicSchema
	Select []string `json:"select"`
	Where  []M      `json:"where"`
}

func (dbo *BasicQuery) GetSelect(schemaPrefix string, fillEmpty bool) ([]string, error) {
	if len(dbo.Select) == 0 {
		if fillEmpty {
			return []string{"*"}, nil
		} else {
			return []string{}, nil
		}
	}

	return ValidParseSqlFields(schemaPrefix, dbo.Select)
}

var allowedJoins = map[string]bool{"INNER": true, "LEFT": true, "RIGHT": true, "FULL": true, "CROSS": true}

var allowedFuncs = map[string]bool{"COUNT": true, "SUM": true, "AVG": true, "MAX": true, "MIN": true}
