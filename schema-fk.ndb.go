package ndb

import (
	"errors"
	"fmt"

	goutils "github.com/nitsugaro/go-utils"
)

type ForeignKey struct {
	PSchema   string         `json:"schema"`
	PColumn   string         `json:"column"`
	POnDelete ForeignKeyRule `json:"on_delete,omitempty"`
	POnUpdate ForeignKeyRule `json:"on_update,omitempty"`
	f         *SchemaField
}

func (fk *ForeignKey) GetSchemaName() string {
	return fk.PSchema
}

func (fk *ForeignKey) GetColumn() string {
	return fk.PColumn
}

func (fk *ForeignKey) OnUpdate(OnUpdate ForeignKeyRule) *ForeignKey {
	fk.POnUpdate = OnUpdate
	return fk
}

func (fk *ForeignKey) OnDelete(onDelete ForeignKeyRule) *ForeignKey {
	fk.POnDelete = onDelete
	return fk
}

func (fk *ForeignKey) DoneFK() *SchemaField {
	return fk.f
}

func (fk *ForeignKey) Validate() error {
	if len(fk.PSchema) == 0 || len(fk.PColumn) == 0 {
		return errors.New("schema & column fields are required")
	}

	if err := IsSQLName(fk.PSchema); err != nil {
		return err
	}

	if err := IsSQLName(fk.PColumn); err != nil {
		return err
	}

	if fk.POnDelete != "" && goutils.All(foreignKeyRules, func(fkr ForeignKeyRule, _ int) bool { return fkr != fk.POnDelete }) {
		return fmt.Errorf("invalid on delete clausure: %s", fk.POnDelete)
	}

	if fk.POnUpdate != "" && goutils.All(foreignKeyRules, func(fkr ForeignKeyRule, _ int) bool { return fkr != fk.POnUpdate }) {
		return fmt.Errorf("invalid on update clausure: %s", fk.POnUpdate)
	}

	return nil
}
