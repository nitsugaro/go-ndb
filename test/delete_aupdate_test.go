package test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/nitsugaro/go-ndb"
)

// helpers

func mustStep(t *testing.T, name string, fn func(t *testing.T)) {
	t.Helper()
	if !t.Run(name, fn) {
		t.FailNow()
	}
}

func resetSchemas(t *testing.T) {
	t.Helper()

	bridge.DeleteSchema(userPayments.PName)
	bridge.DeleteSchema(userType.PName)
	bridge.DeleteSchema(usersTable.PName)

	if err := bridge.CreateSchema(usersTable); err != nil {
		t.Fatalf("create_schema_users: %v", err)
	}
	if err := bridge.CreateSchema(userType); err != nil {
		t.Fatalf("create_schema_users_type: %v", err)
	}
	if err := bridge.CreateSchema(userPayments); err != nil {
		t.Fatalf("create_schema_user_payments: %v", err)
	}
}

/* -------------------------------------------------
   UPDATE / DELETE FLOW
---------------------------------------------------*/

func TestUpdateAndDelete(t *testing.T) {
	var (
		user   User
		payIDs []uint
	)

	mustStep(t, "01_reset_schemas", func(t *testing.T) {
		resetSchemas(t)
	})

	mustStep(t, "02_insert_user_and_type", func(t *testing.T) {
		payload := ndb.M{
			"public_id": uuid.NewString(),
			"email":     "userC@test.com",
			"username":  "userC",
		}

		q := ndb.NewCreateQuery(usersTable.PName).
			Payload(payload).
			Fields("id", "public_id", "email", "username", "created_at")

		if err := bridge.CreateOneB(q, &user); err != nil {
			t.Fatalf("insert_user_error: %v", err)
		}

		typePayload := ndb.M{"user_id": user.ID, "type": "USER_CLIENT"}
		if _, err := bridge.Create(ndb.NewCreateQuery(userType.PName).Payload(typePayload)); err != nil {
			t.Fatalf("insert_user_type_error: %v", err)
		}
	})

	mustStep(t, "03_insert_payments", func(t *testing.T) {
		amounts := []float64{5, 15, 30}
		for _, a := range amounts {
			p := ndb.M{"user_id": user.ID, "amount": a}
			var row UserPayment

			q := ndb.NewCreateQuery(userPayments.PName).
				Payload(p).
				Fields("id", "user_id", "amount", "created_at")

			if err := bridge.CreateOneB(q, &row); err != nil {
				t.Fatalf("insert_payment_error amount=%v: %v", a, err)
			}
			payIDs = append(payIDs, row.ID)
		}
		if len(payIDs) != 3 {
			t.Fatalf("expected 3 payments, got %d", len(payIDs))
		}
	})

	mustStep(t, "04_update_user_with_returning", func(t *testing.T) {
		newEmail := "userC+updated@test.com"

		q := ndb.NewUpdateQuery(usersTable.PName).
			Payload(ndb.M{"email": newEmail}).
			Where(ndb.M{"id": user.ID}).
			Fields("id", "email", "username")

		var updated User
		if err := bridge.UpdateOneWithFieldsB(q, &updated); err != nil {
			t.Fatalf("update_user_error: %v", err)
		}

		if updated.ID != user.ID {
			t.Fatalf("update_user_id_mismatch expected=%d actual=%d", user.ID, updated.ID)
		}
		if updated.Email != newEmail {
			t.Fatalf("update_user_email_mismatch expected=%q actual=%q", newEmail, updated.Email)
		}
	})

	mustStep(t, "05_update_payments_rows_affected", func(t *testing.T) {
		q := ndb.NewUpdateQuery(userPayments.PName).
			Payload(ndb.M{"amount": 999.0}).
			Where(ndb.M{"user_id": user.ID})

		affected, err := bridge.UpdateWithRowsAffected(q)
		if err != nil {
			t.Fatalf("update_payments_error: %v", err)
		}
		if affected != int64(len(payIDs)) {
			t.Fatalf("update_payments_rows_mismatch expected=%d actual=%d", len(payIDs), affected)
		}

		read := ndb.NewReadQuery(userPayments.PName).
			Where(ndb.M{"user_id": user.ID}).
			Fields("id", "user_id", "amount")

		var payments []UserPayment
		if err := bridge.ReadB(read, &payments); err != nil {
			t.Fatalf("read_payments_after_update_error: %v", err)
		}
		for _, p := range payments {
			if p.Amount != 999.0 {
				t.Fatalf("payment_amount_not_updated id=%d amount=%v", p.ID, p.Amount)
			}
		}
	})

	mustStep(t, "06_delete_one_with_fieldsB", func(t *testing.T) {
		q := ndb.NewDeleteQuery(userType.PName).
			Where(ndb.M{"user_id": user.ID}).
			Fields("user_id", "type")

		var deleted struct {
			UserID uint   `json:"user_id"`
			Type   string `json:"type"`
		}
		if err := bridge.DeleteOneWithFieldsB(q, &deleted); err != nil {
			t.Fatalf("delete_user_type_error: %v", err)
		}
		if deleted.UserID != user.ID {
			t.Fatalf("delete_user_type_user_id_mismatch expected=%d actual=%d", user.ID, deleted.UserID)
		}
	})

	mustStep(t, "07_delete_payments_rows_affected", func(t *testing.T) {
		q := ndb.NewDeleteQuery(userPayments.PName).
			Where(ndb.M{"user_id": user.ID})

		affected, err := bridge.DeleteWithRowsAffected(q)
		if err != nil {
			t.Fatalf("delete_payments_error: %v", err)
		}
		if affected != int64(len(payIDs)) {
			t.Fatalf("delete_payments_rows_mismatch expected=%d actual=%d", len(payIDs), affected)
		}

		read := ndb.NewReadQuery(userPayments.PName).
			Where(ndb.M{"user_id": user.ID})

		var payments []UserPayment
		if err := bridge.ReadB(read, &payments); err != nil {
			t.Fatalf("read_payments_after_delete_error: %v", err)
		}
		if len(payments) != 0 {
			t.Fatalf("expected 0 payments after delete, got %d", len(payments))
		}
	})
}

/* -------------------------------------------------
   FIELD OPERATIONS (BASIC)
---------------------------------------------------*/

type UserAggSimple struct {
	RowCount int64  `json:"row_count"`
	MinEmail string `json:"min_email"`
	MaxEmail string `json:"max_email"`
}

func TestFieldOperationsBasic(t *testing.T) {
	var users []User

	mustStep(t, "01_reset_and_seed_users", func(t *testing.T) {
		resetSchemas(t)

		emails := []string{
			"a@test.com",
			"b@test.com",
			"c@test.com",
		}

		for i, e := range emails {
			payload := ndb.M{
				"public_id": uuid.NewString(),
				"email":     e,
				"username":  fmt.Sprintf("u%d", i+1),
			}

			q := ndb.NewCreateQuery(usersTable.PName).
				Payload(payload).
				Fields("id", "public_id", "email", "username", "created_at")

			var u User
			if err := bridge.CreateOneB(q, &u); err != nil {
				t.Fatalf("insert_user_error email=%q: %v", e, err)
			}
			users = append(users, u)
		}
	})

	mustStep(t, "02_aggregate_min_max_count", func(t *testing.T) {
		read := ndb.NewReadQuery(usersTable.PName).
			NewField("users.id").Count().As("row_count").DoneField().
			NewField("users.email").Min().As("min_email").DoneField().
			NewField("users.email").Max().As("max_email").DoneField()

		var rows []UserAggSimple
		if err := bridge.ReadB(read, &rows); err != nil {
			t.Fatalf("read_agg_error: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("agg_rows_count_invalid expected=1 actual=%d", len(rows))
		}

		agg := rows[0]
		if agg.RowCount != int64(len(users)) {
			t.Fatalf("agg_row_count_mismatch expected=%d actual=%d", len(users), agg.RowCount)
		}
		if agg.MinEmail != "a@test.com" || agg.MaxEmail != "c@test.com" {
			t.Fatalf("agg_email_bounds_mismatch min=%q max=%q", agg.MinEmail, agg.MaxEmail)
		}
	})
}
