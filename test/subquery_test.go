package test

import (
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/nitsugaro/go-ndb"
)

func assertFloatEq(t *testing.T, name string, exp, act float64) {
	t.Helper()
	if math.Abs(exp-act) > 1e-9 {
		t.Fatalf("%s_mismatch expected=%v actual=%v", name, exp, act)
	}
}

func TestSubQueryCRUD(t *testing.T) {
	var (
		userA User
		userB User

		payA = []float64{1.25, 2.50}
		payB = []float64{10.5, 20.75, 32.10}
	)

	mustStep(t, "01_reset_schemas", func(t *testing.T) {
		resetSchemas(t)
	})

	mustStep(t, "02_seed_users", func(t *testing.T) {
		qA := ndb.NewCreateQuery(usersTable.PName).
			Payload(ndb.M{
				"public_id": uuid.NewString(),
				"email":     "sub_a@test.com",
				"username":  "sub_a",
			}).
			Fields("id", "public_id", "email", "username", "created_at")

		if err := bridge.CreateOneB(qA, &userA); err != nil {
			t.Fatalf("insert_userA_error: %v", err)
		}

		qB := ndb.NewCreateQuery(usersTable.PName).
			Payload(ndb.M{
				"public_id": uuid.NewString(),
				"email":     "sub_b@test.com",
				"username":  "sub_b",
			}).
			Fields("id", "public_id", "email", "username", "created_at")

		if err := bridge.CreateOneB(qB, &userB); err != nil {
			t.Fatalf("insert_userB_error: %v", err)
		}
	})

	mustStep(t, "03_seed_payments", func(t *testing.T) {
		for _, a := range payA {
			if _, err := bridge.Create(
				ndb.NewCreateQuery(userPayments.PName).
					Payload(ndb.M{"user_id": userA.ID, "amount": a}),
			); err != nil {
				t.Fatalf("insert_payment_userA_error amount=%v: %v", a, err)
			}
		}
		for _, a := range payB {
			if _, err := bridge.Create(
				ndb.NewCreateQuery(userPayments.PName).
					Payload(ndb.M{"user_id": userB.ID, "amount": a}),
			); err != nil {
				t.Fatalf("insert_payment_userB_error amount=%v: %v", a, err)
			}
		}
	})

	mustStep(t, "04_create_with_subquery", func(t *testing.T) {
		sub := ndb.NewReadQuery(usersTable.PName).
			Fields("users.id").
			Where(ndb.M{"id": userB.ID}).
			Limit(1)

		q := ndb.NewCreateQuery(userType.PName).
			SubQuery(sub, ndb.Fs("user_id")).
			Fields("user_id", "type")

		rows, err := bridge.Create(q)
		if err != nil {
			t.Fatalf("create_subquery_error: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("create_subquery_rows_invalid expected=1 actual=%d", len(rows))
		}

		read := ndb.NewReadQuery(userType.PName).
			Where(ndb.M{"user_id": userB.ID})

		var got []map[string]any
		if err := bridge.ReadB(read, &got); err != nil {
			t.Fatalf("read_users_type_error: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("users_type_count_invalid expected=1 actual=%d", len(got))
		}
	})

	mustStep(t, "05_read_with_subquery", func(t *testing.T) {
		sub := ndb.NewReadQuery(userPayments.PName).
			NewField("user_payments.user_id").As("user_id").DoneField().
			NewField("user_payments.amount").Sum().As("total_amount").DoneField().
			Where(ndb.M{"user_id": userB.ID}).
			Group(ndb.Fs("user_payments.user_id")).
			Limit(1)

		outer := ndb.NewReadQuery(usersTable.PName).
			SubQueryName("sq", sub, ndb.Fs("user_id", "total_amount")).
			NewField("sq.user_id").As("user_id").DoneField().
			NewField("sq.total_amount").As("total_amount").DoneField().
			NewField("users.username").DoneField().
			NewJoin(usersTable.GetName(), ndb.INNER_JOIN).On(ndb.M{"users.id": ndb.M{"eq_field": "sq.user_id"}}).DoneJoin().
			Where(ndb.M{"users.id": userB.ID}).
			Limit(1)

		type Row struct {
			UserID      uint    `json:"user_id"`
			TotalAmount float64 `json:"total_amount"`
			Username    string  `json:"username"`
		}

		var rows []Row
		if err := bridge.ReadB(outer, &rows); err != nil {
			t.Fatalf("read_subquery_error: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("read_subquery_len_mismatch expected=1 actual=%d", len(rows))
		}

		var exp float64
		for _, v := range payB {
			exp += v
		}

		if rows[0].UserID != userB.ID {
			t.Fatalf("read_subquery_user_id_mismatch expected=%d actual=%d", userB.ID, rows[0].UserID)
		}
		if rows[0].Username != userB.Username {
			t.Fatalf("read_subquery_username_mismatch expected=%q actual=%q", userB.Username, rows[0].Username)
		}
		assertFloatEq(t, "read_subquery_total_amount", exp, rows[0].TotalAmount)
	})

	mustStep(t, "06_update_with_subquery", func(t *testing.T) {
		sub := ndb.NewReadQuery(usersTable.PName).
			Fields("users.status").
			Where(ndb.M{"id": userA.ID}).
			Limit(1)

		q := ndb.NewUpdateQuery(usersTable.PName).
			SubQuery(sub, ndb.Fs("status")).
			Where(ndb.M{"id": userB.ID}).
			Fields("id", "email", "username", "status")

		var updated struct {
			ID       uint   `json:"id"`
			Email    string `json:"email"`
			Username string `json:"username"`
			Status   string `json:"status"`
		}

		if err := bridge.UpdateOneWithFieldsB(q, &updated); err != nil {
			t.Fatalf("update_subquery_error: %v", err)
		}

		if updated.ID != userB.ID {
			t.Fatalf("update_subquery_id_mismatch expected=%d actual=%d", userB.ID, updated.ID)
		}

		if updated.Status != "active" {
			t.Fatalf("update_subquery_status_mismatch expected=%q actual=%q", "active", updated.Status)
		}
	})

	mustStep(t, "07_delete_with_subquery", func(t *testing.T) {
		sub := ndb.NewReadQuery(usersTable.PName).
			Fields("users.id").
			Where(ndb.M{"id": userB.ID}).
			Limit(1)

		q := ndb.NewDeleteQuery(userPayments.PName).
			SubQueryName("sub", sub, ndb.Fs("id")).Where(ndb.M{
			"user_payments.user_id": ndb.M{"eq_field": "sub.id"},
		})

		affected, err := bridge.DeleteWithRowsAffected(q)
		if err != nil {
			t.Fatalf("delete_subquery_error: %v", err)
		}
		if affected != int64(len(payB)) {
			t.Fatalf("delete_subquery_rows_mismatch expected=%d actual=%d", len(payB), affected)
		}

		readA := ndb.NewReadQuery(userPayments.PName).Where(ndb.M{"user_id": userA.ID})
		var leftA []UserPayment
		if err := bridge.ReadB(readA, &leftA); err != nil {
			t.Fatalf("read_payments_userA_error: %v", err)
		}
		if len(leftA) != len(payA) {
			t.Fatalf("userA_payments_len_mismatch expected=%d actual=%d", len(payA), len(leftA))
		}
	})
}
