package test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nitsugaro/go-ndb"
)

/* -------------------------------------------------
   SCHEMAS
---------------------------------------------------*/

type User struct {
	ID        uint      `json:"id"`
	PublicID  string    `json:"public_id"`
	Email     string    `json:"email"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"created_at"`
}

type UserPayment struct {
	ID        uint      `json:"id"`
	UserID    uint      `json:"user_id"`
	Amount    float64   `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}

type UserPaymentAgg struct {
	RowCount    int64   `json:"row_count"`    // COUNT(*)
	TotalAmount float64 `json:"total_amount"` // SUM(user_payments.amount)
	Username    string  `json:"username"`     // users.username
}

var usersTable = ndb.NewSchema("users").
	Comment("User Table").
	Extension(`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`).
	UniqueIndex("public_id").
	Indexes("email", "username").
	NewField("id").Type(ndb.FIELD_BIG_SERIAL).PK().DoneField().
	NewField("public_id").Type(ndb.FIELD_UUID).Unique().DoneField().
	NewField("email").Type(ndb.FIELD_VARCHAR).Max(254).Unique().DoneField().
	NewField("username").Type(ndb.FIELD_VARCHAR).Max(100).Unique().Nullable().DoneField().
	NewField("status").Type(ndb.FIELD_VARCHAR).Max(20).Default("'active'").DoneField().
	NewField("created_at").Type(ndb.FIELD_TIMESTAMP).Default("now()").DoneField().
	NewField("updated_at").Type(ndb.FIELD_TIMESTAMP).Default("now()").DoneField()

var userType = ndb.NewSchema("users_type").
	NewField("user_id").Type(ndb.FIELD_BIG_INT).NewFK(usersTable.GetName(), "id").OnDelete(ndb.CASCADE).DoneFK().DoneField().
	NewField("type").Type(ndb.FIELD_TEXT).Max(100).Default("'client'").DoneField()

/* -------------------------------------------------
   SCHEMA: user_payments
---------------------------------------------------*/

var userPayments = ndb.NewSchema("user_payments").
	NewField("id").Type(ndb.FIELD_BIG_SERIAL).PK().DoneField().
	NewField("user_id").Type(ndb.FIELD_BIG_INT).Nullable().NewFK(usersTable.GetName(), "id").OnDelete(ndb.CASCADE).DoneFK().DoneField().
	NewField("amount").Type(ndb.FIELD_FLOAT).Nullable().DoneField().
	NewField("created_at").Type(ndb.FIELD_TIMESTAMP).Default("now()").Nullable().DoneField().
	NewField("arr_field").Type(ndb.FIELD_SMALL_INT_ARRAY).Enum("0", "1", "2").Nullable().DoneField()

/* -------------------------------------------------
   TEST CASE
---------------------------------------------------*/

func TestUser(t *testing.T) {
	var (
		userA, userB  User
		expectedSum   float64
		paymentValues = []float64{10.5, 20.75, 32.10, 100.0}
	)

	must(t, "01_schema_reset", func(t *testing.T) {
		bridge.DeleteSchema(userPayments.GetName())
		bridge.DeleteSchema(userType.GetName())
		bridge.DeleteSchema(usersTable.GetName())

		if err := bridge.CreateSchema(usersTable); err != nil {
			t.Fatalf("create_schema_users: %v", err)
		}
		if err := bridge.CreateSchema(userType); err != nil {
			t.Fatalf("create_schema_users_type: %v", err)
		}
		if err := bridge.CreateSchema(userPayments); err != nil {
			t.Fatalf("create_schema_user_payments: %v", err)
		}
	})

	must(t, "02_insert_userA", func(t *testing.T) {
		payloadA := ndb.M{
			"public_id": uuid.NewString(),
			"email":     "userA@test.com",
			"username":  "userA",
		}

		qA := ndb.NewCreateQuery(usersTable.GetName()).
			Payload(payloadA).
			Fields("id", "public_id", "email", "username", "created_at")

		if err := bridge.CreateOneB(qA, &userA); err != nil {
			t.Fatalf("insert_userA_error: %v", err)
		}

		vinfo(t, "userA=%+v", userA)
	})

	must(t, "03_insert_userB", func(t *testing.T) {
		payloadB := ndb.M{
			"public_id": uuid.NewString(),
			"email":     "userB@test.com",
			"username":  "userB",
		}

		qB := ndb.NewCreateQuery(usersTable.GetName()).
			Payload(payloadB).
			Fields("id", "public_id", "email", "username", "created_at")

		if err := bridge.CreateOneB(qB, &userB); err != nil {
			t.Fatalf("insert_userB_error: %v", err)
		}

		vinfo(t, "userB=%+v", userB)
	})

	must(t, "04_insert_users_type", func(t *testing.T) {
		typePayload := ndb.M{"user_id": userB.ID, "type": "USER_ADMIN"}

		if _, err := bridge.Create(ndb.NewCreateQuery(userType.GetName()).Payload(typePayload)); err != nil {
			t.Fatalf("insert_user_type_error: %v", err)
		}
	})

	must(t, "05_insert_payments_and_read_sum", func(t *testing.T) {
		for _, amount := range paymentValues {
			p := ndb.M{"user_id": userB.ID, "amount": amount}
			if _, err := bridge.Create(ndb.NewCreateQuery(userPayments.GetName()).Payload(p)); err != nil {
				t.Fatalf("insert_payment_error amount=%v: %v", amount, err)
			}
			expectedSum += amount
		}

		readPayments := ndb.NewReadQuery(userPayments.GetName()).Where(ndb.M{"user_id": userB.ID}).Order(ndb.Fs("user_payments.amount", "ASC"))

		var payments []UserPayment
		if err := bridge.ReadB(readPayments, &payments); err != nil {
			t.Fatalf("read_payments_error: %v", err)
		}

		var sum float64
		for _, p := range payments {
			sum += p.Amount
		}

		if sum != expectedSum {
			t.Fatalf("payments_sum_mismatch expected=%v actual=%v", expectedSum, sum)
		}

		vinfo(t, "payments=%d expectedSum=%v", len(payments), expectedSum)
	})

	must(t, "06_join_agg", func(t *testing.T) {
		readJoin := ndb.NewReadQuery(userType.GetName()).
			NewField("users_type.user_id").Distinct().Count().As("row_count").DoneField().
			NewField("user_payments.amount").Sum().As("total_amount").DoneField().
			NewField("users.username").DoneField().
			NewJoin(usersTable.GetName(), ndb.INNER_JOIN).On(ndb.M{"users_type.user_id": ndb.M{"eq_field": "users.id"}}).DoneJoin().
			NewJoin(userPayments.GetName(), ndb.LEFT_JOIN).On(ndb.M{"user_payments.user_id": ndb.M{"eq_field": "users.id"}}).DoneJoin().
			Group(ndb.Fs("users.username"))

		var aggRows []UserPaymentAgg
		if err := bridge.ReadB(readJoin, &aggRows); err != nil {
			t.Fatalf("join_query_error: %v", err)
		}
		if len(aggRows) != 1 {
			t.Fatalf("join_row_count_invalid expected=1 actual=%d rows=%v", len(aggRows), aggRows)
		}

		agg := aggRows[0]
		if agg.RowCount != 1 {
			t.Fatalf("join_count_mismatch expected=1 actual=%d row=%+v", agg.RowCount, agg)
		}
		if agg.TotalAmount != expectedSum {
			t.Fatalf("join_sum_mismatch expected=%v actual=%v row=%+v", expectedSum, agg.TotalAmount, agg)
		}
		if agg.Username != userB.Username {
			t.Fatalf("join_username_mismatch expected=%q actual=%q row=%+v", userB.Username, agg.Username, agg)
		}

		vinfo(t, "agg=%+v", agg)
	})

	must(t, "07_rest_query_create", func(t *testing.T) {
		userName := "user_rest"
		email := "user_rest@example.com"
		createQuery, _ := ndb.NewQueryFromURIParams(usersTable.GetName(), "POST", map[string][]string{
			"f": {"username,email"},
		})

		createQuery.Payload(ndb.M{"username": userName, "email": email, "public_id": uuid.NewString()})

		result, err := bridge.CreateOne(createQuery)
		if err != nil {
			t.Fatalf("rest_query_create error creating user %s", err.Error())
		}

		if len(result) != 2 {
			t.Fatalf("rest_query_create_mismatch map fields expected=2 actual=%q", len(result))
		}

		if result["username"].(string) != userName {
			t.Fatalf("rest_query_create_mismatch username expected=%q actual=%q", userName, result["username"])
		}

		if result["email"].(string) != email {
			t.Fatalf("rest_query_create_mismatch email expected=%q actual=%q", email, result["email"])
		}
	})

	must(t, "08_rest_query_read", func(t *testing.T) {
		userName := "user_rest"
		readQuery, _ := ndb.NewQueryFromURIParams(usersTable.GetName(), "GET", map[string][]string{
			"q": {`username/eq/"` + userName + `"`},
			"f": {"email"},
		})

		result, err := bridge.Read(readQuery)
		if err != nil {
			t.Fatalf("rest_query_read error reading user %s", err.Error())
		}

		if len(result) != 1 {
			t.Fatalf("rest_query_read_mismatch map fields expected=1 actual=%q", len(result))
		}
	})

	must(t, "09_rest_query_update", func(t *testing.T) {
		newEmail := "newemail@example.com"
		userName := "user_rest"
		updateQuery, _ := ndb.NewQueryFromURIParams(usersTable.GetName(), "PUT", map[string][]string{
			"q": {`username/eq/"` + userName + `"`},
			"f": {"email"},
		})

		result, err := bridge.UpdateOneWithFields(updateQuery.Payload(ndb.M{"email": newEmail}))
		if err != nil {
			t.Fatalf("rest_query_update error updating user %s", err.Error())
		}

		if len(result) != 1 {
			t.Fatalf("rest_query_update_mismatch map fields expected=1 actual=%q", len(result))
		}

		if result["email"].(string) != newEmail {
			t.Fatalf("rest_query_update_mismatch email expected=%q actual=%q", newEmail, result["email"])
		}
	})

	must(t, "10_rest_delete_update", func(t *testing.T) {
		newEmail := "newemail@example.com"
		deleteQuery, _ := ndb.NewQueryFromURIParams(usersTable.GetName(), "DELETE", map[string][]string{
			"q": {`email/eq/"` + newEmail + `"`},
			"f": {"public_id"},
		})

		result, err := bridge.DeleteOneWithFields(deleteQuery.Payload(ndb.M{"email": newEmail}))
		if err != nil {
			t.Fatalf("rest_query_delete error updating user %s", err.Error())
		}

		if len(result) != 1 {
			t.Fatalf("rest_query_delete_mismatch map fields expected=1 actual=%q", len(result))
		}

		if _, ok := result["public_id"].(string); !ok {
			t.Fatalf("rest_query_delete_mismatch expecte to get public_id field")
		}
	})

	must(t, "11_arr_field_ok", func(t *testing.T) {
		if _, err := bridge.Create(ndb.NewCreateQuery(userPayments.GetName()).Payload(ndb.M{
			"user_id":   userB.ID,
			"amount":    12.0,
			"arr_field": []int16{1},
		})); err != nil {
			t.Fatalf("arr_field insert error: %v", err)
		}
	})

	must(t, "12_arr_field_rejects", func(t *testing.T) {
		_, err := bridge.Create(ndb.NewCreateQuery(userPayments.GetName()).Payload(ndb.M{
			"user_id":   userB.ID,
			"amount":    12.0,
			"arr_field": []int16{10},
		}))
		if err == nil {
			t.Fatalf("expected enum constraint error, got nil")
		}
	})

	bridge.DeleteSchema(userPayments.GetName())
	bridge.DeleteSchema(userType.GetName())
	bridge.DeleteSchema(usersTable.GetName())
}
