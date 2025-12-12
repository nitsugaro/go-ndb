package test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/nitsugaro/go-ndb"
)

func TestSQLInjectionHardening(t *testing.T) {
	mustStep(t, "01_reset_schemas", func(t *testing.T) {
		resetSchemas(t)
	})

	seed := func(t *testing.T) (User, User, User) {
		t.Helper()

		ins := func(email, username string) User {
			var u User
			q := ndb.NewCreateQuery(usersTable.PName).
				Payload(ndb.M{
					"public_id": uuid.NewString(),
					"email":     email,
					"username":  username,
				}).
				Fields("id", "public_id", "email", "username", "created_at")

			if err := bridge.CreateOneB(q, &u); err != nil {
				t.Fatalf("seed_user_error email=%q: %v", email, err)
			}
			return u
		}

		return ins("victim@test.com", "victim"),
			ins("admin@test.com", "admin"),
			ins("other@test.com", "other")
	}

	countUsers := func(t *testing.T) int64 {
		t.Helper()
		type Row struct {
			RowCount int64 `json:"row_count"`
		}
		var rows []Row
		q := ndb.NewReadQuery(usersTable.PName).NewField("users.id").Count().As("row_count").DoneField()

		if err := bridge.ReadB(q, &rows); err != nil {
			t.Fatalf("count_users_error: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("count_users_rows_invalid expected=1 actual=%d", len(rows))
		}
		return rows[0].RowCount
	}

	expectZeroOrErr := func(t *testing.T, q *ndb.Query, name string) {
		t.Helper()

		var got []User
		err := bridge.ReadB(q, &got)
		if err != nil {
			// error es aceptable: lo importante es que NO “bypassee” el filtro.
			return
		}
		if len(got) != 0 {
			t.Fatalf("%s_sqli_bypass detected rows=%d sample=%+v", name, len(got), got[0])
		}
	}

	expectErr := func(t *testing.T, q *ndb.Query, name string) {
		t.Helper()

		var got []User
		if err := bridge.ReadB(q, &got); err == nil {
			t.Fatalf("%s_expected_error_but_got_none rows=%d", name, len(got))
		}
	}

	mustStep(t, "02_seed_users", func(t *testing.T) {
		seed(t)
		if c := countUsers(t); c != 3 {
			t.Fatalf("seed_count_invalid expected=3 actual=%d", c)
		}
	})

	mustStep(t, "03_sqli_in_values_should_not_bypass_where", func(t *testing.T) {
		payloads := []string{
			`' OR 1=1 --`,
			`' OR '1'='1`,
			`x' OR 1=1 --`,
			`'/**/OR/**/1=1/**/--`,
			`' OR 1=1; --`,
			`'; DROP TABLE users; --`,
			`'; SELECT pg_sleep(1); --`,
		}

		for i, p := range payloads {
			q := ndb.NewReadQuery(usersTable.PName).
				Where(ndb.M{"email": p}).
				Fields("id", "email", "username").
				Limit(10)

			expectZeroOrErr(t, q, "email_payload_"+uuid.NewString()+"_"+string(rune('A'+i)))
		}
	})

	mustStep(t, "04_sqli_in_numeric_field_should_not_bypass", func(t *testing.T) {
		q := ndb.NewReadQuery(usersTable.PName).
			Where(ndb.M{"id": "1 OR 1=1"}).
			Fields("id", "email").
			Limit(10)

		expectZeroOrErr(t, q, "id_string_injection")
	})

	mustStep(t, "05_sqli_in_identifiers_must_error", func(t *testing.T) {
		// Si esto NO rompe, tenés un vector de SQLi por ORDER BY / identifiers.
		qOrder := ndb.NewReadQuery(usersTable.PName).
			Where(ndb.M{"email": "victim@test.com"}).
			Fields("id", "email").
			Order(ndb.Fs(`users.email DESC; DROP TABLE users;--`, "ASC")).
			Limit(10)

		expectErr(t, qOrder, "order_identifier_injection")

		qFields := ndb.NewReadQuery(usersTable.PName).
			Where(ndb.M{"email": "victim@test.com"}).
			Fields(`id, (SELECT 1);--`, "email").
			Limit(10)

		expectErr(t, qFields, "fields_identifier_injection")
	})

	mustStep(t, "06_post_checks_tables_intact", func(t *testing.T) {
		if c := countUsers(t); c != 3 {
			t.Fatalf("post_check_user_count_changed expected=3 actual=%d", c)
		}

		var victim []User
		q := ndb.NewReadQuery(usersTable.PName).
			Where(ndb.M{"email": "victim@test.com"}).
			Fields("id", "email", "username").
			Limit(10)

		if err := bridge.ReadB(q, &victim); err != nil {
			t.Fatalf("post_check_read_victim_error: %v", err)
		}
		if len(victim) != 1 {
			t.Fatalf("post_check_victim_len_invalid expected=1 actual=%d", len(victim))
		}
	})

	mustStep(t, "07b_create_user_with_sqli_like_payload_values", func(t *testing.T) {
		before := countUsers(t)

		email := `sqli' OR '1'='1;--@test.com`
		username := `u/*x*/';--`

		var created User
		q := ndb.NewCreateQuery(usersTable.PName).
			Payload(ndb.M{
				"public_id": uuid.NewString(),
				"email":     email,
				"username":  username,
			}).
			Fields("id", "public_id", "email", "username", "created_at")

		if err := bridge.CreateOneB(q, &created); err != nil {
			t.Fatalf("create_sqli_like_user_error: %v", err)
		}
		if created.Email != email {
			t.Fatalf("create_sqli_like_user_email_mismatch expected=%q actual=%q", email, created.Email)
		}
		if created.Username != username {
			t.Fatalf("create_sqli_like_user_username_mismatch expected=%q actual=%q", username, created.Username)
		}

		var got []User
		read := ndb.NewReadQuery(usersTable.PName).
			Where(ndb.M{"id": created.ID}).
			Fields("id", "email", "username").
			Limit(1)

		if err := bridge.ReadB(read, &got); err != nil {
			t.Fatalf("read_sqli_like_user_error: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("read_sqli_like_user_len_invalid expected=1 actual=%d", len(got))
		}
		if got[0].Email != email || got[0].Username != username {
			t.Fatalf("read_sqli_like_user_mismatch got=%+v", got[0])
		}

		after := countUsers(t)
		if after != before+1 {
			t.Fatalf("create_sqli_like_user_count_mismatch before=%d after=%d", before, after)
		}
	})

	bridge.DeleteSchema(userPayments.PName)
	bridge.DeleteSchema(userType.PName)
	bridge.DeleteSchema(usersTable.PName)
}
