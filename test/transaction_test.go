package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nitsugaro/go-ndb"
)

type TrxUser struct {
	ID        uint      `json:"id"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

var trxUsersTable = ndb.NewSchema("trx_users").
	Comment("Transaction test table").
	Indexes("email").
	NewField("id").Type(ndb.FIELD_BIG_SERIAL).PK().DoneField().
	NewField("email").Type(ndb.FIELD_VARCHAR).Max(254).Unique().DoneField().
	NewField("created_at").Type(ndb.FIELD_TIMESTAMP).Default("now()").DoneField()

func countTrxUsers(t *testing.T) int64 {
	t.Helper()

	type Row struct {
		RowCount int64 `json:"row_count"`
	}

	q := ndb.NewReadQuery(trxUsersTable.PName).NewField("trx_users.id").Count().As("row_count").DoneField()

	var rows []Row
	if err := bridge.ReadB(q, &rows); err != nil {
		t.Fatalf("count_trx_users_error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("count_trx_users_len_invalid expected=1 actual=%d", len(rows))
	}

	return rows[0].RowCount
}

func TestTransactionCommitAndRollback(t *testing.T) {
	mustStep(t, "01_reset_schema", func(t *testing.T) {
		_ = bridge.DeleteSchema(trxUsersTable.PName)
		if err := bridge.CreateSchema(trxUsersTable); err != nil {
			t.Fatalf("create_schema_error: %v", err)
		}
	})

	mustStep(t, "02_commit_inserts_row", func(t *testing.T) {
		before := countTrxUsers(t)

		err := bridge.Transaction(func(tx *ndb.DBBridge) error {
			q := ndb.NewCreateQuery(trxUsersTable.PName).
				Payload(ndb.M{"email": "commit@test.com"}).
				Fields("id", "email", "created_at")

			var u TrxUser
			if err := tx.CreateOneB(q, &u); err != nil {
				return err
			}
			if u.ID == 0 {
				return fmt.Errorf("commit_insert_invalid_id")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("transaction_commit_error: %v", err)
		}

		after := countTrxUsers(t)
		if after != before+1 {
			t.Fatalf("commit_count_mismatch before=%d after=%d", before, after)
		}
	})

	mustStep(t, "03_rollback_does_not_persist_row", func(t *testing.T) {
		before := countTrxUsers(t)

		err := bridge.Transaction(func(tx *ndb.DBBridge) error {
			q := ndb.NewCreateQuery(trxUsersTable.PName).
				Payload(ndb.M{"email": "rollback@test.com"}).
				Fields("id", "email", "created_at")

			var u TrxUser
			if err := tx.CreateOneB(q, &u); err != nil {
				return err
			}
			return fmt.Errorf("force_rollback")
		})

		if err != nil {
			t.Logf("transaction returned error (ok): %v", err)
		}

		after := countTrxUsers(t)
		if after != before {
			t.Fatalf("rollback_count_mismatch before=%d after=%d", before, after)
		}
	})

	mustStep(t, "04_cleanup", func(t *testing.T) {
		_ = bridge.DeleteSchema(trxUsersTable.PName)
	})
}
