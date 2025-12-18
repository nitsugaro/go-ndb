# go-ndb

Minimal query builder + schema manager for PostgreSQL, focused on:

- **Practical CRUD**
- **Safe payload binding**
- **Composable queries** (joins, grouping, order, subqueries)
- **Transactions + middlewares**
- **Dynamic query + schema construction**, designed for platforms where the backend must execute **runtime-defined** SQL.

The goal is to allow not only writing SQL in Go, but also **building SQL dynamically** from:

- workflow/journey builders
- admin panels
- SaaS where customers define tables/fields
- engines that generate SQL from JSON or interpreted rules

This library ensures **safety, predictability and strict API structure**.

---

## 📦 Installation

```bash
go get github.com/nitsugaro/go-ndb@v1.2.2
```

---

## 🧩 Core Concepts

### Schema

A `Schema` holds table definition + fields + indexes + FK rules.

### Field types supported

```
SMALLINT, SMALLSERIAL, INT, BIGINT, SERIAL, BIGSERIAL
VARCHAR, TEXT
UUID
BOOLEAN
TIMESTAMP
JSONB
FLOAT, DOUBLE PRECISION
```

### FK rules

```
NO ACTION, RESTRICT, CASCADE, SET NULL, SET DEFAULT
```

---

## ⚙️ Quick Start

```go
import (
  "database/sql"
  "github.com/nitsugaro/go-ndb"
  "github.com/nitsugaro/go-nstore"
)

db, _ := sql.Open("postgres", "<dsn>")

store := nstore.NewNStorage[*ndb.Schema]()
bridge := ndb.NewBridge(&ndb.NBridge{
  DB:            db,
  SchemaPrefix:  "",
  SchemaStorage: store,
})
```

---

# 🧱 Defining Schemas

Example matching the real test code:

```go
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
```

Creating / deleting schemas:

```go
_ = bridge.DeleteSchema(usersTable.PName)
_ = bridge.CreateSchema(usersTable)
```

---

# 🧠 Query Builder

## CREATE

```go
q := ndb.NewCreateQuery("users").
  Payload(ndb.M{"email":"a@test.com"}).
  Fields("id", "email", "created_at")
```

## READ

```go
q := ndb.NewReadQuery("users").
  Where(ndb.M{"status":"active"}).
  Fields("id","email","username").
  Order(ndb.Fs("users.created_at", "DESC")).
  Limit(10)
```

## UPDATE

```go
q := ndb.NewUpdateQuery("users").
  Payload(ndb.M{"email":"updated@test.com"}).
  Where(ndb.M{"id":10}).
  Fields("id","email")
```

## DELETE

```go
q := ndb.NewDeleteQuery("users").
  Where(ndb.M{"id":10})
```

---

# 💾 CRUD EXAMPLES

### Insert One (CreateOneB)

```go
type User struct {
  ID uint `json:"id"`
  Email string `json:"email"`
}

payload := ndb.M{
  "public_id": uuid.NewString(),
  "email":     "user@test.com",
  "username":  "user",
}

q := ndb.NewCreateQuery(usersTable.PName).
  Payload(payload).
  Fields("id","public_id","email","username","created_at")

var u User
if err := bridge.CreateOneB(q, &u); err != nil {
  panic(err)
}
```

---

### Read

```go
var users []User

q := ndb.NewReadQuery(usersTable.PName).
  Where(ndb.M{"status":"active"}).
  Order(ndb.Fs("users.id","ASC")).
  Fields("id","email","username")

if err := bridge.ReadB(q, &users); err != nil {
  panic(err)
}
```

---

### Update (UpdateOneWithFieldsB)

```go
q := ndb.NewUpdateQuery(usersTable.PName).
  Payload(ndb.M{"email":"user+updated@test.com"}).
  Where(ndb.M{"id": u.ID}).
  Fields("id","email","username")

var updated User
if err := bridge.UpdateOneWithFieldsB(q, &updated); err != nil {
  panic(err)
}
```

---

### Delete → DeleteWithRowsAffected

```go
q := ndb.NewDeleteQuery(usersTable.PName).
  Where(ndb.M{"id": u.ID})

rows, err := bridge.DeleteWithRowsAffected(q)
if err != nil { panic(err) }
fmt.Println(rows)
```

---

# 🔗 Joins & Aggregations

```go
readJoin := ndb.NewReadQuery(userType.PName).
  NewField("users_type.user_id").Distinct().Count().As("row_count").DoneField().
  NewField("user_payments.amount").Sum().As("total_amount").DoneField().
  NewField("users.username").DoneField().

  NewJoin(usersTable.PName, ndb.INNER_JOIN).
    On(ndb.M{"users_type.user_id": ndb.M{"eq_field": "users.id"}}).
    DoneJoin().

  NewJoin(userPayments.PName, ndb.LEFT_JOIN).
    On(ndb.M{"user_payments.user_id": ndb.M{"eq_field": "users.id"}}).
    DoneJoin().

  Group(ndb.Fs("users.username"))
```

---

# 🌀 Subqueries (UPDATED)

### Create + SubQuery

```go
sub := ndb.NewReadQuery(usersTable.PName).
  Fields("users.id").
  Where(ndb.M{"id": userB.ID}).
  Limit(1)

q := ndb.NewCreateQuery(userType.PName).
  SubQuery(sub, ndb.Fs("user_id")).
  Fields("user_id","type")

_, err := bridge.Create(q)
```

---

### Read + Named SubQuery

```go
sub := ndb.NewReadQuery(userPayments.PName).
  NewField("user_payments.user_id").As("user_id").DoneField().
  NewField("user_payments.amount").Sum().As("total_amount").DoneField().
  Where(ndb.M{"user_id": userB.ID}).
  Group(ndb.Fs("user_payments.user_id")).
  Limit(1)

outer := ndb.NewReadQuery(usersTable.PName).
  SubQueryName("sq", sub, ndb.Fs("user_id","total_amount")).
  NewField("sq.user_id").As("user_id").DoneField().
  NewField("sq.total_amount").As("total_amount").DoneField().
  NewField("users.username").DoneField().
  NewJoin(usersTable.PName, ndb.INNER_JOIN).
    On(ndb.M{"users.id": ndb.M{"eq_field": "sq.user_id"}}).
    DoneJoin().
  Where(ndb.M{"users.id": userB.ID}).
  Limit(1)
```

---

# 🔄 Update / Delete with Subqueries

### UPDATE + SubQuery

```go
sub := ndb.NewReadQuery(usersTable.PName).
  Fields("users.status").
  Where(ndb.M{"id": userA.ID}).
  Limit(1)

q := ndb.NewUpdateQuery(usersTable.PName).
  SubQuery(sub, ndb.Fs("status")).
  Where(ndb.M{"id": userB.ID}).
  Fields("id","email","username","status")

var updated ...
err := bridge.UpdateOneWithFieldsB(q, &updated)
```

---

### DELETE + SubQueryName

```go
sub := ndb.NewReadQuery(usersTable.PName).
  Fields("users.id").
  Where(ndb.M{"id": userB.ID}).
  Limit(1)

q := ndb.NewDeleteQuery(userPayments.PName).
  SubQueryName("sub", sub, ndb.Fs("id")).
  Where(ndb.M{
    "user_payments.user_id": ndb.M{"eq_field": "sub.id"},
  })

rows, _ := bridge.DeleteWithRowsAffected(q)
```

---

# 🧮 Field Operations (Aggregates, Min/Max/Count)

```go
read := ndb.NewReadQuery(usersTable.PName).
  NewField("users.id").Count().As("row_count").DoneField().
  NewField("users.email").Min().As("min_email").DoneField().
  NewField("users.email").Max().As("max_email").DoneField()
```

---

# 💥 Transactions

```go
err := bridge.Transaction(func(tx *ndb.DBBridge) error {
  q := ndb.NewCreateQuery("trx_users").
    Payload(ndb.M{"email":"commit@test.com"}).
    Fields("id","email","created_at")

  var row TrxUser
  if err := tx.CreateOneB(q, &row); err != nil {
    return err
  }
  return nil
})
```

---

# 🧰 Middlewares

```go
bridge.AddMiddleware(func(q *ndb.Query) error {
  if q.Type() == ndb.DELETE && len(q.Where) == 0 {
    return fmt.Errorf("DELETE without WHERE is forbidden")
  }
  return nil
})
```

---

# ❓ FAQ

### What is `M`?

```go
type M = map[string]any
```

### Default limit

If not set, `GetLimit()` returns `100`.

---

# 📜 License

MIT
