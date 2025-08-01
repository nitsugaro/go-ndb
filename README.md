# go-ndb Usage Example

This project showcases how to leverage [`go-ndb`](https://github.com/nitsugaro/go-ndb) for dynamic and programmatic interaction with PostgreSQL schemas, using a powerful abstraction layer. With support for schema definitions, versioning, runtime modifications, filters, joins, and even middleware, it is ideal for building modular and scalable data-driven applications.

It also integrates with:
- [`go-conf`](https://github.com/nitsugaro/go-conf): for configuration loading.
- [`go-nstore`](https://github.com/nitsugaro/go-nstore): for persistent schema storage and versioning.

---

## ✨ Features Demonstrated

- ✅ Define and version table schemas via Go structs
- ✅ Automatically create or drop schemas from code
- ✅ Add or rename columns at runtime
- ✅ Advanced filtering with `AND`, `OR`, `IN`, `LIKE`, etc.
- ✅ Join multiple schemas and filter results
- ✅ Enforce foreign key constraints and cascading deletes
- ✅ Use middlewares to intercept and modify queries
- ✅ Save/load schema definitions from disk
- ✅ Full support for field types, enums, indexes and composite keys

---

## 📁 Schema Definitions

We define two schemas: `users` and `users_type`. These schemas include fields, indexes, default values, foreign keys, enums, and more.

### Users Table

```go
var usersTable = &ndb.Schema{
	Name:    "users",
	Comment: "Main users table",
	Fields: []ndb.Field{
		{Name: "id", Type: ndb.FieldBigSerial, PrimaryKey: true},
		{Name: "public_id", Type: ndb.FieldUUID, Nullable: false, Unique: true},
		{Name: "email", Type: ndb.FieldVarchar, Max: ndb.Ptr(254), Nullable: false, Unique: true},
		{Name: "email_verified", Type: ndb.FieldBoolean, Default: ndb.Ptr("false"), Nullable: false},
		{Name: "phone", Type: ndb.FieldVarchar, Max: ndb.Ptr(20), Unique: true, Nullable: true},
		{Name: "phone_verified", Type: ndb.FieldBoolean, Default: ndb.Ptr("false"), Nullable: false},
		{Name: "password_hash", Type: ndb.FieldVarchar, Max: ndb.Ptr(100)},
		{Name: "given_name", Type: ndb.FieldVarchar, Max: ndb.Ptr(100)},
		{Name: "family_name", Type: ndb.FieldVarchar, Max: ndb.Ptr(100)},
		{Name: "full_name", Type: ndb.FieldVarchar, Max: ndb.Ptr(200)},
		{Name: "username", Type: ndb.FieldVarchar, Max: ndb.Ptr(100), Unique: true, EnumValues: []string{"agus", "nitsugaro", "rome"}},
		{Name: "provider", Type: ndb.FieldVarchar, Max: ndb.Ptr(50)},
		{Name: "provider_subject", Type: ndb.FieldVarchar, Max: ndb.Ptr(255)},
		{Name: "status", Type: ndb.FieldVarchar, Max: ndb.Ptr(20), Default: ndb.Ptr("'active'"), Nullable: false},
		{Name: "last_login_at", Type: ndb.FieldTimestamp},
		{Name: "created_at", Type: ndb.FieldTimestamp, Default: ndb.Ptr("now()"), Nullable: false},
		{Name: "updated_at", Type: ndb.FieldTimestamp, Default: ndb.Ptr("now()"), Nullable: false},
	},
	UniqueIndexes: [][]string{{"public_id"}},
	Indexes: [][]string{
		{"email"},
		{"username"},
		{"provider", "provider_subject"},
	},
}
```

### Users Type Table

```go
var userType = &ndb.Schema{
	Name: "users_type",
	Fields: []ndb.Field{
		{
			Name: "user_id", Type: ndb.FieldBigInt, ForeignKey: &ndb.ForeignKey{
				Schema: "users", Column: "id", OnDelete: ndb.Cascade,
			},
		},
		{Name: "type", Type: ndb.FieldText, Max: ndb.Ptr(100), Default: ndb.Ptr("'client'")},
	},
}
```

---

## 🧪 Bootstrapping the Bridge

```go
db, _ := sql.Open("postgres", "postgres://postgres:password@localhost:5432/test?sslmode=disable")
goconf.LoadConfig()

storage, _ := nstore.New[*ndb.Schema](goconf.GetOpField("ndb.schema.folder", "schemas"))
storage.LoadFromDisk()

bridge := ndb.NewBridge(&ndb.NBridge{
	DB:            db,
	SchemaPrefix:  "prefix_",
	SchemaStorage: storage,
})
```

---

## ⚙️ Create or Delete Schemas

```go
// Create schema if missing
if _, exists := bridge.GetSchema("users"); !exists {
	_ = bridge.CreateSchema(usersTable)
}

// Delete a schema
_ = bridge.DeleteSchema("old_schema")
```

---

## 🧱 Modify Schema

You can add, rename, or alter fields at runtime.

```go
_ = bridge.ModifySchema("users", []*ndb.AlterField{
	{
		Field:       &ndb.Field{Name: "nickname", Type: ndb.FieldVarchar, Max: ndb.Ptr(50)},
		AlterAction: ndb.AddColumn,
	},
	{
		Field: &ndb.Field{Name: "full_name", Type: ndb.FieldVarchar, Max: ndb.Ptr(100)},
		AlterAction: ndb.AlterColumn,
		AlterOptions: &ndb.AlterOptions{
			NewName: ndb.Ptr("display_name"),
		},
	},
})
```

---

## 👀 Read Queries with Filters

### Basic `WHERE` clause

```go
read := ndb.NewReadQuery("users")
read.Where = []ndb.M{{"email_verified": true}}
res, _ := bridge.Read(read)
```

### Combined `AND` conditions

```go
read := ndb.NewReadQuery("users")
read.Where = []ndb.M{{
	"email_verified": true,
	"status":         "active",
}}
```

### `OR` conditions

```go
read := ndb.NewReadQuery("users")
read.Where = []ndb.M{
	{"status": "disabled"},
	{"username": "agus"},
}
```

### `IN` clause

```go
read := ndb.NewReadQuery("users")
read.Where = []ndb.M{{
	"username": ndb.M{"in": []string{"agus", "nitsugaro", "rome"}},
}}
```

### `LIKE` matching

```go
read := ndb.NewReadQuery("users")
read.Where = []ndb.M{
	{"email": ndb.M{"like": "%@gmail.com"}},
}
```

---

## 🔀 Ordering and Pagination

```go
read := ndb.NewReadQuery("users")
read.OrderBy = []string{"created_at", "DESC"}
read.Limit = 10
read.Offset = 20
```

---

## 🔁 Join Queries

```go
read := ndb.NewReadQuery("users_type")
read.Joins = []*ndb.Join{
	{
		Type: ndb.InnerJoin,
		BasicSchema: &ndb.BasicSchema{Schema: "users"},
		On: []ndb.M{
			{"user_id": ndb.M{"eq_field": "users.id"}},
		},
	},
}
joined, _ := bridge.Read(read)
```

---

## ➕ Insert Records

```go
create := ndb.NewCreateQuery("users")
create.Data = ndb.M{
	"email":      "someone@example.com",
	"public_id":  "f47ac10b-58cc-4372-a567-0e02b2c3d479",
	"username":   "newuser",
	"status":     "active",
	"created_at": "now()",
}
user, _ := bridge.Create(create)
```

---

## ✏️ Update Records

```go
update := ndb.NewUpdateQuery("users")
update.Data = ndb.M{"email_verified": true}
update.Where = []ndb.M{{"username": "newuser"}}
bridge.Update(update)
```

---

## ❌ Delete Records

```go
del := ndb.NewDeleteQuery("users")
del.Where = []ndb.M{{"status": "disabled"}}
bridge.Delete(del)
```

---

## 🧩 Using Middleware

Middleware lets you inspect or modify queries:

```go
bridge.AddMiddleware(func(query any) error {
	fmt.Printf("Intercepted query: %#v\n", query)
	return nil
})
```

---

## 🧠 Listing Available Schemas

```go
schemas := bridge.GetSchemas()
for _, s := range schemas {
	fmt.Println(s.Name)
}
```

Or filtered by name prefix:

```go
schemas := bridge.GetSchemas(func(s *ndb.Schema) bool {
	return strings.HasPrefix(s.Name, "user")
})
```

---

## 🧬 Enum Support

Enum values are defined directly on a field:

```go
{
	Name:       "username",
	Type:       ndb.FieldVarchar,
	Max:        ndb.Ptr(100),
	Nullable:   true,
	Unique:     true,
	EnumValues: []string{"agus", "nitsugaro", "rome"},
}
```

---

## 🧨 Composite Indexes and Keys

```go
&ndb.Schema{
	Name: "sessions",
	CompositePrimaryKey: []string{"user_id", "device_id"},
	CompositeUniqueKeys: [][]string{{"user_id", "token"}},
}
```

---

## ⚠️ Foreign Key Cascades

```go
{
	Name: "user_id", Type: ndb.FieldBigInt,
	ForeignKey: &ndb.ForeignKey{
		Schema:   "users",
		Column:   "id",
		OnDelete: ndb.Cascade,
	},
}
```