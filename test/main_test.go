package test

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	_ "github.com/lib/pq"
	goconf "github.com/nitsugaro/go-conf"
	"github.com/nitsugaro/go-ndb"
	"github.com/nitsugaro/go-nstore"
)

var usersTable = &ndb.Schema{
	Name:    "users",
	Comment: "Tabla principal de usuarios",
	Extensions: []string{
		`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`,
	},
	Fields: []ndb.Field{
		{
			Name:       "id",
			Type:       ndb.FieldBigSerial,
			PrimaryKey: true,
		},
		{
			Name:     "public_id",
			Type:     ndb.FieldUUID,
			Nullable: false,
			Unique:   true,
		},
		{
			Name:     "email",
			Type:     ndb.FieldVarchar,
			Max:      ndb.Ptr(254),
			Nullable: false,
			Unique:   true,
		},
		{
			Name:     "email_verified",
			Type:     ndb.FieldBoolean,
			Default:  ndb.Ptr("false"),
			Nullable: false,
		},
		{
			Name:     "phone",
			Type:     ndb.FieldVarchar,
			Max:      ndb.Ptr(20),
			Unique:   true,
			Nullable: true,
		},
		{
			Name:     "phone_verified",
			Type:     ndb.FieldBoolean,
			Default:  ndb.Ptr("false"),
			Nullable: false,
		},
		{
			Name:     "password_hash",
			Type:     ndb.FieldVarchar,
			Max:      ndb.Ptr(100),
			Nullable: true,
		},
		{
			Name:     "given_name",
			Type:     ndb.FieldVarchar,
			Max:      ndb.Ptr(100),
			Nullable: true,
		},
		{
			Name:     "family_name",
			Type:     ndb.FieldVarchar,
			Max:      ndb.Ptr(100),
			Nullable: true,
		},
		{
			Name:     "full_name",
			Type:     ndb.FieldVarchar,
			Max:      ndb.Ptr(200),
			Nullable: true,
		},
		{
			Name:       "username",
			Type:       ndb.FieldVarchar,
			Max:        ndb.Ptr(100),
			Unique:     true,
			Nullable:   true,
			EnumValues: []string{"agus", "nitsugaro", "rome"},
		},
		{
			Name:     "provider",
			Type:     ndb.FieldVarchar,
			Max:      ndb.Ptr(50),
			Nullable: true,
		},
		{
			Name:     "provider_subject",
			Type:     ndb.FieldVarchar,
			Max:      ndb.Ptr(255),
			Nullable: true,
		},
		{
			Name:     "status",
			Type:     ndb.FieldVarchar,
			Max:      ndb.Ptr(20),
			Default:  ndb.Ptr("'active'"),
			Nullable: false,
		},
		{
			Name:     "last_login_at",
			Type:     ndb.FieldTimestamp,
			Nullable: true,
		},
		{
			Name:     "created_at",
			Type:     ndb.FieldTimestamp,
			Default:  ndb.Ptr("now()"),
			Nullable: false,
		},
		{
			Name:     "updated_at",
			Type:     ndb.FieldTimestamp,
			Default:  ndb.Ptr("now()"),
			Nullable: false,
		},
	},
	UniqueIndexes: [][]string{
		{"public_id"},
	},
	Indexes: [][]string{
		{"email"},
		{"username"},
		{"provider", "provider_subject"},
	},
}

var userType = &ndb.Schema{
	Name: "users_type",
	Fields: []ndb.Field{
		{Name: "user_id", Type: ndb.FieldBigInt, ForeignKey: &ndb.ForeignKey{
			Schema:   "users",
			Column:   "id",
			OnDelete: ndb.Cascade,
		}},
		{
			Name:    "type",
			Type:    ndb.FieldText,
			Max:     ndb.Ptr(100),
			Default: ndb.Ptr("'client'"),
		},
	},
}

func TestMain(m *testing.M) {
	db, err := sql.Open("postgres", "postgres://postgres:password@localhost:5432/test?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	goconf.LoadConfig()

	storage, err := nstore.New[*ndb.Schema](goconf.GetOpField("ndb.schema.folder", "schemas"))
	if err != nil {
		panic(err)
	}

	storage.LoadFromDisk()

	bridge := ndb.NewBridge(&ndb.NBridge{DB: db, SchemaPrefix: "prefix_", SchemaStorage: storage})

	if _, ok := bridge.GetSchema("users"); !ok {
		if err := bridge.CreateSchema(usersTable); err != nil {
			fmt.Println("error creating schema: " + err.Error())
			return
		}
	}

	if _, ok := bridge.GetSchema("users_type"); !ok {
		if err := bridge.CreateSchema(userType); err != nil {
			fmt.Println("error creating schema: " + err.Error())
			return
		}
	}

	readQueryUser := ndb.NewReadQuery("users")
	readQueryUser.Where = []ndb.M{{
		"status": "active",
	}}
	user, _ := bridge.Read(readQueryUser)
	fmt.Println(user)
	if len(user.([]ndb.M)) == 0 {
		createQueryUser := ndb.NewCreateQuery("users")
		createQueryUser.Data = ndb.M{
			"email":     "nitsugaro@gmail.com",
			"public_id": "711cebd5-077b-4b54-839f-b0e9c9866565",
			"username":  "rome",
		}

		createdUser, _ := bridge.Create(createQueryUser)
		fmt.Println(createdUser)
		user = []any{createdUser}
	}

	readQueryUserType := ndb.NewReadQuery("users_type")
	readQueryUserType.Where = []ndb.M{{
		"user_id": 1,
	}}
	userType, _ := bridge.Read(readQueryUserType)
	fmt.Println(userType)
	if len(userType.([]ndb.M)) == 0 {
		createQueryType := ndb.NewCreateQuery("users_type")
		createQueryType.Data = ndb.M{
			"user_id": user.([]ndb.M)[0]["id"],
		}
		fmt.Println(bridge.Create(createQueryType))
	}

	readQueryJoin := ndb.NewReadQuery("users_type")
	readQueryJoin.Joins = []*ndb.Join{
		{BasicSchema: &ndb.BasicSchema{Schema: "users"}, Type: ndb.InnerJoin, On: []ndb.M{
			{"user_id": ndb.M{"eq_field": "users.id"}},
		}},
	}
	fmt.Println("read")
	fmt.Println(bridge.Read(readQueryJoin))

	/*
		fmt.Println(bridge.ModifySchema("users", []*ndb.AlterField{
			{Field: &ndb.Field{Name: "add_column", Type: ndb.FieldVarchar, Max: ndb.Ptr(55), Unique: true}, AlterAction: ndb.AddColumn},
			{Field: &ndb.Field{Name: "family_name", Type: ndb.FieldVarchar, Max: ndb.Ptr(55), Nullable: true}, AlterAction: ndb.AlterColumn,
				AlterOptions: &ndb.AlterOptions{
					NewName: ndb.Ptr("alter_column"),
				}},
		}))
	*/
}
