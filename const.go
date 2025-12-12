package ndb

type AlterAction string

const (
	ADD_COLUMN   AlterAction = "ADD"
	ALTER_COLUMN AlterAction = "ALTER"
	DROP_COLUMN  AlterAction = "DROP"
)

type ForeignKeyRule string

const (
	NO_ACTION   ForeignKeyRule = "NO ACTION"
	RESTRICT    ForeignKeyRule = "RESTRICT"
	CASCADE     ForeignKeyRule = "CASCADE"
	SET_NULL    ForeignKeyRule = "SET NULL"
	SET_DEFAULT ForeignKeyRule = "SET DEFAULT"
)

var foreignKeyRules = []ForeignKeyRule{NO_ACTION, RESTRICT, CASCADE, SET_NULL, SET_DEFAULT}

type SchemaFieldType string

const (
	FIELD_SMALL_INT    SchemaFieldType = "SMALLINT"
	FIELD_SMALL_SERIAL SchemaFieldType = "SMALLSERIAL"
	FIELD_INT          SchemaFieldType = "INT"
	FIELD_BIG_INT      SchemaFieldType = "BIGINT"
	FIELD_SERIAL       SchemaFieldType = "SERIAL"
	FIELD_BIG_SERIAL   SchemaFieldType = "BIGSERIAL"
	FIELD_VARCHAR      SchemaFieldType = "VARCHAR"
	FIELD_TEXT         SchemaFieldType = "TEXT"
	FIELD_UUID         SchemaFieldType = "UUID"
	FIELD_BOOLEAN      SchemaFieldType = "BOOLEAN"
	FIELD_TIMESTAMP    SchemaFieldType = "TIMESTAMP"
	FIELD_JSONB        SchemaFieldType = "JSONB"
	FIELD_FLOAT        SchemaFieldType = "FLOAT"
	FIELD_DOUBLE       SchemaFieldType = "DOUBLE PRECISION"
)

type JoinType string

const (
	INNER_JOIN JoinType = "INNER"
	LEFT_JOIN  JoinType = "LEFT"
	RIGHT_JOIN JoinType = "RIGHT"
	FULL_JOIN  JoinType = "FULL OUTER"
	CROSS_JOIN JoinType = "CROSS"
)

var allowedJoins = []string{string(INNER_JOIN), string(LEFT_JOIN), string(RIGHT_JOIN), string(FULL_JOIN), string(CROSS_JOIN)}

type QueryType string

const (
	READ   QueryType = "READ"
	CREATE QueryType = "CREATE"
	UPDATE QueryType = "UPDATE"
	DELETE QueryType = "DELETE"
)
