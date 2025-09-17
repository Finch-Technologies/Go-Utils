package types

type ValueStoreMode string

const (
	ValueStoreModeString ValueStoreMode = "string"
	ValueStoreModeObject ValueStoreMode = "object"
)

type DbOptions struct {
	Driver           string //redis, dynamodb
	DbName           string
	TableName        string
	PrimaryKey       string
	TTLAttribute     string
	SortKeyAttribute string
	ValueStoreMode   ValueStoreMode
	ValueAttribute   string
}
