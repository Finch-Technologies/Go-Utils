package dynamo

type ValueStoreMode string

const (
	ValueStoreModeString ValueStoreMode = "string"
	ValueStoreModeObject ValueStoreMode = "object"
)

type DbOptions struct {
	TableName        string
	PrimaryKey       string
	TTLAttribute     string
	SortKeyAttribute string
	ValueStoreMode   ValueStoreMode
	ValueAttribute   string
}
