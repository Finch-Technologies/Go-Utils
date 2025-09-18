package dynamo

type ValueStoreMode string

const (
	ValueStoreModeJson       ValueStoreMode = "json"
	ValueStoreModeAttributes ValueStoreMode = "attributes"
)

type DbOptions struct {
	Region                string
	TableName             string
	PartitionKeyAttribute string
	TtlAttribute          string
	SortKeyAttribute      string
	ValueStoreMode        ValueStoreMode
	ValueAttribute        string
}
