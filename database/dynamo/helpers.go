package dynamo

import (
	"fmt"
	"os"

	"github.com/finch-technologies/go-utils/utils"
)

func getTable(tableName string) (*DynamoDB, error) {

	table := tableMap[tableName]

	if table == nil {
		return nil, fmt.Errorf("table not found")
	}

	return table, nil
}

func getGetOptions(options ...GetOptions) GetOptions {
	defaultOpts := GetOptions{
		Result: map[string]interface{}{},
	}

	opts := defaultOpts

	if len(options) > 0 {
		opts = options[0]

		utils.MergeObjects(&opts, defaultOpts)
	}

	return opts
}

func getOptions(options ...DbOptions) DbOptions {
	defaultOpts := DbOptions{
		Region:                utils.StringOrDefault(os.Getenv("AWS_REGION"), "af-south-1"),
		PartitionKeyAttribute: "id",
		TtlAttribute:          "expiration_time",
		SortKeyAttribute:      "",
		ValueStoreMode:        ValueStoreModeJson,
		ValueAttribute:        "value",
	}

	opts := defaultOpts

	if len(options) > 0 {
		opts = options[0]
		utils.MergeObjects(&opts, defaultOpts)
	}

	return opts
}

func getQueryOptions(options ...QueryOptions) QueryOptions {
	defaultOpts := QueryOptions{
		Result:                map[string]interface{}{},
		PartitionKeyCondition: QueryConditionEquals,
		SortKeyCondition:      QueryConditionNone,
	}

	opts := defaultOpts

	if len(options) > 0 {
		opts = options[0]
		utils.MergeObjects(&opts, defaultOpts)
	}

	return opts
}

func getSetOptions(options ...SetOptions) SetOptions {
	if len(options) > 0 {
		return options[0]
	}
	return SetOptions{}
}
