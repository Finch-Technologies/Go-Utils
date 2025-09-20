package dynamo

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// ValueStoreMode defines how values are stored in DynamoDB tables
type ValueStoreMode string

const (
	// ValueStoreModeJson stores values as JSON strings in a single attribute
	ValueStoreModeJson ValueStoreMode = "json"
	// ValueStoreModeAttributes stores values as native DynamoDB attributes (maps, lists, etc.)
	ValueStoreModeAttributes ValueStoreMode = "attributes"
)

// DynamoDB represents a configured DynamoDB table connection with all necessary
// settings for performing operations on a specific table
type DynamoDB struct {
	client                *dynamodb.Client // AWS DynamoDB client instance
	tableName             string           // Name of the DynamoDB table
	partitionKeyAttribute string           // Name of the partition key attribute
	ttlAttribute          string           // Name of the TTL (Time To Live) attribute
	sortKeyAttribute      string           // Name of the sort key attribute (optional)
	valueStoreMode        ValueStoreMode   // How values are stored (JSON vs attributes)
	valueAttribute        string           // Name of the attribute that stores the value
	ttl                   time.Duration    // Default TTL for items
}

// DbOptions contains configuration options for creating a new DynamoDB connection
type DbOptions struct {
	Region                string         // AWS region for the DynamoDB service
	TableName             string         // Name of the DynamoDB table
	PartitionKeyAttribute string         // Name of the partition key attribute
	TtlAttribute          string         // Name of the TTL attribute for automatic item expiration
	SortKeyAttribute      string         // Name of the sort key attribute (optional)
	ValueStoreMode        ValueStoreMode // Storage mode for values (JSON or attributes)
	ValueAttribute        string         // Name of the attribute that stores the value
	Ttl                   time.Duration  // Default TTL for items
}

// GetOptions contains options for DynamoDB Get operations
type GetOptions struct {
	SortKey string // Sort key value for tables with composite keys
	Result  any    // Pointer to struct where the result will be unmarshaled
}

// SetOptions contains options for DynamoDB Put and Update operations
type SetOptions struct {
	Ttl     time.Duration // TTL for the item (overrides default table TTL)
	SortKey string        // Sort key value for tables with composite keys
}

// QueryCondition defines the types of conditions that can be applied to sort keys in DynamoDB queries
type QueryCondition string

const (
	// QueryConditionNone indicates no condition should be applied to the sort key
	QueryConditionNone QueryCondition = "none"
	// QueryConditionBeginsWith checks if the sort key begins with a specific value
	QueryConditionBeginsWith QueryCondition = "beginsWith"
	// QueryConditionEndsWith checks if the sort key ends with a specific value
	QueryConditionEndsWith QueryCondition = "endsWith"
	// QueryConditionContains checks if the sort key contains a specific value
	QueryConditionContains QueryCondition = "contains"
	// QueryConditionEquals checks if the sort key equals a specific value
	QueryConditionEquals QueryCondition = "equals"
	// QueryConditionNotEquals checks if the sort key does not equal a specific value
	QueryConditionNotEquals QueryCondition = "notEquals"
	// QueryConditionGreaterThan checks if the sort key is greater than a specific value
	QueryConditionGreaterThan QueryCondition = "greaterThan"
	// QueryConditionLessThan checks if the sort key is less than a specific value
	QueryConditionLessThan QueryCondition = "lessThan"
	// QueryConditionGreaterThanOrEqualTo checks if the sort key is greater than or equal to a specific value
	QueryConditionGreaterThanOrEqualTo QueryCondition = "greaterThanOrEqualTo"
	// QueryConditionLessThanOrEqualTo checks if the sort key is less than or equal to a specific value
	QueryConditionLessThanOrEqualTo QueryCondition = "lessThanOrEqualTo"
)

// QueryOptions contains options for DynamoDB Query operations
type QueryOptions struct {
	Result                any            // Pointer to struct where the results will be unmarshaled
	SortKey               string         // Sort key value to apply the condition against
	PartitionKeyCondition QueryCondition // Condition to apply to the partition key (usually equals)
	SortKeyCondition      QueryCondition // Condition to apply to the sort key
	Limit                 int            // Maximum number of items to return (0 = no limit)
}
