package dynamo

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type ValueStoreMode string

const (
	ValueStoreModeJson       ValueStoreMode = "json"
	ValueStoreModeAttributes ValueStoreMode = "attributes"
)

type DynamoDB struct {
	client                *dynamodb.Client
	tableName             string
	partitionKeyAttribute string
	ttlAttribute          string
	sortKeyAttribute      string
	valueStoreMode        ValueStoreMode
	valueAttribute        string
}

type DbOptions struct {
	Region                string
	TableName             string
	PartitionKeyAttribute string
	TtlAttribute          string
	SortKeyAttribute      string
	ValueStoreMode        ValueStoreMode
	ValueAttribute        string
}

type GetOptions struct {
	SortKey string
	Result  any
}

type SetOptions struct {
	Expiration time.Duration
	SortKey    string
}

type QueryCondition string

const (
	QueryConditionNone                 QueryCondition = "none"
	QueryConditionBeginsWith           QueryCondition = "beginsWith"
	QueryConditionEndsWith             QueryCondition = "endsWith"
	QueryConditionContains             QueryCondition = "contains"
	QueryConditionEquals               QueryCondition = "equals"
	QueryConditionNotEquals            QueryCondition = "notEquals"
	QueryConditionGreaterThan          QueryCondition = "greaterThan"
	QueryConditionLessThan             QueryCondition = "lessThan"
	QueryConditionGreaterThanOrEqualTo QueryCondition = "greaterThanOrEqualTo"
	QueryConditionLessThanOrEqualTo    QueryCondition = "lessThanOrEqualTo"
)

type QueryOptions struct {
	Result                any
	SortKey               string
	PartitionKeyCondition QueryCondition
	SortKeyCondition      QueryCondition
}
