package dynamo

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/finch-technologies/go-utils/utils"
)

// getTable retrieves a DynamoDB instance from the global table map by table name.
// This is an internal helper function used by the utility functions to get the appropriate
// table connection for operations.
//
// Parameters:
//   - tableName: The name of the DynamoDB table to retrieve the connection for
//
// Returns:
//   - *DynamoDB: The DynamoDB instance for the specified table
//   - error: Returns an error if the table name is not found in the table map
func getTable(tableName string) (*DynamoDB, error) {

	table := tableMap[tableName]

	if table == nil {
		// If the table name is default and its not found, use the first table in the map
		if tableName == "default" || tableName == "main" {
			for _, table := range tableMap {
				return table, nil
			}
		}
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	return table, nil
}

// getGetOptions processes and merges GetOptions with default values.
// This helper function ensures that all Get operations have proper default configuration
// while allowing callers to override specific options.
//
// Parameters:
//   - options: Variadic GetOptions parameter, only the first option is used if provided
//
// Returns:
//   - GetOptions: A properly configured GetOptions struct with defaults applied
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

// getOptions processes and merges DbOptions with default values for DynamoDB configuration.
// This helper function sets up default DynamoDB connection and table settings while allowing
// callers to override specific configuration options.
//
// Default values include:
//   - Region: Uses AWS_REGION environment variable or defaults to "af-south-1"
//   - PartitionKeyAttribute: "id"
//   - TtlAttribute: "expiration_time"
//   - ValueStoreMode: JSON mode
//   - ValueAttribute: "value"
//
// Parameters:
//   - options: Variadic DbOptions parameter, only the first option is used if provided
//
// Returns:
//   - DbOptions: A properly configured DbOptions struct with defaults applied
func getOptions(options ...DbOptions) DbOptions {
	defaultOpts := DbOptions{
		Region:                utils.StringOrDefault(os.Getenv("AWS_REGION"), "af-south-1"),
		PartitionKeyAttribute: "id",
		TtlAttribute:          "expiration_time",
		SortKeyAttribute:      "",
		ValueStoreMode:        ValueStoreModeJson,
		ValueAttribute:        "value",
		Ttl:                   0,
	}

	opts := defaultOpts

	if len(options) > 0 {
		opts = options[0]
		utils.MergeObjects(&opts, defaultOpts)
	}

	return opts
}

// getQueryOptions processes and merges QueryOptions with default values for DynamoDB Query operations.
// This helper function ensures that all Query operations have proper default configuration
// while allowing callers to override specific query parameters.
//
// Default values include:
//   - Result: Empty map for storing query results
//   - PartitionKeyCondition: Equality condition (QueryConditionEquals)
//   - SortKeyCondition: No sort key condition (QueryConditionNone)
//
// Parameters:
//   - options: Variadic QueryOptions parameter, only the first option is used if provided
//
// Returns:
//   - QueryOptions: A properly configured QueryOptions struct with defaults applied
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

// getSetOptions processes SetOptions for DynamoDB Put and Update operations.
// This helper function returns the provided SetOptions or an empty SetOptions struct
// if none are provided. Unlike other option helpers, this doesn't merge with defaults
// as SetOptions are typically used for optional configurations like TTL.
//
// Parameters:
//   - options: Variadic SetOptions parameter, only the first option is used if provided
//
// Returns:
//   - SetOptions: The provided SetOptions or an empty SetOptions struct
func getSetOptions(options ...PutOptions) PutOptions {
	if len(options) > 0 {
		return options[0]
	}
	return PutOptions{}
}

// ensureTableExists creates the DynamoDB table if it doesn't exist
func ensureTableExists(ctx context.Context, client *dynamodb.Client, tableName string) error {
	// Check if table exists
	_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		// Check if it's a ResourceNotFoundException
		var resourceNotFound *types.ResourceNotFoundException
		if errors.As(err, &resourceNotFound) {
			return fmt.Errorf("table does not exist: %w", err)
		} else {
			return fmt.Errorf("failed to describe table: %w", err)
		}
	}

	return nil
}
