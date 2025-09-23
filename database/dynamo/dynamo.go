package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/utils"
)

// tableMap stores DynamoDB table instances by table name for reuse across the application
var tableMap map[string]*DynamoDB = make(map[string]*DynamoDB)

// New creates a new DynamoDB instance with the provided configuration options.
// It initializes the AWS DynamoDB client and stores the instance in the global tableMap for reuse.
//
// Parameters:
//   - options: Variable number of DbOptions to configure the DynamoDB instance
//
// Returns:
//   - *DynamoDB: Configured DynamoDB instance
//   - error: Error if table name is missing or client creation fails
//
// Example:
//
//	db, err := New(DbOptions{
//	    TableName:        "my-table",
//	    Region:           "us-east-1",
//	    ValueStoreMode:   ValueStoreModeAttributes,
//	    SortKeyAttribute: "sk",
//	})
func New(options ...DbOptions) (*DynamoDB, error) {

	opts := getOptions(options...)

	if opts.TableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	client, err := GetDynamoClient(opts.Region)

	if err != nil {
		return nil, fmt.Errorf("failed to get dynamodb client: %w", err)
	}

	err = ensureTableExists(context.Background(), client, opts.TableName)

	if err != nil {
		return nil, err
	}

	d := &DynamoDB{
		client:                client,
		tableName:             opts.TableName,
		partitionKeyAttribute: opts.PartitionKeyAttribute,
		ttlAttribute:          opts.TtlAttribute,
		sortKeyAttribute:      opts.SortKeyAttribute,
		valueStoreMode:        opts.ValueStoreMode,
		valueAttribute:        opts.ValueAttribute,
		ttl:                   opts.Ttl,
	}

	tableMap[opts.TableName] = d

	return d, nil
}

// Get retrieves a single item from DynamoDB using the partition key and optional sort key.
// The function supports TTL (Time To Live) checking and returns nil for expired items.
// It supports both JSON and attribute value store modes.
//
// Parameters:
//   - key: The partition key value to retrieve
//   - options: Optional GetOptions containing sort key and result type information
//
// Returns:
//   - any: The retrieved item (type depends on value store mode)
//   - *time.Time: The expiration time of the item
//   - error: Error if retrieval fails
//
// Behavior:
//   - Returns nil if item doesn't exist
//   - Returns empty string (JSON mode) or nil (attribute mode) for expired items
//   - JSON mode: Returns the value from the configured value attribute
//   - Attribute mode: Returns the entire item unmarshaled into the provided result type
//
// Example:
//
//	// Simple get
//	item, expiry, err := db.Get("user123")
//
//	// Get with sort key and typed result
//	result, expiry, err := db.Get("user123", GetOptions{
//	    SortKey: "profile",
//	    Result:  &Person{},
//	})
func (d *DynamoDB) Get(key string, options ...GetOptions) (any, *time.Time, error) {

	opts := getGetOptions(options...)

	keys := map[string]types.AttributeValue{
		d.partitionKeyAttribute: &types.AttributeValueMemberS{Value: key},
	}

	if d.sortKeyAttribute != "" {
		keys[d.sortKeyAttribute] = &types.AttributeValueMemberS{Value: utils.StringOrDefault(opts.SortKey, "null")}
	}

	result, err := d.client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key:       keys,
	})

	if err != nil {
		log.Error("Failed to get item from DynamoDB: ", err)
		return nil, nil, err
	}

	if result.Item == nil {
		return nil, nil, nil
	}

	//Check expiration time
	var expirationTimestamp int64
	err = attributevalue.Unmarshal(result.Item[d.ttlAttribute], &expirationTimestamp)

	now := time.Now().Unix()

	var expirationTime *time.Time

	if err == nil && expirationTimestamp > 0 {
		unixTime := time.Unix(expirationTimestamp, 0)
		expirationTime = &unixTime

		if now > expirationTimestamp {
			if d.valueStoreMode == ValueStoreModeJson {
				return "", expirationTime, nil
			} else {
				return nil, expirationTime, nil
			}
		}
	}

	if d.valueStoreMode == ValueStoreModeJson {
		// Assuming the data is stored as JSON in DynamoDB
		var resultItem map[string]interface{}
		err = attributevalue.UnmarshalMap(result.Item, &resultItem)
		if err != nil {
			log.Error("Failed to unmarshal DynamoDB item: ", err)
			return nil, nil, err
		}
		value := resultItem[d.valueAttribute]
		return value, expirationTime, nil
	} else {
		err = attributevalue.UnmarshalMap(result.Item, &opts.Result)
		if err != nil {
			log.Error("Failed to unmarshal DynamoDB item: ", err)
			return nil, nil, err
		}
		return opts.Result, expirationTime, nil
	}
}

// Query retrieves multiple items from DynamoDB using the partition key and optional sort key conditions.
// It performs efficient queries using DynamoDB's Query operation (not Scan) and supports various
// sort key conditions like begins_with, equals, greater_than, etc.
//
// Parameters:
//   - key: The partition key value to query
//   - options: Optional QueryOptions containing sort key conditions and result type information
//
// Returns:
//   - []QueryResult[any]: Slice of retrieved items (type depends on value store mode)
//   - error: Error if query fails
//
// Features:
//   - Automatic TTL filtering (expired items are excluded)
//   - Support for multiple sort key conditions (begins_with, equals, comparisons)
//   - Both JSON and attribute value store modes supported
//   - Efficient DynamoDB Query operation (not table scan)
//   - Configurable result limits
//
// Sort Key Conditions:
//   - QueryConditionEquals: Exact match
//   - QueryConditionBeginsWith: Prefix match
//   - QueryConditionGreaterThan/LessThan: Comparison operators
//   - QueryConditionGreaterThanOrEqualTo/LessThanOrEqualTo: Inclusive comparisons
//
// Example:
//
//	// Query all items for a partition key
//	items, err := db.Query("user123")
//
//	// Query with sort key condition
//	sessions, err := db.Query("user123", QueryOptions{
//	    SortKeyCondition: QueryConditionBeginsWith,
//	    SortKey:          "session_",
//	})
//
//	// Query with typed result for attribute mode
//	people, err := db.Query("company1", QueryOptions{
//	    Result: &Person{},
//	})
func (d *DynamoDB) Query(key string, options ...QueryOptions) ([]QueryResult[any], error) {
	opts := getQueryOptions(options...)
	now := time.Now().Unix()

	// Build key condition expression
	keyConditionExpression := "#pk = :pk"
	expressionAttributeNames := map[string]string{
		"#pk": d.partitionKeyAttribute,
	}
	expressionAttributeValues := map[string]types.AttributeValue{
		":pk": &types.AttributeValueMemberS{Value: key},
	}

	// Add sort key condition if specified
	if d.sortKeyAttribute != "" && opts.SortKeyCondition != QueryConditionNone {
		expressionAttributeNames["#sk"] = d.sortKeyAttribute

		switch opts.SortKeyCondition {
		case QueryConditionEquals:
			keyConditionExpression += " AND #sk = :sk"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKeyValue}
		case QueryConditionBeginsWith:
			keyConditionExpression += " AND begins_with(#sk, :sk)"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKeyValue}
		case QueryConditionGreaterThan:
			keyConditionExpression += " AND #sk > :sk"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKeyValue}
		case QueryConditionLessThan:
			keyConditionExpression += " AND #sk < :sk"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKeyValue}
		case QueryConditionGreaterThanOrEqualTo:
			keyConditionExpression += " AND #sk >= :sk"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKeyValue}
		case QueryConditionLessThanOrEqualTo:
			keyConditionExpression += " AND #sk <= :sk"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKeyValue}
		default:
			return nil, fmt.Errorf("unsupported sort key condition: %s", opts.SortKeyCondition)
		}
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(d.tableName),
		KeyConditionExpression:    aws.String(keyConditionExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	if opts.Limit > 0 {
		input.Limit = aws.Int32(int32(opts.Limit))
	}

	result, err := d.client.Query(context.Background(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to query dynamodb: %w", err)
	}

	var items []QueryResult[any]

	for _, item := range result.Items {
		// Check expiration time
		var expirationTime int64
		err = attributevalue.Unmarshal(item[d.ttlAttribute], &expirationTime)
		if err == nil && expirationTime > 0 && now > expirationTime {
			continue // Skip expired items
		}

		expiryTime := time.Unix(expirationTime, 0)

		sortKey := ""
		if d.sortKeyAttribute != "" {
			sortKey = item[d.sortKeyAttribute].(*types.AttributeValueMemberS).Value
		}

		if d.valueStoreMode == ValueStoreModeJson {
			// Handle JSON value store mode
			var resultItem map[string]interface{}
			err = attributevalue.UnmarshalMap(item, &resultItem)
			if err != nil {
				log.Error("Failed to unmarshal DynamoDB item: ", err)
				continue
			}

			value := resultItem[d.valueAttribute]

			items = append(items, QueryResult[any]{
				Value:   value,
				Expiry:  &expiryTime,
				SortKey: sortKey,
			})
		} else {
			// Handle attribute value store mode
			var resultItem interface{}
			if opts.Result != nil {
				// Create a new instance of the same type as opts.Result
				resultType := reflect.TypeOf(opts.Result)
				if resultType.Kind() == reflect.Ptr {
					resultItem = reflect.New(resultType.Elem()).Interface()
				} else {
					resultItem = reflect.New(resultType).Interface()
				}
			} else {
				resultItem = make(map[string]interface{})
			}

			err = attributevalue.UnmarshalMap(item, resultItem)
			if err != nil {
				log.Error("Failed to unmarshal DynamoDB item: ", err)
				continue
			}

			items = append(items, QueryResult[any]{
				Value:   resultItem,
				Expiry:  &expiryTime,
				SortKey: sortKey,
			})
		}
	}

	return items, nil
}

// Update performs partial updates to existing DynamoDB items using the efficient UpdateItem operation.
// It only updates the fields provided in the value parameter, leaving other fields unchanged.
// The function uses DynamoDB's UpdateExpression with SET operations for optimal performance.
//
// Parameters:
//   - key: The partition key of the item to update
//   - value: Struct containing the fields to update (uses dynamodbav tags for field mapping)
//   - options: Optional SetOptions containing sort key and TTL information
//
// Returns:
//   - error: Error if update fails
//
// Features:
//   - Partial updates: Only modifies specified fields
//   - Automatic key protection: Partition/sort keys cannot be updated
//   - TTL support: Can set/update expiration times
//   - Type safety: Uses struct tags for field mapping
//   - Efficient: Uses DynamoDB's native UpdateItem operation
//   - Upsert behavior: Creates item if it doesn't exist (DynamoDB default behavior)
//
// Field Mapping:
//
//	The function uses Go struct tags to map fields to DynamoDB attributes:
//	- `dynamodbav:"field_name"` - Maps struct field to DynamoDB attribute
//	- Fields without tags use the struct field name
//	- Partition/sort key fields are automatically skipped
//
// Example:
//
//	// Partial update - only email field
//	update := struct {
//	    Email string `dynamodbav:"email"`
//	}{
//	    Email: "new@email.com",
//	}
//	err := db.Update("user123", update, SetOptions{SortKey: "profile"})
//
//	// Multiple field update with TTL
//	person := Person{
//	    Name:  "New Name",
//	    Email: "new@email.com",
//	}
//	err := db.Update("user123", person, SetOptions{
//	    SortKey: "profile",
//	    Ttl:     1 * time.Hour,
//	})
func (d *DynamoDB) Update(key string, value any, options ...PutOptions) error {
	opts := getSetOptions(options...)

	// Build key for the item to update
	keys := map[string]types.AttributeValue{
		d.partitionKeyAttribute: &types.AttributeValueMemberS{Value: key},
	}

	if d.sortKeyAttribute != "" {
		keys[d.sortKeyAttribute] = &types.AttributeValueMemberS{Value: utils.StringOrDefault(opts.SortKey, "null")}
	}

	// Marshal the update value to get attribute values
	updateValues, err := attributevalue.MarshalMap(value)
	if err != nil {
		return fmt.Errorf("failed to marshal update value: %w", err)
	}

	// Build update expression components
	var updateExpressions []string
	expressionAttributeNames := make(map[string]string)
	expressionAttributeValues := make(map[string]types.AttributeValue)

	// Counter for placeholder names to avoid conflicts
	counter := 0

	for attrName, attrValue := range updateValues {
		// Skip key attributes - can't update them
		if attrName == d.partitionKeyAttribute || attrName == d.sortKeyAttribute {
			continue
		}

		// Create placeholders for attribute names and values
		namePlaceholder := fmt.Sprintf("#attr%d", counter)
		valuePlaceholder := fmt.Sprintf(":val%d", counter)

		expressionAttributeNames[namePlaceholder] = attrName
		expressionAttributeValues[valuePlaceholder] = attrValue
		updateExpressions = append(updateExpressions, fmt.Sprintf("%s = %s", namePlaceholder, valuePlaceholder))

		counter++
	}

	ttl := utils.DurationOrDefault(opts.Ttl, d.ttl)

	// Handle TTL if expiration is set
	if ttl > 0 {
		expiryTime := time.Now().Add(ttl).Unix()
		expiryPlaceholder := fmt.Sprintf("#attr%d", counter)
		expiryValuePlaceholder := fmt.Sprintf(":val%d", counter)

		expressionAttributeNames[expiryPlaceholder] = d.ttlAttribute
		expressionAttributeValues[expiryValuePlaceholder] = &types.AttributeValueMemberN{Value: strconv.FormatInt(expiryTime, 10)}
		updateExpressions = append(updateExpressions, fmt.Sprintf("%s = %s", expiryPlaceholder, expiryValuePlaceholder))
	}

	if len(updateExpressions) == 0 {
		return fmt.Errorf("no attributes to update")
	}

	// Build the complete update expression
	updateExpression := "SET " + updateExpressions[0]
	for i := 1; i < len(updateExpressions); i++ {
		updateExpression += ", " + updateExpressions[i]
	}

	// Create the UpdateItem input
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(d.tableName),
		Key:                       keys,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		ReturnValues:              types.ReturnValueNone, // Don't return the updated item
	}

	// Execute the update
	_, err = d.client.UpdateItem(context.Background(), input)
	if err != nil {
		return fmt.Errorf("failed to update item in dynamodb: %w", err)
	}

	return nil
}

// Put stores a complete item in DynamoDB, replacing any existing item with the same key.
// It supports both JSON and attribute value store modes and automatically handles type conversion.
// The function uses DynamoDB's PutItem operation which performs a complete item replacement.
//
// Parameters:
//   - key: The partition key value for the item
//   - value: The data to store (can be any serializable type)
//   - options: Optional SetOptions containing sort key and TTL information
//
// Returns:
//   - error: Error if storage fails
//
// Storage Modes:
//   - JSON Mode: Serializes value to JSON and stores in the configured value attribute
//   - Attribute Mode: Maps struct fields directly to DynamoDB attributes using dynamodbav tags
//
// Type Support (JSON Mode):
//   - Structs, pointers, interfaces, maps, slices, arrays: JSON serialized
//   - Strings: Stored directly
//   - Integers: Converted to string representation
//   - Floats: Converted to string representation
//   - Booleans: Converted to string representation
//
// Features:
//   - Complete item replacement (unlike Update which does partial updates)
//   - Automatic type conversion based on reflection
//   - TTL support for automatic item expiration
//   - Sort key support for composite primary keys
//   - Handles both simple and complex data types
//
// Example:
//
//	// Store a struct (attribute mode)
//	person := Person{Name: "John", Email: "john@example.com"}
//	err := db.Put("user123", person, SetOptions{
//	    SortKey: "profile",
//	    Ttl:     24 * time.Hour,
//	})
//
//	// Store simple string (JSON mode)
//	err := db.Put("cache_key", "cached_value")
//
//	// Store complex data with expiration
//	data := map[string]interface{}{
//	    "settings": map[string]string{"theme": "dark"},
//	    "lastLogin": time.Now(),
//	}
//	err := db.Put("user123", data, SetOptions{
//	    SortKey: "settings",
//	    Ttl:     7 * 24 * time.Hour,
//	})
func (d *DynamoDB) Put(key string, value any, options ...PutOptions) error {

	opts := getSetOptions(options...)

	item := map[string]types.AttributeValue{
		d.partitionKeyAttribute: &types.AttributeValueMemberS{Value: key},
	}

	if d.sortKeyAttribute != "" {
		item[d.sortKeyAttribute] = &types.AttributeValueMemberS{Value: utils.StringOrDefault(opts.SortKey, "null")}
	}

	if value == nil {
		item[d.valueAttribute] = &types.AttributeValueMemberS{Value: ""}
	} else if d.valueStoreMode == ValueStoreModeJson {
		var payload string

		t := reflect.TypeOf(value).Kind()

		switch t {
		case reflect.Struct, reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Array:
			bytes, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("failed to marshal payload: %w", err)
			}
			payload = string(bytes)
		case reflect.String:
			payload = value.(string)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			payload = strconv.FormatInt(reflect.ValueOf(value).Int(), 10)
		case reflect.Float32, reflect.Float64:
			payload = strconv.FormatFloat(reflect.ValueOf(value).Float(), 'f', -1, 64)
		case reflect.Bool:
			payload = strconv.FormatBool(value.(bool))
		default:
			return fmt.Errorf("unsupported type: %v", t)
		}

		item[d.valueAttribute] = &types.AttributeValueMemberS{Value: payload}
	} else {
		payload, err := attributevalue.MarshalMap(value)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %w", err)
		}

		//Merge payload with item
		for k, v := range payload {
			item[k] = v
		}
	}

	ttl := utils.DurationOrDefault(opts.Ttl, d.ttl)

	if ttl > 0 {
		expiryTime := time.Now().Add(ttl).Unix()
		item[d.ttlAttribute] = &types.AttributeValueMemberN{Value: strconv.FormatInt(expiryTime, 10)}
	}

	_, err := d.client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      item,
	})

	if err != nil {
		return fmt.Errorf("failed to write value to dynamodb: %w", err)
	}

	return nil
}

// Delete removes an item from the DynamoDB table by its partition key and optional sort key.
// It uses the DeleteItem operation to permanently remove the item from the table.
//
// Parameters:
//   - key: The partition key value that uniquely identifies the item (or combined with sort key)
//   - sortKey: Optional variadic parameter for the sort key value. If the table has a sort key
//     attribute configured, the first sortKey value will be used. If no sortKey is provided
//     but the table requires one, "null" will be used as the default value.
//
// Returns:
//   - error: Returns an error if the delete operation fails due to network issues,
//     permission problems, or other DynamoDB service errors. Returns nil on success.
//
// Note: This operation will succeed even if the item doesn't exist (DynamoDB doesn't
// return an error for deleting non-existent items).
func (d *DynamoDB) Delete(key string, sortKey ...string) error {

	sk := "null"

	if len(sortKey) > 0 {
		sk = sortKey[0]
	}

	deleteInput := &dynamodb.DeleteItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			d.partitionKeyAttribute: &types.AttributeValueMemberS{Value: key},
		},
	}

	if d.sortKeyAttribute != "" {
		deleteInput.Key[d.sortKeyAttribute] = &types.AttributeValueMemberS{Value: sk}
	}

	_, err := d.client.DeleteItem(context.Background(), deleteInput)

	if err != nil {
		return fmt.Errorf("failed to delete key from dynamodb: %s", err)
	}

	return nil
}

// Get is a generic utility function that retrieves an item from a DynamoDB table and returns it
// as a strongly-typed pointer. This function provides a convenient wrapper around the DynamoDB
// Get operation with automatic type conversion.
//
// Type Parameter:
//   - T: The type to unmarshal the retrieved item into. Must be compatible with the stored data format.
//
// Parameters:
//   - tableName: Name of the DynamoDB table to retrieve from
//   - key: The partition key value that uniquely identifies the item
//   - sortKey: Optional variadic parameter for the sort key value when the table uses composite keys
//
// Returns:
//   - *T: A pointer to the retrieved and unmarshaled item, or nil if the item doesn't exist
//   - *expirationTime: A pointer to the time the item will expire, or nil if the item doesn't have a TTL
//   - error: Returns an error if the retrieval fails, table doesn't exist, or unmarshaling fails
//
// The function automatically handles different storage modes (JSON vs native DynamoDB types)
// and returns nil without error if the requested item doesn't exist in the table.
func Get[T any](tableName string, key string, sortKey ...string) (*T, *time.Time, error) {

	var value T

	table, err := getTable(tableName)

	if err != nil {
		return nil, nil, err
	}

	opts := GetOptions{
		Result: &value,
	}

	//update the options with the result type
	if len(sortKey) > 0 {
		opts.SortKey = sortKey[0]
	}

	result, expiry, err := table.Get(key, opts)

	if err != nil {
		return nil, nil, err
	}

	if result == nil {
		return nil, expiry, nil
	}

	if table.valueStoreMode == ValueStoreModeJson {
		valueStr := result.(string)

		if valueStr == "" {
			return nil, expiry, nil
		}

		err = json.Unmarshal([]byte(valueStr), &value)
	} else {
		value = *result.(*T)
	}

	return &value, expiry, err
}

// Query is a generic utility function that performs a DynamoDB Query operation and returns
// a slice of strongly-typed items. This function provides a convenient wrapper around the
// DynamoDB Query operation with automatic type conversion for multiple results.
//
// Type Parameter:
//   - T: The type to unmarshal each retrieved item into. Must be compatible with the stored data format.
//
// Parameters:
//   - tableName: Name of the DynamoDB table to query
//   - key: The partition key value to query for
//   - options: Optional QueryOptions to specify sort key conditions, filters, limits, etc.
//
// Returns:
//   - []QueryResult[T]: A slice of retrieved and unmarshaled items matching the query criteria
//   - error: Returns an error if the query fails, table doesn't exist, or unmarshaling fails
//
// The function automatically handles type conversion and returns an empty slice if no items
// match the query criteria.
func Query[T any](tableName string, key string, options ...QueryOptions) ([]QueryResult[T], error) {
	table, err := getTable(tableName)

	if err != nil {
		return nil, err
	}

	opts := getQueryOptions(options...)

	var value T

	opts.Result = value

	items, err := table.Query(key, opts)

	if err != nil {
		return nil, err
	}

	var result []QueryResult[T]

	for _, item := range items {
		var resultValue T

		if item.Value != nil {
			// Handle different value storage modes and types
			if table.valueStoreMode == ValueStoreModeJson {
				// In JSON mode, the value is the raw data (string, number, etc.)
				// Try direct type assertion first
				if directValue, ok := item.Value.(T); ok {
					resultValue = directValue
				} else {
					// If direct assertion fails, try JSON unmarshaling for complex types
					if jsonStr, ok := item.Value.(string); ok {
						// For string types, use the string directly
						var zeroValue T
						if reflect.TypeOf(zeroValue).Kind() == reflect.String {
							resultValue = any(jsonStr).(T)
						} else {
							// For complex types, unmarshal from JSON
							if err := json.Unmarshal([]byte(jsonStr), &resultValue); err != nil {
								log.Error("Failed to unmarshal JSON in generic query: ", err)
								continue
							}
						}
					}
				}
			} else {
				// In attribute mode, the value should be a pointer to the type
				if ptrValue, ok := item.Value.(*T); ok {
					resultValue = *ptrValue
				} else {
					// Try direct assertion
					if directValue, ok := item.Value.(T); ok {
						resultValue = directValue
					}
				}
			}
		}

		result = append(result, QueryResult[T]{
			Value:   resultValue,
			Expiry:  item.Expiry,
			SortKey: item.SortKey,
		})
	}

	return result, nil
}

// GetString is a utility function that retrieves a string value from a DynamoDB table.
// This is a convenience function for cases where you know the stored value is a string
// and want to avoid the overhead of generic type parameters.
//
// Parameters:
//   - tableName: Name of the DynamoDB table to retrieve from
//   - key: The partition key value that uniquely identifies the item
//
// Returns:
//   - string: The string value stored in the table, or empty string if not found
//   - *expirationTime: A pointer to the time the item will expire, or nil if the item doesn't have a TTL
//   - error: Returns an error if the retrieval fails or the value cannot be converted to string
func GetString(tableName, key string, sortKey ...string) (string, *time.Time, error) {
	table, err := getTable(tableName)

	if err != nil {
		return "", nil, err
	}

	opts := GetOptions{}

	if len(sortKey) > 0 {
		opts.SortKey = sortKey[0]
	}

	result, expiry, err := table.Get(key, opts)

	if err != nil || result == nil {
		return "", expiry, err
	}

	value := result.(string)

	return value, expiry, nil
}

// GetInt is a utility function that retrieves an integer value from a DynamoDB table.
// This function expects the stored value to be a string representation of an integer
// and automatically converts it using strconv.Atoi.
//
// Parameters:
//   - tableName: Name of the DynamoDB table to retrieve from
//   - key: The partition key value that uniquely identifies the item
//
// Returns:
//   - int: The integer value converted from the stored string, or 0 if not found
//   - *expirationTime: A pointer to the time the item will expire, or nil if the item doesn't have a TTL
//   - error: Returns an error if retrieval fails or the value cannot be converted to integer
func GetInt(tableName, key string) (int, *time.Time, error) {
	table, err := getTable(tableName)

	if err != nil {
		return 0, nil, err
	}

	result, expiry, err := table.Get(key)

	if err != nil || result == nil {
		return 0, expiry, err
	}

	value, err := strconv.Atoi(result.(string))

	if err != nil {
		return 0, expiry, err
	}

	return value, expiry, nil
}

// Put is a utility function that stores an item in a DynamoDB table using the specified key.
// This function provides a convenient wrapper around the DynamoDB Put operation for
// simple key-value storage scenarios.
//
// Parameters:
//   - tableName: Name of the DynamoDB table to store the item in
//   - key: The partition key value that will uniquely identify the stored item
//   - value: The data to store - can be any type that can be marshaled to DynamoDB format
//   - options: Optional SetOptions to configure TTL, sort keys, or other storage settings
//
// Returns:
//   - error: Returns an error if the table doesn't exist or the put operation fails
//
// This function automatically handles the table lookup and delegates to the table's Put method.
func Put(tableName, key string, value any, options ...PutOptions) error {
	table, err := getTable(tableName)

	if err != nil {
		return err
	}

	table.Put(key, value, options...)

	return nil
}

// Delete is a utility function that removes an item from a DynamoDB table by its key.
// This function provides a convenient wrapper around the DynamoDB Delete operation
// for simple deletion scenarios.
//
// Parameters:
//   - tableName: Name of the DynamoDB table to delete the item from
//   - key: The partition key value that uniquely identifies the item to delete
//   - sortKey: Optional variadic parameter for the sort key value when the table uses composite keys
//
// Returns:
//   - error: Returns an error if the table doesn't exist or the delete operation fails
//
// This function will succeed even if the item doesn't exist (DynamoDB doesn't return an error
// for deleting non-existent items). It automatically handles the table lookup and delegates
// to the table's Delete method.
func Delete(tableName, key string, sortKey ...string) error {
	table, err := getTable(tableName)

	if err != nil {
		return err
	}

	return table.Delete(key, sortKey...)
}
