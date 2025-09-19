package dynamo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/finch-technologies/go-utils/adapters"
	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/utils"
)

var tableMap map[string]*DynamoDB = make(map[string]*DynamoDB)

func New(options ...DbOptions) (*DynamoDB, error) {

	opts := getOptions(options...)

	if opts.TableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	client, err := adapters.GetDynamoClient(opts.Region)

	if err != nil {
		return nil, fmt.Errorf("failed to get dynamodb client: %w", err)
	}

	d := &DynamoDB{
		client:                client,
		tableName:             opts.TableName,
		partitionKeyAttribute: opts.PartitionKeyAttribute,
		ttlAttribute:          opts.TtlAttribute,
		sortKeyAttribute:      opts.SortKeyAttribute,
		valueStoreMode:        opts.ValueStoreMode,
		valueAttribute:        opts.ValueAttribute,
	}

	tableMap[opts.TableName] = d

	return d, nil
}

func (d *DynamoDB) Get(key string, options ...GetOptions) (interface{}, error) {

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
		return nil, err
	}

	if result.Item == nil {
		return nil, nil
	}

	//Check expiration time
	var expirationTime int64
	err = attributevalue.Unmarshal(result.Item["expiration_time"], &expirationTime)

	now := time.Now().Unix()

	if err == nil && expirationTime > 0 && now > expirationTime {
		if d.valueStoreMode == ValueStoreModeJson {
			return "", nil
		} else {
			return nil, nil
		}
	}

	if d.valueStoreMode == ValueStoreModeJson {
		// Assuming the data is stored as JSON in DynamoDB
		var resultItem map[string]interface{}
		err = attributevalue.UnmarshalMap(result.Item, &resultItem)
		if err != nil {
			log.Error("Failed to unmarshal DynamoDB item: ", err)
			return nil, err
		}

		return resultItem[d.valueAttribute], nil
	} else {
		err = attributevalue.UnmarshalMap(result.Item, &opts.Result)
		if err != nil {
			log.Error("Failed to unmarshal DynamoDB item: ", err)
			return nil, err
		}
		return opts.Result, nil
	}
}

func (d *DynamoDB) Query(key string, options ...QueryOptions) ([]interface{}, error) {
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
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKey}
		case QueryConditionBeginsWith:
			keyConditionExpression += " AND begins_with(#sk, :sk)"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKey}
		case QueryConditionGreaterThan:
			keyConditionExpression += " AND #sk > :sk"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKey}
		case QueryConditionLessThan:
			keyConditionExpression += " AND #sk < :sk"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKey}
		case QueryConditionGreaterThanOrEqualTo:
			keyConditionExpression += " AND #sk >= :sk"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKey}
		case QueryConditionLessThanOrEqualTo:
			keyConditionExpression += " AND #sk <= :sk"
			expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: opts.SortKey}
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

	var items []interface{}

	for _, item := range result.Items {
		// Check expiration time
		var expirationTime int64
		err = attributevalue.Unmarshal(item[d.ttlAttribute], &expirationTime)
		if err == nil && expirationTime > 0 && now > expirationTime {
			continue // Skip expired items
		}

		if d.valueStoreMode == ValueStoreModeJson {
			// Handle JSON value store mode
			var resultItem map[string]interface{}
			err = attributevalue.UnmarshalMap(item, &resultItem)
			if err != nil {
				log.Error("Failed to unmarshal DynamoDB item: ", err)
				continue
			}
			items = append(items, resultItem[d.valueAttribute])
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
			items = append(items, resultItem)
		}
	}

	return items, nil
}

func (d *DynamoDB) Update(key string, value any, options ...SetOptions) error {
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

	// Handle TTL if expiration is set
	if opts.Expiration > 0 {
		ttl := time.Now().Add(opts.Expiration).Unix()
		ttlPlaceholder := fmt.Sprintf("#attr%d", counter)
		ttlValuePlaceholder := fmt.Sprintf(":val%d", counter)

		expressionAttributeNames[ttlPlaceholder] = d.ttlAttribute
		expressionAttributeValues[ttlValuePlaceholder] = &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)}
		updateExpressions = append(updateExpressions, fmt.Sprintf("%s = %s", ttlPlaceholder, ttlValuePlaceholder))
	}

	if len(updateExpressions) == 0 {
		return fmt.Errorf("no attributes to update")
	}

	// Build the complete update expression
	updateExpression := "SET " + fmt.Sprintf("%s", updateExpressions[0])
	for i := 1; i < len(updateExpressions); i++ {
		updateExpression += ", " + updateExpressions[i]
	}

	// Create the UpdateItem input
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(d.tableName),
		Key:                      keys,
		UpdateExpression:         aws.String(updateExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		ReturnValues:             types.ReturnValueNone, // Don't return the updated item
	}

	// Execute the update
	_, err = d.client.UpdateItem(context.Background(), input)
	if err != nil {
		return fmt.Errorf("failed to update item in dynamodb: %w", err)
	}

	return nil
}

func (d *DynamoDB) Put(key string, value any, options ...SetOptions) error {

	opts := getSetOptions(options...)

	item := map[string]types.AttributeValue{
		d.partitionKeyAttribute: &types.AttributeValueMemberS{Value: key},
	}

	if d.sortKeyAttribute != "" {
		item[d.sortKeyAttribute] = &types.AttributeValueMemberS{Value: utils.StringOrDefault(opts.SortKey, "null")}
	}

	if d.valueStoreMode == ValueStoreModeJson {
		var payload string

		t := reflect.TypeOf(value).Kind()

		if t == reflect.Struct || t == reflect.Ptr || t == reflect.Interface || t == reflect.Map || t == reflect.Slice || t == reflect.Array {
			bytes, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("failed to marshal payload: %w", err)
			}
			payload = string(bytes)
		} else if t == reflect.String {
			payload = value.(string)
		} else if t == reflect.Int || t == reflect.Int8 || t == reflect.Int16 || t == reflect.Int32 || t == reflect.Int64 {
			payload = strconv.FormatInt(value.(int64), 10)
		} else if t == reflect.Float32 || t == reflect.Float64 {
			payload = strconv.FormatFloat(value.(float64), 'f', -1, 64)
		} else if t == reflect.Bool {
			payload = strconv.FormatBool(value.(bool))
		} else {
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

	if opts.Expiration > 0 {
		ttl := time.Now().Add(opts.Expiration).Unix()
		item[d.ttlAttribute] = &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)}
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

func (d *DynamoDB) Delete(key string) error {
	_, err := d.client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			d.partitionKeyAttribute: &types.AttributeValueMemberS{Value: key},
		},
	},
	)

	if err != nil {
		return fmt.Errorf("failed to delete key from dynamodb: %s", err)
	}

	return nil
}

func Get[T any](tableName string, key string, options ...GetOptions) (T, error) {

	var value T

	table, err := getTable(tableName)

	if err != nil {
		return value, err
	}

	valueInterface, err := table.Get(key, options...)

	if err != nil {
		return value, err
	}

	valueStr := valueInterface.(string)

	if valueStr == "" {
		return value, errors.New("item not found in database")
	}

	err = json.Unmarshal([]byte(valueStr), &value)

	return value, err
}

func GetString(tableName, key string) (string, error) {
	table, err := getTable(tableName)

	if err != nil {
		return "", err
	}

	value, err := table.Get(key)

	if err != nil {
		return "", err
	}

	return value.(string), nil
}

func GetInt(tableName, key string) (int, error) {
	table, err := getTable(tableName)

	if err != nil {
		return 0, err
	}

	str, err := table.Get(key)

	if err != nil {
		return 0, err
	}

	value, err := strconv.Atoi(str.(string))

	if err != nil {
		return 0, err
	}

	return value, nil
}

func Put(tableName, key string, value any, options ...SetOptions) error {
	table, err := getTable(tableName)

	if err != nil {
		return err
	}

	table.Put(key, value, options...)

	return nil
}

func Delete(tableName, key string) error {
	table, err := getTable(tableName)

	if err != nil {
		return err
	}

	return table.Delete(key)
}
