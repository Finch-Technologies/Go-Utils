package dynamodb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/finch-technologies/go-utils/adapters"
	"github.com/finch-technologies/go-utils/database/types"
	"github.com/finch-technologies/go-utils/log"
)

type DynamoDB struct {
	db               *dynamodb.DynamoDB
	tableName        string
	primaryKey       string
	ttlAttribute     string
	sortKeyAttribute string
	valueStoreMode   types.ValueStoreMode
	valueAttribute   string
}

func getOptions(options ...types.DbOptions) types.DbOptions {
	if len(options) > 0 {
		return options[0]
	}
	return types.DbOptions{
		PrimaryKey:       "id",
		TTLAttribute:     "expiration_time",
		SortKeyAttribute: "group_id",
		ValueStoreMode:   types.ValueStoreModeString,
		ValueAttribute:   "value",
	}
}

func New(options ...types.DbOptions) (*DynamoDB, error) {

	opts := getOptions(options...)

	if opts.TableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	d := &DynamoDB{
		db:               adapters.GetDynamoClient(),
		tableName:        opts.TableName,
		primaryKey:       opts.PrimaryKey,
		ttlAttribute:     opts.TTLAttribute,
		sortKeyAttribute: opts.SortKeyAttribute,
		valueStoreMode:   opts.ValueStoreMode,
		valueAttribute:   opts.ValueAttribute,
	}

	return d, nil
}

func (d *DynamoDB) GetString(key string) (string, error) {
	result, err := d.db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			d.primaryKey: {
				S: aws.String(key),
			},
		},
	})

	if err != nil {
		return "", fmt.Errorf("failed to get item from dynamodb: %s", err)
	}

	if result == nil {
		return "", nil
	}

	//Check expiration time
	var expirationTime int64
	err = dynamodbattribute.Unmarshal(result.Item["expiration_time"], &expirationTime)

	now := time.Now().Unix()

	if err == nil && expirationTime > 0 && now > expirationTime {
		return "", nil
	}

	var value string
	err = dynamodbattribute.Unmarshal(result.Item["value"], &value)

	if err != nil {
		return "", fmt.Errorf("failed to unmarshal value from dynamodb: %s", err)
	}

	return value, nil
}

func (d *DynamoDB) Get(key string) ([]byte, error) {
	result, err := d.db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			d.primaryKey: {
				S: aws.String(key),
			},
		},
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
	err = dynamodbattribute.Unmarshal(result.Item["expiration_time"], &expirationTime)

	now := time.Now().Unix()

	if err == nil && expirationTime > 0 && now > expirationTime {
		return nil, nil
	}

	// Assuming the data is stored as JSON in DynamoDB
	var value map[string]interface{}
	err = dynamodbattribute.UnmarshalMap(result.Item, &value)
	if err != nil {
		log.Error("Failed to unmarshal DynamoDB item: ", err)
		return nil, err
	}

	// Serialize the map to JSON bytes
	valueJSON, err := json.Marshal(value)
	if err != nil {
		log.Error("Failed to marshal value to JSON: ", err)
		return nil, err
	}

	return valueJSON, nil
}

func (d *DynamoDB) Set(key string, value any, expiration time.Duration) {

	//log.Debug("DynamoDB: Writing to table ", d.tableName, " with key: ", key, " and value: ", value)

	payload := value

	t := reflect.TypeOf(value).Kind()

	if t == reflect.Struct || t == reflect.Interface || t == reflect.Map || t == reflect.Slice || t == reflect.Array {
		bytes, err := json.Marshal(value)
		if err != nil {
			log.Error("Failed to marshal payload", err)
			return
		}
		payload = string(bytes)
	}

	item := map[string]interface{}{
		d.primaryKey:     key,
		d.valueAttribute: payload,
	}

	if expiration > 0 {
		item[d.ttlAttribute] = time.Now().Add(expiration).Unix()
	}

	av, err := dynamodbattribute.MarshalMap(item)

	if err != nil {
		log.Error("Failed to marshal item", err)
	}

	_, err = d.db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      av,
	})

	if err != nil {
		log.Error("Failed to write value to dynamodb", err)
	}
}

func (d *DynamoDB) SetWithSortKey(pk string, sk string, value any, expiration time.Duration) {

	payload := value

	t := reflect.TypeOf(value).Kind()

	if t == reflect.Struct || t == reflect.Interface || t == reflect.Map || t == reflect.Slice || t == reflect.Array {
		bytes, err := json.Marshal(value)
		if err != nil {
			log.Error("Failed to marshal payload", err)
			return
		}
		payload = string(bytes)
	}

	item := map[string]interface{}{
		d.primaryKey:       pk,
		d.sortKeyAttribute: sk,
		d.valueAttribute:   payload,
	}

	if expiration > 0 {
		item[d.ttlAttribute] = time.Now().Add(expiration).Unix()
	}

	av, err := dynamodbattribute.MarshalMap(item)

	if err != nil {
		log.Error("Failed to marshal item", err)
	}

	_, err = d.db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      av,
	})

	if err != nil {
		log.Error("Failed to write value to dynamodb", err)
	}
}

func (d *DynamoDB) Delete(key string) error {
	_, err := d.db.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			d.primaryKey: {
				S: aws.String(key),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to delete key from dynamodb: %s", err)
	}

	return nil
}

func (d *DynamoDB) GetListWithPrefix(prefix string, limit int64) ([]string, error) {
	var values []string
	now := time.Now().Unix()

	input := &dynamodb.QueryInput{
		TableName:              aws.String(d.tableName),
		KeyConditionExpression: aws.String("#id = :id AND begins_with(#group_id, :prefix)"),
		ExpressionAttributeNames: map[string]*string{
			"#id":       aws.String("id"),
			"#group_id": aws.String("group_id"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":id":     {S: aws.String("browserInfo")},
			":prefix": {S: aws.String(prefix)},
		},
		Limit: aws.Int64(limit),
	}

	result, err := d.db.Query(input)
	if err != nil {
		return nil, fmt.Errorf("failed to query dynamodb: %w", err)
	}

	for _, item := range result.Items {
		var expirationTime int64
		_ = dynamodbattribute.Unmarshal(item["expiration_time"], &expirationTime)

		if expirationTime > 0 && now > expirationTime {
			continue
		}

		var value string
		err := dynamodbattribute.Unmarshal(item["value"], &value)
		if err == nil {
			values = append(values, value)
		}
	}

	return values, nil
}
