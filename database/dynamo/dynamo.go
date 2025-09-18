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
)

type DynamoDB struct {
	client           *dynamodb.Client
	tableName        string
	primaryKey       string
	ttlAttribute     string
	sortKeyAttribute string
	valueStoreMode   ValueStoreMode
	valueAttribute   string
}

var tableMap map[string]*DynamoDB = make(map[string]*DynamoDB)

func getOptions(options ...DbOptions) DbOptions {
	if len(options) > 0 {
		return options[0]
	}
	return DbOptions{
		PrimaryKey:       "id",
		TTLAttribute:     "expiration_time",
		SortKeyAttribute: "group_id",
		ValueStoreMode:   ValueStoreModeString,
		ValueAttribute:   "value",
	}
}

func New(options ...DbOptions) (*DynamoDB, error) {

	opts := getOptions(options...)

	if opts.TableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	client, err := adapters.GetDynamoClient()

	if err != nil {
		return nil, fmt.Errorf("failed to get dynamodb client: %w", err)
	}

	d := &DynamoDB{
		client:           client,
		tableName:        opts.TableName,
		primaryKey:       opts.PrimaryKey,
		ttlAttribute:     opts.TTLAttribute,
		sortKeyAttribute: opts.SortKeyAttribute,
		valueStoreMode:   opts.ValueStoreMode,
		valueAttribute:   opts.ValueAttribute,
	}

	tableMap[opts.TableName] = d

	return d, nil
}

func getTable(tableName string) (*DynamoDB, error) {

	table := tableMap[tableName]

	if table == nil {
		return nil, fmt.Errorf("table not found")
	}

	return table, nil
}

func (d *DynamoDB) GetString(key string) (string, error) {
	result, err := d.client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			d.primaryKey: &types.AttributeValueMemberS{Value: key},
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
	err = attributevalue.Unmarshal(result.Item[d.ttlAttribute], &expirationTime)

	now := time.Now().Unix()

	if err == nil && expirationTime > 0 && now > expirationTime {
		return "", nil
	}

	var value string
	err = attributevalue.Unmarshal(result.Item[d.valueAttribute], &value)

	if err != nil {
		return "", fmt.Errorf("failed to unmarshal value from dynamodb: %s", err)
	}

	return value, nil
}

func (d *DynamoDB) Get(key string) ([]byte, error) {
	result, err := d.client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			d.primaryKey: &types.AttributeValueMemberS{Value: key},
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
	err = attributevalue.Unmarshal(result.Item["expiration_time"], &expirationTime)

	now := time.Now().Unix()

	if err == nil && expirationTime > 0 && now > expirationTime {
		return nil, nil
	}

	// Assuming the data is stored as JSON in DynamoDB
	var value map[string]interface{}
	err = attributevalue.UnmarshalMap(result.Item, &value)
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

func (d *DynamoDB) Set(key string, value any, expiration time.Duration) error {

	//log.Debug("DynamoDB: Writing to table ", d.tableName, " with key: ", key, " and value: ", value)

	payload := value

	t := reflect.TypeOf(value).Kind()

	if t == reflect.Struct || t == reflect.Interface || t == reflect.Map || t == reflect.Slice || t == reflect.Array {
		bytes, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %w", err)
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

	dynamoItem, err := attributevalue.MarshalMap(item)

	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	_, err = d.client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      dynamoItem,
	})

	if err != nil {
		return fmt.Errorf("failed to write value to dynamodb: %w", err)
	}

	return nil
}

func (d *DynamoDB) SetWithSortKey(pk string, sk string, value any, expiration time.Duration) error {

	payload := value

	t := reflect.TypeOf(value).Kind()

	if t == reflect.Struct || t == reflect.Interface || t == reflect.Map || t == reflect.Slice || t == reflect.Array {
		bytes, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %w", err)
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

	av, err := attributevalue.MarshalMap(item)

	if err != nil {
		log.Error("Failed to marshal item", err)
	}

	_, err = d.client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      av,
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
			d.primaryKey: &types.AttributeValueMemberS{Value: key},
		},
	},
	)

	if err != nil {
		return fmt.Errorf("failed to delete key from dynamodb: %s", err)
	}

	return nil
}

func (d *DynamoDB) GetListWithPrefix(id string, skPrefix string, limit int64) ([]string, error) {
	var values []string
	now := time.Now().Unix()

	input := &dynamodb.QueryInput{
		TableName:              aws.String(d.tableName),
		KeyConditionExpression: aws.String("#id = :id AND begins_with(#sk, :prefix)"),
		ExpressionAttributeNames: map[string]string{
			"#id": d.primaryKey,
			"#sk": d.sortKeyAttribute,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id":     &types.AttributeValueMemberS{Value: id},
			":prefix": &types.AttributeValueMemberS{Value: skPrefix},
		},
	}

	result, err := d.client.Query(context.Background(), input)

	if err != nil {
		return nil, fmt.Errorf("failed to query dynamodb: %w", err)
	}

	for _, item := range result.Items {
		var expirationTime int64
		_ = attributevalue.Unmarshal(item[d.ttlAttribute], &expirationTime)

		if expirationTime > 0 && now > expirationTime {
			continue
		}

		var value string
		err := attributevalue.Unmarshal(item[d.valueAttribute], &value)
		if err == nil {
			values = append(values, value)
		}
	}

	return values, nil
}

func Get[T any](tableName string, key string) (T, error) {

	var value T

	table, err := getTable(tableName)

	if err != nil {
		return value, err
	}

	valueStr, err := table.GetString(key)

	if err != nil {
		return value, err
	}

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

	return table.GetString(key)
}

func GetInt(tableName, key string) (int, error) {
	table, err := getTable(tableName)

	if err != nil {
		return 0, err
	}

	str, err := table.GetString(key)

	if err != nil {
		return 0, err
	}

	value, err := strconv.Atoi(str)

	if err != nil {
		return 0, err
	}

	return value, nil
}

func Set(tableName, key string, value any, expiration time.Duration) error {
	table, err := getTable(tableName)

	if err != nil {
		return err
	}

	table.Set(key, value, expiration)

	return nil
}

func Delete(tableName, key string) error {
	table, err := getTable(tableName)

	if err != nil {
		return err
	}

	return table.Delete(key)
}
