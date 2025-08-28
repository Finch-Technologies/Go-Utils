package dynamodb

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/finch-technologies/go-utils/adapters"
	"github.com/finch-technologies/go-utils/config/database"
	"github.com/finch-technologies/go-utils/config/environment"
	"github.com/google/uuid"
)

type DynamoDBLogDriver struct {
	db        *dynamodb.DynamoDB
	tableName string
}

func New(dbName database.Name) *DynamoDBLogDriver {
	d := &DynamoDBLogDriver{
		db:        adapters.GetDynamoClient(),
		tableName: getTableName(string(dbName)),
	}

	return d
}

func (d *DynamoDBLogDriver) Write(p []byte) (n int, err error) {
	input := &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item: map[string]*dynamodb.AttributeValue{
			"timestamp": {
				N: aws.String(strconv.FormatInt(time.Now().Unix(), 10)),
			},
			"unique_id": {
				S: aws.String(uuid.New().String()),
			},
			"event": {
				S: aws.String(string(p)),
			},
			"expiration_time": {
				N: aws.String(strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 10)),
			},
		},
	}

	_, err = d.db.PutItem(input)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (d *DynamoDBLogDriver) FetchListBatch(listName string, count int64) ([]string, error) {
	result, err := d.scanReturn(count)
	if err != nil {
		return nil, err
	}

	var eventsJSON []string
	for _, item := range result.Items {
		eventsJSON = append(eventsJSON, *item["event"].S)
	}

	return eventsJSON, nil
}

func (d *DynamoDBLogDriver) scanReturn(count int64) (*dynamodb.ScanOutput, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(d.tableName),
		Limit:     aws.Int64(count),
	}

	result, err := d.db.Scan(input)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (d *DynamoDBLogDriver) DeleteListBatch(listName string, count int64) error {
	scan, _ := d.scanReturn(count)

	writeRequests := make([]*dynamodb.WriteRequest, 0, len(scan.Items))

	for _, item := range scan.Items {
		writeRequests = append(writeRequests, &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String(*item["id"].S),
					},
				},
			},
		})

		// DynamoDB BatchWriteItem allows a maximum of 25 items per request
		if len(writeRequests) == 25 {
			d.batchDelete(writeRequests)
			writeRequests = writeRequests[:0] // Reset slice
		}
	}

	// Delete remaining items
	if len(writeRequests) > 0 {
		d.batchDelete(writeRequests)
	}

	return nil
}

func (d *DynamoDBLogDriver) batchDelete(writeRequests []*dynamodb.WriteRequest) error {
	_, err := d.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			d.tableName: writeRequests,
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func getTableName(suffix string) string {
	env := environment.GetEnvironment()

	if env == "" {
		env = environment.Local
	}

	return fmt.Sprintf("shrike.%s.%s", env, suffix)
}
