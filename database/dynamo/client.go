package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var dynamodbClient *dynamodb.Client

func GetDynamoClient(region string) (*dynamodb.Client, error) {
	if dynamodbClient == nil {
		awsConfig, err := config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}

		dynamodbClient = dynamodb.NewFromConfig(awsConfig)
	}

	return dynamodbClient, nil
}
