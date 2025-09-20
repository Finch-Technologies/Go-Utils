package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// dynamodbClient holds a singleton instance of the DynamoDB client to avoid
// creating multiple clients for the same region
var dynamodbClient *dynamodb.Client

// GetDynamoClient returns a singleton DynamoDB client for the specified AWS region.
// This function implements lazy initialization and reuses the same client instance
// for subsequent calls to improve performance and resource utilization.
//
// Parameters:
//   - region: The AWS region identifier (e.g., "us-east-1", "af-south-1") where the
//     DynamoDB service should be accessed
//
// Returns:
//   - *dynamodb.Client: A configured DynamoDB client ready for use
//   - error: Returns an error if the AWS configuration cannot be loaded or the client
//     cannot be created
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
