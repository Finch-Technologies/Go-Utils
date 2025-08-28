package adapters

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var dynamodbClient *dynamodb.DynamoDB

func GetDynamoClient() *dynamodb.DynamoDB {
	if dynamodbClient == nil {
		sess := session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config: aws.Config{
				Region: aws.String(os.Getenv("AWS_REGION")),
			},
		}))
		dynamodbClient = dynamodb.New(sess)
	}

	return dynamodbClient
}
