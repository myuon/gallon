package gallon

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewDynamoDbLocalClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("ap-northeast-1"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	if err != nil {
		panic(err)
	}

	return dynamodb.NewFromConfig(cfg)
}

func DynamoDbCheckIfTableExists(svc *dynamodb.Client, name string) (bool, error) {
	_, err := svc.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		if _, ok := err.(*types.ResourceNotFoundException); !ok {
			return false, err
		}
	}

	return true, nil
}

func CreateDynamoDbTableIfNotExists(
	dynamoClient *dynamodb.Client,
	input dynamodb.CreateTableInput,
	onCreate func() error,
) error {
	tableName := *input.TableName

	exists, err := DynamoDbCheckIfTableExists(dynamoClient, tableName)
	if err != nil {
		return err
	}

	if !exists {
		if _, err := dynamoClient.CreateTable(context.TODO(), &input); err != nil {
			return err
		}

		if onCreate != nil {
			if err := onCreate(); err != nil {
				return err
			}
		}
	}

	return nil
}
