package gallon

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brianvoe/gofakeit/v6"
	"testing"
)

type UserTable struct {
	ID        string `dynamodbav:"id"`
	Name      string `dynamodbav:"name"`
	Age       int    `dynamodbav:"age"`
	CreatedAt int64  `dynamodbav:"created_at"`
}

func NewFakeUserTable() (UserTable, error) {
	v := UserTable{}
	if err := gofakeit.Struct(&v); err != nil {
		return v, err
	}

	return v, nil
}

func CreateLocalClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
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

func checkIfTableExists(svc *dynamodb.Client, name string) (bool, error) {
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

func Test_run(t *testing.T) {
	svc := CreateLocalClient()

	// check table exists
	exists, err := checkIfTableExists(svc, "users")
	if err != nil {
		t.Fatal(err)
	}

	if !exists {
		if _, err := svc.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
			TableName: aws.String("users"),
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       types.KeyTypeHash,
				},
			},
			BillingMode: types.BillingModePayPerRequest,
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 100; i++ {
		v, err := NewFakeUserTable()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := svc.PutItem(context.TODO(), &dynamodb.PutItemInput{
			TableName: aws.String("users"),
			Item: map[string]types.AttributeValue{
				"id":         &types.AttributeValueMemberS{Value: v.ID},
				"name":       &types.AttributeValueMemberS{Value: v.Name},
				"age":        &types.AttributeValueMemberN{Value: fmt.Sprint(v.Age)},
				"created_at": &types.AttributeValueMemberN{Value: fmt.Sprint(v.CreatedAt)},
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
}
