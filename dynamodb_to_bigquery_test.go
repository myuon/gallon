package gallon

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brianvoe/gofakeit/v6"
	"log"
	"testing"
)

func StructToJsonTagMap(data interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	m := map[string]interface{}{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	return m, nil
}

type UserTable struct {
	ID        string `dynamodbav:"id" json:"id"`
	Name      string `dynamodbav:"name" json:"name"`
	Age       int    `dynamodbav:"age" json:"age"`
	CreatedAt int64  `dynamodbav:"created_at" json:"createdAt"`
}

var schema = bigquery.Schema{
	{Name: "id", Type: bigquery.StringFieldType},
	{Name: "name", Type: bigquery.StringFieldType},
	{Name: "age", Type: bigquery.IntegerFieldType},
	{Name: "created_at", Type: bigquery.IntegerFieldType},
}

func NewFakeUserTable() (UserTable, error) {
	v := UserTable{}
	if err := gofakeit.Struct(&v); err != nil {
		return v, err
	}

	return v, nil
}

func InitDynamoDbData(dynamoClient *dynamodb.Client) error {
	tableName := aws.String("users")

	if err := CreateDynamoDbTableIfNotExists(dynamoClient, dynamodb.CreateTableInput{
		TableName: tableName,
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
	}, func() error {
		for i := 0; i < 1000; i++ {
			v, err := NewFakeUserTable()
			if err != nil {
				return err
			}

			if _, err := dynamoClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
				TableName: tableName,
				Item: map[string]types.AttributeValue{
					"id":         &types.AttributeValueMemberS{Value: v.ID},
					"name":       &types.AttributeValueMemberS{Value: v.Name},
					"age":        &types.AttributeValueMemberN{Value: fmt.Sprint(v.Age)},
					"created_at": &types.AttributeValueMemberN{Value: fmt.Sprint(v.CreatedAt)},
				},
			}); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func run() error {
	dynamoClient := NewDynamoDbLocalClient()
	bigqueryClient := NewBigQueryLocalClient()

	if err := InitDynamoDbData(dynamoClient); err != nil {
		return err
	}

	gallon := Gallon{
		Input: NewInputPluginDynamoDb(
			dynamoClient,
			"users",
			func(item map[string]types.AttributeValue) (interface{}, error) {
				user := UserTable{}
				if err := attributevalue.UnmarshalMap(item, &user); err != nil {
					return nil, err
				}

				record, err := StructToJsonTagMap(user)
				if err != nil {
					return nil, err
				}

				return record, nil
			},
		),
		Output: NewOutputPluginBigQuery(
			bigqueryClient,
			"test",
			"users",
			schema,
			func(item interface{}) ([]bigquery.Value, error) {
				values := []bigquery.Value{}
				for _, v := range schema {
					values = append(values, item.(map[string]interface{})[v.Name])
				}

				return values, nil
			},
		),
	}

	if err := gallon.Run(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func Test_run(t *testing.T) {
	if err := run(); err != nil {
		t.Fatal(err)
	}
}
