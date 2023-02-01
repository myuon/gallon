package gallon

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"net/http"
	"os"
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

		for i := 0; i < 1000; i++ {
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

	client, err := bigquery.NewClient(context.Background(), "test", option.WithEndpoint("http://localhost:9050"))
	if err != nil {
		t.Fatal(err)
	}

	// check dataset exists
	if _, err := client.Dataset("test").Metadata(context.Background()); err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == http.StatusNotFound {
				if err := client.Dataset("test").Create(context.Background(), &bigquery.DatasetMetadata{
					Location: "asia-northeast1",
				}); err != nil {
					t.Fatal(err)
				}

				if err := client.Dataset("test").Table("users").Create(context.Background(), &bigquery.TableMetadata{
					Schema: schema,
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Fatal(err)
			}
		}
	}
	inserter := client.Dataset("test").Table("users").Inserter()

	messages := make(chan interface{}, 1000)

	go func() {
		hasNext := true
		lastEvaluatedKey := map[string]types.AttributeValue(nil)

		for hasNext {
			resp, err := svc.Scan(
				context.TODO(),
				&dynamodb.ScanInput{
					TableName:         aws.String("users"),
					ExclusiveStartKey: lastEvaluatedKey,
					Limit:             aws.Int32(100),
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			if resp.LastEvaluatedKey != nil {
				hasNext = true
				lastEvaluatedKey = resp.LastEvaluatedKey
			} else {
				hasNext = false
			}

			var msgs []interface{}
			for _, item := range resp.Items {
				user := UserTable{}
				if err := attributevalue.UnmarshalMap(item, &user); err != nil {
					t.Fatal(err)
				}

				record, err := StructToJsonTagMap(user)
				if err != nil {
					t.Fatal(err)
				}

				msgs = append(msgs, record)
			}

			messages <- msgs
		}

		close(messages)
	}()

	file, err := os.Create("output.jsonl")
	if err != nil {
		t.Fatal(err)
	}

	defer file.Close()

	for {
		select {
		case msgs, ok := <-messages:
			if !ok {
				return
			}

			msgSlice := msgs.([]interface{})

			saver := []*bigquery.ValuesSaver{}
			for _, msg := range msgSlice {
				saver = append(saver, &bigquery.ValuesSaver{
					Schema:   schema,
					InsertID: uuid.New().String(),
					Row: []bigquery.Value{
						msg.(map[string]interface{})["id"],
						msg.(map[string]interface{})["name"],
						msg.(map[string]interface{})["age"],
						msg.(map[string]interface{})["created_at"],
					},
				})
			}

			if err := inserter.Put(context.Background(), saver); err != nil {
				t.Fatal(err)
			}

			fmt.Println("wrote", len(msgSlice), "items")
		}
	}
}
