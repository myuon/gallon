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
	"github.com/google/uuid"
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

func run() error {
	dynamoClient := NewDynamoDbLocalClient()

	// check table exists
	exists, err := DynamoDbCheckIfTableExists(dynamoClient, "users")
	if err != nil {
		return err
	}

	if !exists {
		if _, err := dynamoClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
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
			return err
		}

		for i := 0; i < 1000; i++ {
			v, err := NewFakeUserTable()
			if err != nil {
				return err
			}

			if _, err := dynamoClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
				TableName: aws.String("users"),
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
	}

	bigqueryClient := NewBigQueryLocalClient()

	exists, err = BigQueryCheckIfTableExists(bigqueryClient.Dataset("test").Table("users"))
	if err != nil {
		return err
	}

	// check dataset exists
	if !exists {
		if err := bigqueryClient.Dataset("test").Create(context.Background(), &bigquery.DatasetMetadata{
			Location: "asia-northeast1",
		}); err != nil {
			return err
		}

		if err := bigqueryClient.Dataset("test").Table("users").Create(context.Background(), &bigquery.TableMetadata{
			Schema: schema,
		}); err != nil {
			return err
		}
	}

	inserter := bigqueryClient.Dataset("test").Table("users").Inserter()

	messages := make(chan interface{}, 1000)

	go func() {
		hasNext := true
		lastEvaluatedKey := map[string]types.AttributeValue(nil)

		for hasNext {
			resp, err := dynamoClient.Scan(
				context.TODO(),
				&dynamodb.ScanInput{
					TableName:         aws.String("users"),
					ExclusiveStartKey: lastEvaluatedKey,
					Limit:             aws.Int32(100),
				},
			)
			if err != nil {
				log.Fatal(err)
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
					log.Fatal(err)
				}

				record, err := StructToJsonTagMap(user)
				if err != nil {
					log.Fatal(err)
				}

				msgs = append(msgs, record)
			}

			messages <- msgs
		}

		close(messages)
	}()

	for {
		select {
		case msgs, ok := <-messages:
			if !ok {
				return nil
			}

			msgSlice := msgs.([]interface{})

			saver := []*bigquery.ValuesSaver{}
			for _, msg := range msgSlice {
				values := []bigquery.Value{}
				for _, v := range schema {
					values = append(values, msg.(map[string]interface{})[v.Name])
				}

				saver = append(saver, &bigquery.ValuesSaver{
					Schema:   schema,
					InsertID: uuid.New().String(),
					Row:      values,
				})
			}

			if err := inserter.Put(context.Background(), saver); err != nil {
				return err
			}

			fmt.Println("wrote", len(msgSlice), "items")
		}
	}
}

func Test_run(t *testing.T) {
	if err := run(); err != nil {
		t.Fatal(err)
	}
}
