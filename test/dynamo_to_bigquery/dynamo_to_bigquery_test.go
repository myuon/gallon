package dynamo_to_bigquery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/myuon/gallon/cmd"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	zapLog := zap.Must(zap.NewDevelopment())
	defer zapLog.Sync()
	zap.ReplaceGlobals(zapLog)
}

type UserTable struct {
	ID        string         `json:"id" bigquery:"id"`
	Name      string         `json:"name" bigquery:"name"`
	Age       int            `json:"age" bigquery:"age"`
	CreatedAt int64          `json:"created_at" bigquery:"created_at"`
	Address   Address        `json:"address" bigquery:"address"`
	Metadata  map[string]any `json:"metadata" bigquery:"metadata"`
	Skills    []Skill        `json:"skills" bigquery:"skills"`
}

type Address struct {
	Street  string `json:"street" bigquery:"street"`
	City    string `json:"city" bigquery:"city"`
	Country string `json:"country" bigquery:"country"`
}

type Skill struct {
	Name  string `json:"name" bigquery:"name"`
	Level int    `json:"level" bigquery:"level"`
}

var client *bigquery.Client
var endpoint string
var dynamoEndpoint string
var dynamoClient *dynamodb.Client

func TestMain(m *testing.M) {
	var exitCode int
	defer func() {
		os.Exit(exitCode)
	}()

	pool, err := dockertest.NewPool("")
	pool.MaxWait = time.Minute * 2
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// BigQuery Emulator
	bqResource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository:   "ghcr.io/goccy/bigquery-emulator",
			Tag:          "latest",
			Cmd:          []string{"--project=test", "--data-from-yaml=/testdata/data.yaml"},
			ExposedPorts: []string{"9050/tcp"},
			Mounts:       []string{fmt.Sprintf("%v/testdata:/testdata", os.Getenv("PWD"))},
			Platform:     "linux/amd64",
		},
		func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{
				Name: "no",
			}
		},
	)
	defer func() {
		if err := bqResource.Close(); err != nil {
			log.Panicf("Could not close resource: %s", err)
		}
	}()
	if err != nil {
		log.Panicf("Could not start resource: %s", err)
	}
	if err := bqResource.Expire(2 * 60); err != nil {
		log.Panicf("Could not set expiration: %s", err)
	}

	bqPort := bqResource.GetPort("9050/tcp")
	endpoint = fmt.Sprintf("http://localhost:%v", bqPort)

	client, err = bigquery.NewClient(context.Background(), "test", option.WithEndpoint(endpoint), option.WithoutAuthentication())
	if err != nil {
		log.Panicf("Could not create client: %v", err)
	}

	if err := pool.Retry(func() error {
		log.Println("Trying to connect to BigQuery...")
		_, err := client.Dataset("dataset1").Metadata(context.Background())
		return err
	}); err != nil {
		log.Panicf("Could not connect to BigQuery: %v", err)
	}

	// DynamoDB Local
	dynamoResource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository:   "amazon/dynamodb-local",
			Tag:          "latest",
			ExposedPorts: []string{"8000/tcp"},
		},
		func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{
				Name: "no",
			}
		},
	)
	defer func() {
		if err := dynamoResource.Close(); err != nil {
			log.Panicf("Could not close resource: %s", err)
		}
	}()
	if err != nil {
		log.Panicf("Could not start resource: %s", err)
	}
	if err := dynamoResource.Expire(2 * 60); err != nil {
		log.Panicf("Could not set expiration: %s", err)
	}

	dynamoPort := dynamoResource.GetPort("8000/tcp")
	dynamoEndpoint = fmt.Sprintf("http://localhost:%v", dynamoPort)

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: dynamoEndpoint,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy",
			},
		}),
	)
	if err != nil {
		log.Panicf("Could not create AWS config: %v", err)
	}

	dynamoClient = dynamodb.NewFromConfig(cfg)

	if err := pool.Retry(func() error {
		log.Println("Trying to connect to DynamoDB...")
		_, err := dynamoClient.ListTables(context.Background(), &dynamodb.ListTablesInput{})
		return err
	}); err != nil {
		log.Panicf("Could not connect to DynamoDB: %v", err)
	}

	exitCode = m.Run()
}

func Test_dynamo_to_bigquery(t *testing.T) {
	// DynamoDBテーブルの作成
	_, err := dynamoClient.CreateTable(context.Background(), &dynamodb.CreateTableInput{
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
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	if err != nil {
		t.Errorf("Could not create table: %s", err)
	}

	// テストデータの投入
	for i := 0; i < 100; i++ {
		_, err := dynamoClient.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String("users"),
			Item: map[string]types.AttributeValue{
				"id":         &types.AttributeValueMemberS{Value: uuid.New().String()},
				"name":       &types.AttributeValueMemberS{Value: fmt.Sprintf("User %d", i)},
				"age":        &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", rand.Intn(100)+1)},
				"created_at": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Unix())},
				// "address": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				// 	"street":  &types.AttributeValueMemberS{Value: "123 Main St"},
				// 	"city":    &types.AttributeValueMemberS{Value: "Anytown"},
				// 	"country": &types.AttributeValueMemberS{Value: "USA"},
				// }},
				// "skills": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				// 	&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				// 		"name":  &types.AttributeValueMemberS{Value: "Skill 1"},
				// 		"level": &types.AttributeValueMemberN{Value: "10"},
				// 	}},
				// 	&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				// 		"name":  &types.AttributeValueMemberS{Value: "Skill 2"},
				// 		"level": &types.AttributeValueMemberN{Value: "20"},
				// 	}},
				// }},
			},
		})
		if err != nil {
			t.Errorf("Could not put item: %s", err)
		}
	}

	// Gallonの設定
	configYml := fmt.Sprintf(`
in:
  type: dynamodb
  endpoint: %v
  region: us-east-1
  table: users
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: number
    created_at:
      type: number
    address:
      type: object
      properties:
        street:
          type: string
        city:
          type: string
        country:
          type: string
    skills:
      type: array
      items:
        type: object
        properties:
          name:
            type: string
          level:
            type: number
out:
  type: bigquery
  endpoint: %v
  projectId: test
  datasetId: dataset1
  tableId: user
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: integer
    created_at:
      type: integer
#    address:
#      type: record
#      fields:
#        street:
#          type: string
#        city:
#          type: string
#        country:
#          type: string
#    skills:
#      type: string
`, dynamoEndpoint, endpoint)

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	// BigQueryのテーブルを確認
	table := client.Dataset("dataset1").Table("user")

	metadata, err := table.Metadata(context.Background())
	if err != nil {
		t.Errorf("Could not get metadata: %s", err)
	}

	assert.Equal(t, "id", metadata.Schema[0].Name)
	assert.Equal(t, bigquery.StringFieldType, metadata.Schema[0].Type)
	assert.Equal(t, "name", metadata.Schema[1].Name)
	assert.Equal(t, bigquery.StringFieldType, metadata.Schema[1].Type)
	assert.Equal(t, "age", metadata.Schema[2].Name)
	assert.Equal(t, bigquery.IntegerFieldType, metadata.Schema[2].Type)
	assert.Equal(t, "created_at", metadata.Schema[3].Name)
	assert.Equal(t, bigquery.IntegerFieldType, metadata.Schema[3].Type)

	// データの確認
	it := table.Read(context.Background())

	count := 0
	recordSamples := []UserTable{}

	for {
		var v UserTable
		err := it.Next(&v)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Errorf("Could not iterate: %s", err)
		}

		count++
		if rand.Float32() < 0.1 {
			recordSamples = append(recordSamples, v)
		}
	}

	assert.Equal(t, 100, count)

	for _, record := range recordSamples {
		_, err := uuid.Parse(record.ID)
		assert.Nil(t, err)

		assert.NotEqual(t, "", record.Name)
		assert.NotEqual(t, 0, record.Age)
		assert.NotEqual(t, int64(0), record.CreatedAt)
	}
}
