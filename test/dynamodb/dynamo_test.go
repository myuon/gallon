package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/myuon/gallon/cmd"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"go.uber.org/zap"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func init() {
	zapLog := zap.Must(zap.NewDevelopment())
	defer zapLog.Sync()
	zap.ReplaceGlobals(zapLog)
}

type UserTable struct {
	ID        string `json:"id" fake:"{uuid}"`
	Name      string `json:"name" fake:"{firstname}"`
	Age       int    `json:"age" fake:"{number:1,100}"`
	CreatedAt int64  `json:"createdAt" fake:"{number:949720320,1896491520}"`
}

var client *dynamodb.Client
var endpoint string

func NewFakeUserTable() (UserTable, error) {
	v := UserTable{}
	if err := gofakeit.Struct(&v); err != nil {
		return v, err
	}

	return v, nil
}

func Migrate(client *dynamodb.Client) error {
	ctx := context.Background()
	if _, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
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

		record := map[string]types.AttributeValue{
			"id":         &types.AttributeValueMemberS{Value: v.ID},
			"name":       &types.AttributeValueMemberS{Value: v.Name},
			"age":        &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", v.Age)},
			"created_at": &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", v.CreatedAt)},
		}

		if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("users"),
			Item:      record,
		}); err != nil {
			return err
		}
	}

	log.Printf("Migrated %v rows", 1000)

	return nil
}

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

	resource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "amazon/dynamodb-local",
			Tag:        "latest",
			Cmd:        []string{"-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"},
		},
		func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{
				Name: "no",
			}
		},
	)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	if err := resource.Expire(2 * 60); err != nil {
		log.Fatalf("Could not set expiration: %s", err)
	}

	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}

		log.Println("Purged resource")
	}()

	port := resource.GetPort("8000/tcp")
	endpoint = fmt.Sprintf("http://localhost:%v", port)

	cfg := aws.Config{}
	cfg.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: endpoint}, nil
		})
	cfg.Credentials = credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
			Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
		},
	}
	client = dynamodb.NewFromConfig(cfg)

	if err := pool.Retry(func() error {
		log.Println("Trying to connect to database...")

		_, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %v", err)
	}

	if err := Migrate(client); err != nil {
		log.Fatalf("Could not migrate: %v", err)
	}

	exitCode = m.Run()
}

func Test_dynamodb_to_file(t *testing.T) {
	configYml := fmt.Sprintf(`
in:
  type: dynamodb
  table: users
  endpoint: %v
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: number
    created_at:
      type: number
out:
  type: file
  filepath: ./output.jsonl
  format: jsonl
`, endpoint)
	defer func() {
		if err := os.Remove("./output.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	if strings.Count(string(jsonl), "\n") != 1000 {
		t.Errorf("Expected 1000 lines, got %d", strings.Count(string(jsonl), "\n"))
	}
}
