package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/myuon/gallon/cmd"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"log"
	"os"
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

var client *bigquery.Client
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
			Repository:   "ghcr.io/goccy/bigquery-emulator",
			Tag:          "latest",
			Cmd:          []string{"--project=test", "--data-from-yaml=/testdata/data.yaml"},
			ExposedPorts: []string{"9050/tcp"},
			Mounts:       []string{fmt.Sprintf("%v/testdata:/testdata", os.Getenv("PWD"))},
		},
		func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{
				Name: "no",
			}
		},
	)
	defer func() {
		if err := resource.Close(); err != nil {
			log.Panicf("Could not close resource: %s", err)
		}
	}()
	if err != nil {
		log.Panicf("Could not start resource: %s", err)
	}
	if err := resource.Expire(2 * 60); err != nil {
		log.Panicf("Could not set expiration: %s", err)
	}

	port := resource.GetPort("9050/tcp")
	endpoint = fmt.Sprintf("http://localhost:%v", port)

	client, _ := bigquery.NewClient(context.Background(), "test", option.WithEndpoint(endpoint))

	if err := pool.Retry(func() error {
		log.Println("Trying to connect to database...")

		_, err := client.Dataset("dataset1").Metadata(context.Background())
		return err
	}); err != nil {
		log.Panicf("Could not connect to docker: %v", err)
	}

	exitCode = m.Run()
}

func Test_output_bigquery(t *testing.T) {
	log.Println("test run")
	configYml := fmt.Sprintf(`
in:
  type: random
  schema:
    id:
      type: uuid
    name:
      type: name
    age:
      type: int
      min: 0
      max: 100
    created_at:
      type: unixtime
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
`, endpoint)
	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	table := client.Dataset("dataset1").Table("user")
	job, err := client.Query(fmt.Sprintf("SELECT * FROM %v", table.FullyQualifiedName())).Run(context.Background())
	if err != nil {
		t.Errorf("Could not run query: %s", err)
	}

	if _, err := job.Wait(context.Background()); err != nil {
		t.Errorf("Could not wait for job: %s", err)
	}

	it, err := job.Read(context.Background())
	if err != nil {
		t.Errorf("Could not read job: %s", err)
	}

	for {
		var v UserTable
		err := it.Next(&v)
		if err != nil {
			t.Errorf("Could not iterate: %s", err)
		}

		log.Printf("Got: %v", v)
	}
}
