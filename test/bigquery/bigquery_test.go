package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
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
	ID        string `json:"id" bigquery:"id"`
	Name      string `json:"name" bigquery:"name"`
	Age       int    `json:"age" bigquery:"age"`
	CreatedAt int64  `json:"created_at" bigquery:"created_at"`
}

type Address struct {
	Street  string `json:"street" bigquery:"street"`
	City    string `json:"city" bigquery:"city"`
	Country string `json:"country" bigquery:"country"`
}

type UserWithAddress struct {
	ID        string  `json:"id" bigquery:"id"`
	Name      string  `json:"name" bigquery:"name"`
	Address   Address `json:"address" bigquery:"address"`
	CreatedAt int64   `json:"created_at" bigquery:"created_at"`
}

type UserWithJSON struct {
	ID        string         `json:"id" bigquery:"id"`
	Name      string         `json:"name" bigquery:"name"`
	Metadata  map[string]any `json:"metadata" bigquery:"metadata"`
	CreatedAt int64          `json:"created_at" bigquery:"created_at"`
}

var client *bigquery.Client
var endpoint string

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

	client, err = bigquery.NewClient(context.Background(), "test", option.WithEndpoint(endpoint), option.WithoutAuthentication())
	if err != nil {
		log.Panicf("Could not create client: %v", err)
	}

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
      min: 1
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

/*
func Test_output_bigquery_with_record_type(t *testing.T) {
	configYml := fmt.Sprintf(`
in:
  type: random
  schema:
    id:
      type: uuid
    name:
      type: name
    address:
      type: record
      fields:
        street:
          type: string
        city:
          type: string
        country:
          type: string
    created_at:
      type: unixtime
out:
  type: bigquery
  endpoint: %v
  projectId: test
  datasetId: dataset1
  tableId: user_with_address
  schema:
    id:
      type: string
    name:
      type: string
    address:
      type: record
      fields:
        street:
          type: string
        city:
          type: string
        country:
          type: string
    created_at:
      type: integer
`, endpoint)

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	it := client.Dataset("dataset1").Table("user_with_address").Read(context.Background())

	count := 0
	recordSamples := []UserWithAddress{}

	for {
		var v UserWithAddress
		err := it.Next(&v)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Errorf("Could not iterate: %s", err)
			break
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
		assert.NotEqual(t, "", record.Address.Street)
		assert.NotEqual(t, "", record.Address.City)
		assert.NotEqual(t, "", record.Address.Country)
		assert.NotEqual(t, int64(0), record.CreatedAt)
	}
}
*/

func Test_output_bigquery_with_json_type(t *testing.T) {
	configYml := fmt.Sprintf(`
in:
  type: random
  schema:
    id:
      type: uuid
    name:
      type: name
    metadata:
      type: record
      fields:
        preferences:
          type: record
          fields:
            theme:
              type: string
            language:
              type: string
        tag:
          type: string
    created_at:
      type: unixtime
out:
  type: bigquery
  endpoint: %v
  projectId: test
  datasetId: dataset1
  tableId: user_with_json
  schema:
    id:
      type: string
    name:
      type: string
    metadata:
      type: json
    created_at:
      type: integer
`, endpoint)

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	table := client.Dataset("dataset1").Table("user_with_json")
	it := table.Read(context.Background())

	count := 0
	recordSamples := []UserWithJSON{}

	for {
		var item map[string]bigquery.Value
		err := it.Next(&item)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Errorf("Could not iterate: %s", err)
			break
		}

		count++
		if rand.Float32() < 0.1 {
			metadataString := item["metadata"].(string)

			// json type is formed as a string
			var metadataRaw string
			if err := json.Unmarshal([]byte(metadataString), &metadataRaw); err != nil {
				t.Errorf("Could not unmarshal metadata: %s", err)
				break
			}

			var metadata map[string]any
			if err := json.Unmarshal([]byte(metadataRaw), &metadata); err != nil {
				t.Errorf("Could not unmarshal metadata: %s", err)
				break
			}

			recordSamples = append(recordSamples, UserWithJSON{
				ID:        item["id"].(string),
				Name:      item["name"].(string),
				Metadata:  metadata,
				CreatedAt: item["created_at"].(int64),
			})
		}
	}

	assert.Equal(t, 100, count)

	for _, record := range recordSamples {
		_, err := uuid.Parse(record.ID)
		assert.Nil(t, err)

		assert.NotEqual(t, "", record.Name)
		assert.NotNil(t, record.Metadata)

		// metadataが正しいJSON構造を持っていることを確認
		preferences, ok := record.Metadata["preferences"].(map[string]interface{})
		assert.True(t, ok)
		assert.NotNil(t, preferences["theme"])
		assert.NotNil(t, preferences["language"])

		tag, ok := record.Metadata["tag"].(string)
		assert.True(t, ok)
		assert.NotEqual(t, "", tag)

		assert.NotEqual(t, int64(0), record.CreatedAt)
	}
}
