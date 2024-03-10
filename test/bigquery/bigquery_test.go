package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"fmt"
	"github.com/myuon/gallon/cmd"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
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

	client, err = bigquery.NewClient(context.Background(), "test", option.WithEndpoint(endpoint))
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

	job, err := client.Query("SELECT * FROM `dataset1.user`").Run(context.Background())
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
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Errorf("Could not iterate: %s", err)
		}

		log.Printf("Got: %v", v)
	}
}
