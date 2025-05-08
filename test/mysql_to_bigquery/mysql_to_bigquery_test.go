package mysql_to_bigquery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/myuon/gallon/cmd"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	zapLog := zap.Must(zap.NewDevelopment())
	defer zapLog.Sync()
	zap.ReplaceGlobals(zapLog)
}

type UserTable struct {
	ID         string    `json:"id" fake:"{uuid}"`
	Name       string    `json:"name" fake:"{firstname}"`
	Age        int       `json:"age" fake:"{number:1,100}"`
	CreatedAt  int64     `json:"createdAt" fake:"{number:949720320,1896491520}"`
	Birthday   time.Time `json:"birthday"`
	JoinDate   time.Time `json:"joinDate"`
	HasPartner *bool     `json:"hasPartner"`
	Metadata   *string   `json:"metadata"`
	Balance    float64   `json:"balance" fake:"{price:0,1000}"`
}

func NewFakeUserTable() (UserTable, error) {
	v := UserTable{}
	if err := gofakeit.Struct(&v); err != nil {
		return v, err
	}

	// Generate nillable hasPartner
	// NOTE: gofakeit does not support nullable bool
	if gofakeit.Bool() {
		v.HasPartner = nil
	}

	// Generate metadata
	if gofakeit.Bool() {
		v.Metadata = nil
	} else if gofakeit.Bool() {
		metadata := "{\"key\":\"value\"}"
		v.Metadata = &metadata
	} else {
		metadata := "null"
		v.Metadata = &metadata
	}

	v.Birthday = time.Unix(v.CreatedAt, 0)
	v.JoinDate = v.Birthday.AddDate(0, 0, 1)

	return v, nil
}

func Migrate(db *sql.DB) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	queryCreateTable, err := conn.QueryContext(ctx, strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS users (",
		"id VARCHAR(255) NOT NULL,",
		"name VARCHAR(255) NOT NULL,",
		"age INT NOT NULL,",
		"created_at INT NOT NULL,",
		"birthday DATETIME NOT NULL,",
		"join_date DATE NOT NULL,",
		"has_partner BOOLEAN,",
		"metadata JSON,",
		"balance DECIMAL(10,2) NOT NULL,",
		"PRIMARY KEY (id)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	}, "\n"))
	if err != nil {
		return err
	}
	queryCreateTable.Close()

	query, err := conn.PrepareContext(
		ctx,
		"INSERT INTO users (id, name, age, created_at, birthday, join_date, has_partner, metadata, balance) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
	)
	if err != nil {
		return err
	}
	defer query.Close()

	for i := 0; i < 1000; i++ {
		v, err := NewFakeUserTable()
		if err != nil {
			return err
		}

		if _, err := query.Exec(v.ID, v.Name, v.Age, v.CreatedAt, v.Birthday, v.JoinDate, v.HasPartner, v.Metadata, v.Balance); err != nil {
			return err
		}
	}

	log.Printf("Migrated %v rows", 1000)

	return nil
}

var db *sql.DB
var databaseUrl string
var bqClient *bigquery.Client
var bqEndpoint string

func TestMain(m *testing.M) {
	var exitCode int
	defer func() {
		os.Exit(exitCode)
	}()

	// MySQLコンテナの起動
	mysqlPool, err := dockertest.NewPool("")
	mysqlPool.MaxWait = time.Minute * 2
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = mysqlPool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	mysqlResource, err := mysqlPool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "mysql",
			Tag:        "8.0",
			Env:        []string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=test", "MYSQL_CHARSET=utf8mb4"},
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
	if err := mysqlResource.Expire(2 * 60); err != nil {
		log.Fatalf("Could not expire resource: %s", err)
	}

	defer func() {
		if err := mysqlPool.Purge(mysqlResource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	mysqlPort := mysqlResource.GetPort("3306/tcp")
	databaseUrl = fmt.Sprintf("root:root@tcp(localhost:%v)/test?parseTime=false&loc=Asia%%2FTokyo", mysqlPort)

	if err = mysqlPool.Retry(func() error {
		log.Println("Trying to connect to MySQL...")

		var err error
		db, err = sql.Open("mysql", databaseUrl)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to MySQL: %s", err)
	}
	defer db.Close()

	// BigQuery Emulatorの起動
	bqPool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	resource, err := bqPool.RunWithOptions(
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
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	if err := resource.Expire(2 * 60); err != nil {
		log.Fatalf("Could not expire resource: %s", err)
	}

	defer func() {
		if err := bqPool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	bqPort := resource.GetPort("9050/tcp")
	bqEndpoint = fmt.Sprintf("http://localhost:%v", bqPort)

	bqClient, err = bigquery.NewClient(context.Background(), "test", option.WithEndpoint(bqEndpoint), option.WithoutAuthentication())
	if err != nil {
		log.Fatalf("Could not create BigQuery client: %v", err)
	}

	if err := bqPool.Retry(func() error {
		log.Println("Trying to connect to BigQuery...")

		err := bqClient.Dataset("dataset1").Create(context.Background(), nil)
		if err != nil {
			log.Printf("err: %v", err)
			return err
		}

		return nil
	}); err != nil {
		log.Fatalf("Could not connect to BigQuery: %v", err)
	}

	// データのマイグレーション
	if err := Migrate(db); err != nil {
		log.Fatalf("Could not migrate data: %s", err)
	}

	exitCode = m.Run()
}

func Test_mysql_to_bigquery(t *testing.T) {
	configYml := fmt.Sprintf(`
in:
  type: sql
  driver: mysql
  table: users
  database_url: %v
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: int
    created_at:
      type: int
    birthday:
      type: time
    join_date:
      type: date
    has_partner:
      type: bool
    metadata:
      type: json
    balance:
      type: decimal
out:
  type: bigquery
  endpoint: %v
  projectId: test
  datasetId: dataset1
  tableId: users
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: integer
    created_at:
      type: integer
    birthday:
      type: timestamp
    join_date:
      type: string
    has_partner:
      type: boolean
    metadata:
      type: json
    balance:
      type: float
`, databaseUrl, bqEndpoint)

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
		return
	}

	table := bqClient.Dataset("dataset1").Table("users")

	metadata, err := table.Metadata(context.Background())
	if err != nil {
		t.Errorf("Could not get metadata: %s", err)
		return
	}

	assert.Equal(t, "id", metadata.Schema[0].Name)
	assert.Equal(t, bigquery.StringFieldType, metadata.Schema[0].Type)
	assert.Equal(t, "name", metadata.Schema[1].Name)
	assert.Equal(t, bigquery.StringFieldType, metadata.Schema[1].Type)
	assert.Equal(t, "age", metadata.Schema[2].Name)
	assert.Equal(t, bigquery.IntegerFieldType, metadata.Schema[2].Type)
	assert.Equal(t, "created_at", metadata.Schema[3].Name)
	assert.Equal(t, bigquery.IntegerFieldType, metadata.Schema[3].Type)
	assert.Equal(t, "birthday", metadata.Schema[4].Name)
	assert.Equal(t, bigquery.TimestampFieldType, metadata.Schema[4].Type)
	assert.Equal(t, "join_date", metadata.Schema[5].Name)
	assert.Equal(t, bigquery.StringFieldType, metadata.Schema[5].Type)
	assert.Equal(t, "has_partner", metadata.Schema[6].Name)
	assert.Equal(t, bigquery.BooleanFieldType, metadata.Schema[6].Type)
	assert.Equal(t, "metadata", metadata.Schema[7].Name)
	assert.Equal(t, bigquery.JSONFieldType, metadata.Schema[7].Type)
	assert.Equal(t, "balance", metadata.Schema[8].Name)
	assert.Equal(t, bigquery.FloatFieldType, metadata.Schema[8].Type)

	it := table.Read(context.Background())

	count := 0
	recordSamples := []map[string]bigquery.Value{}

	for {
		var v map[string]bigquery.Value
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

	assert.Equal(t, 1000, count)

	for _, record := range recordSamples[0:10] {
		log.Printf("record: %v", record)

		assert.NotEqual(t, "", record["id"])
		assert.NotEqual(t, "", record["name"])
		assert.NotEqual(t, 0, record["age"])
		assert.NotEqual(t, int64(0), record["created_at"])
		assert.NotEqual(t, time.Time{}, record["birthday"])
		assert.NotEqual(t, float64(0), record["balance"])
		assert.NotEqual(t, "", record["join_date"])
		assert.NotEqual(t, "", record["metadata"])
	}
}

func Test_mysql_to_bigquery_null(t *testing.T) {
	// テスト用のテーブルを作成
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Could not get connection: %s", err)
	}
	defer conn.Close()

	queryCreateTable, err := conn.QueryContext(ctx, strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS null_test (",
		"id VARCHAR(255) NOT NULL,",
		"nullable_string VARCHAR(255),",
		"PRIMARY KEY (id)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	}, "\n"))
	if err != nil {
		t.Fatalf("Could not create table: %s", err)
	}
	queryCreateTable.Close()

	// テストデータを挿入
	query, err := conn.PrepareContext(
		ctx,
		"INSERT INTO null_test (id, nullable_string) VALUES (?, ?)",
	)
	if err != nil {
		t.Fatalf("Could not prepare query: %s", err)
	}
	defer query.Close()

	// NULL値と非NULL値の両方を挿入
	if _, err := query.Exec("1", "not null"); err != nil {
		t.Fatalf("Could not insert data: %s", err)
	}
	if _, err := query.Exec("2", nil); err != nil {
		t.Fatalf("Could not insert data: %s", err)
	}

	configYml := fmt.Sprintf(`
in:
  type: sql
  driver: mysql
  table: null_test
  database_url: %v
  schema:
    id:
      type: string
    nullable_string:
      type: string
out:
  type: bigquery
  endpoint: %v
  projectId: test
  datasetId: dataset1
  tableId: null_test
  schema:
    id:
      type: string
    nullable_string:
      type: string
`, databaseUrl, bqEndpoint)

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Fatalf("Could not run command: %s", err)
	}

	table := bqClient.Dataset("dataset1").Table("null_test")
	it := table.Read(context.Background())

	records := make([]map[string]bigquery.Value, 0)
	for {
		var v map[string]bigquery.Value
		err := it.Next(&v)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Fatalf("Could not iterate: %s", err)
		}
		records = append(records, v)
	}

	// レコード数の確認
	assert.Equal(t, 2, len(records))

	// NULL値と非NULL値の確認
	for _, record := range records {
		log.Printf("record: %v", record)

		if record["id"] == "1" {
			assert.Equal(t, "not null", record["nullable_string"])
		} else if record["id"] == "2" {
			assert.Nil(t, record["nullable_string"])
		} else {
			t.Fatalf("Unexpected id: %v", record["id"])
		}
	}
}
