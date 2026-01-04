package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/myuon/gallon/cmd"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

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
	IsActive   int       `json:"isActive" fake:"{number:0,1}"`
	IsPremium  int       `json:"isPremium" fake:"{number:0,1}"`
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
		"is_active BIT(1) NOT NULL,",
		"is_premium TINYINT(1) NOT NULL,",
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
		"INSERT INTO users (id, name, age, created_at, birthday, join_date, has_partner, is_active, is_premium, metadata, balance) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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

		if _, err := query.Exec(v.ID, v.Name, v.Age, v.CreatedAt, v.Birthday, v.JoinDate, v.HasPartner, v.IsActive, v.IsPremium, v.Metadata, v.Balance); err != nil {
			return err
		}
	}

	log.Printf("Migrated %v rows", 1000)

	return nil
}

var db *sql.DB
var databaseUrl string

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
	port := resource.GetPort("3306/tcp")

	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	if err := resource.Expire(2 * 60); err != nil {
		log.Fatalf("Could not expire resource: %s", err)
	}

	defer func() {
		// When you're done, kill and remove the container
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}

		log.Println("Purged resource")
	}()
	databaseUrl = fmt.Sprintf("root:root@tcp(localhost:%v)/test?parseTime=true&loc=Asia%%2FTokyo", port)

	if err = pool.Retry(func() error {
		log.Println("Trying to connect to database...")

		var err error
		db, err = sql.Open("mysql", databaseUrl)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	defer db.Close()

	log.Println("Connected to mysql")

	// Migrate data
	if err := Migrate(db); err != nil {
		log.Fatalf("Could not migrate data: %s", err)
	}

	log.Println("Migrated data")

	log.Println("Starting tests...")

	exitCode = m.Run()

	log.Println("Tests finished")
}

func Test_mysql_to_file(t *testing.T) {
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
    has_partner:
      type: bool
    metadata:
      type: json
    balance:
      type: decimal
out:
  type: file
  filepath: ./output.jsonl
  format: jsonl
`, databaseUrl)
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
	log.Println(strings.Join(strings.Split(string(jsonl), "\n")[0:10], "\n"))

	if strings.Count(string(jsonl), "\n") != 1000 {
		t.Errorf("Expected 1000 lines, got %d", strings.Count(string(jsonl), "\n"))
	}

	record := strings.Split(string(jsonl), "\n")[0]

	// checks key order
	parts := strings.Split(record, ",")
	assert.Contains(t, parts[0], "\"id\":")
	assert.Contains(t, parts[1], "\"name\":")
	assert.Contains(t, parts[2], "\"age\":")
	assert.Contains(t, parts[3], "\"created_at\":")
	assert.Contains(t, parts[4], "\"birthday\":")
	assert.Contains(t, parts[5], "\"has_partner\":")

}

func Test_mysql_to_file_with_tinyint(t *testing.T) {
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
    has_partner:
      type: bool
    is_active:
      type: bool
    metadata:
      type: json
    balance:
      type: decimal
out:
  type: file
  filepath: ./output_tinyint.jsonl
  format: jsonl
`, databaseUrl)
	defer func() {
		if err := os.Remove("./output_tinyint.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output_tinyint.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	if len(lines) > 10 {
		lines = lines[:10]
	}
	fmt.Printf("%v\n", strings.Join(lines, "\n"))

	if strings.Count(string(jsonl), "\n") != 1000 {
		t.Errorf("Expected 1000 lines, got %d", strings.Count(string(jsonl), "\n"))
	}

	for i, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Errorf("Failed to parse line %d: %v", i, err)
			continue
		}

		if _, ok := record["is_active"].(bool); !ok {
			t.Errorf("is_active is not bool in line %d: %v", i, record["is_active"])
		}
	}
}

func Test_mysql_to_file_with_bit(t *testing.T) {
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
    has_partner:
      type: bool
    is_active:
      type: bool
    metadata:
      type: json
    balance:
      type: decimal
out:
  type: file
  filepath: ./output_bit.jsonl
  format: jsonl
`, databaseUrl)
	defer func() {
		if err := os.Remove("./output_bit.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output_bit.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	if len(lines) > 10 {
		lines = lines[:10]
	}
	fmt.Printf("%v\n", strings.Join(lines, "\n"))

	if strings.Count(string(jsonl), "\n") != 1000 {
		t.Errorf("Expected 1000 lines, got %d", strings.Count(string(jsonl), "\n"))
	}

	for i, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Errorf("Failed to parse line %d: %v", i, err)
			continue
		}

		if _, ok := record["is_active"].(bool); !ok {
			t.Errorf("is_active is not bool in line %d: %v", i, record["is_active"])
		}
	}
}

func Test_mysql_to_file_with_bool_types(t *testing.T) {
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
    has_partner:
      type: bool
    is_active:
      type: bool
    is_premium:
      type: bool
    metadata:
      type: json
    balance:
      type: decimal
out:
  type: file
  filepath: ./output_bool_types.jsonl
  format: jsonl
`, databaseUrl)
	defer func() {
		if err := os.Remove("./output_bool_types.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output_bool_types.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	if len(lines) > 10 {
		lines = lines[:10]
	}
	fmt.Printf("%v\n", strings.Join(lines, "\n"))

	if strings.Count(string(jsonl), "\n") != 1000 {
		t.Errorf("Expected 1000 lines, got %d", strings.Count(string(jsonl), "\n"))
	}

	for i, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Errorf("Failed to parse line %d: %v", i, err)
			continue
		}

		if _, ok := record["is_active"].(bool); !ok {
			t.Errorf("is_active is not bool in line %d: %v", i, record["is_active"])
		}

		if _, ok := record["is_premium"].(bool); !ok {
			t.Errorf("is_premium is not bool in line %d: %v", i, record["is_premium"])
		}
	}
}

func Test_mysql_to_file_with_date(t *testing.T) {
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
    is_active:
      type: bool
    is_premium:
      type: bool
    metadata:
      type: json
    balance:
      type: decimal
out:
  type: file
  filepath: ./output_date.jsonl
  format: jsonl
`, databaseUrl)
	defer func() {
		if err := os.Remove("./output_date.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output_date.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	if len(lines) > 10 {
		lines = lines[:10]
	}
	fmt.Printf("%v\n", strings.Join(lines, "\n"))

	if strings.Count(string(jsonl), "\n") != 1000 {
		t.Errorf("Expected 1000 lines, got %d", strings.Count(string(jsonl), "\n"))
	}

	for i, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Errorf("Failed to parse line %d: %v", i, err)
			continue
		}

		joinDate, ok := record["join_date"].(string)
		if !ok {
			t.Errorf("join_date is not string in line %d: %v", i, record["join_date"])
			continue
		}

		re := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
		if !re.MatchString(joinDate) {
			t.Errorf("join_date is not in YYYY-MM-DD format in line %d: %v", i, joinDate)
		}
	}
}

func Test_mysql_to_file_with_time_transform(t *testing.T) {
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
      transforms:
        - type: time
          as: unix
        - type: string
          format: "2006-01-02 15:04:05"
      rename: created_at_date
    birthday:
      type: time
      transforms:
        - type: string
          format: "2006-01-02 15:04:05"
    join_date:
      type: date
    has_partner:
      type: bool
    is_active:
      type: bool
    is_premium:
      type: bool
    metadata:
      type: json
    balance:
      type: decimal
out:
  type: file
  filepath: ./output_time_transform.jsonl
  format: jsonl
`, databaseUrl)
	defer func() {
		if err := os.Remove("./output_time_transform.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output_time_transform.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	if len(lines) > 10 {
		lines = lines[:10]
	}
	fmt.Printf("%v\n", strings.Join(lines, "\n"))

	if strings.Count(string(jsonl), "\n") != 1000 {
		t.Errorf("Expected 1000 lines, got %d", strings.Count(string(jsonl), "\n"))
	}

	for i, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Errorf("Failed to parse line %d: %v", i, err)
			continue
		}

		birthday, ok := record["birthday"].(string)
		if !ok {
			t.Errorf("birthday is not string in line %d: %v", i, record["birthday"])
			continue
		}

		re := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
		if !re.MatchString(birthday) {
			t.Errorf("birthday is not in YYYY-MM-DD HH:mm:ss format in line %d: %v", i, birthday)
		}

		// パースして実際に有効な日時かどうかも確認
		_, err := time.Parse("2006-01-02 15:04:05", birthday)
		if err != nil {
			t.Errorf("birthday is not a valid datetime in line %d: %v", i, err)
		}
	}
}

func Test_mysql_to_file_raw_query(t *testing.T) {
	configYml := fmt.Sprintf(`
in:
  type: sql
  driver: mysql
  database_url: %v
  query: "SELECT id, name, age FROM users WHERE age > 50"
out:
  type: file
  filepath: ./output_raw_query.jsonl
  format: jsonl
`, databaseUrl)
	defer func() {
		if err := os.Remove("./output_raw_query.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output_raw_query.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	if len(lines) > 10 {
		log.Println(strings.Join(lines[:10], "\n"))
	}

	// Should have some records (users with age > 50)
	recordCount := strings.Count(string(jsonl), "\n")
	if recordCount == 0 {
		t.Errorf("Expected some records, got 0")
	}
	log.Printf("Raw query returned %d records", recordCount)

	// Verify the record structure (should have only id, name, age from the query)
	for i, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Errorf("Failed to parse line %d: %v", i, err)
			continue
		}

		// Check that only the queried columns exist
		if _, ok := record["id"]; !ok {
			t.Errorf("id should exist in line %d", i)
		}
		if _, ok := record["name"]; !ok {
			t.Errorf("name should exist in line %d", i)
		}
		if _, ok := record["age"]; !ok {
			t.Errorf("age should exist in line %d", i)
		}

		// Verify age > 50 as per the query
		age, ok := record["age"].(float64)
		if !ok {
			t.Errorf("age is not a number in line %d: %v", i, record["age"])
			continue
		}
		if age <= 50 {
			t.Errorf("age should be > 50 in line %d, got %v", i, age)
		}

		// birthday, created_at etc should not exist (not in query)
		if _, ok := record["birthday"]; ok {
			t.Errorf("birthday should not exist in line %d (not selected in query)", i)
		}
	}
}

func Test_mysql_to_file_raw_query_with_join(t *testing.T) {
	// This test verifies that raw query mode works with complex queries like JOINs
	// For simplicity, we use a self-join to demonstrate the capability
	configYml := fmt.Sprintf(`
in:
  type: sql
  driver: mysql
  database_url: %v
  query: "SELECT u1.id as user_id, u1.name as user_name, COUNT(*) as same_age_count FROM users u1 INNER JOIN users u2 ON u1.age = u2.age GROUP BY u1.id, u1.name HAVING COUNT(*) > 1"
out:
  type: file
  filepath: ./output_raw_query_join.jsonl
  format: jsonl
`, databaseUrl)
	defer func() {
		if err := os.Remove("./output_raw_query_join.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output_raw_query_join.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	if len(lines) > 10 {
		log.Println(strings.Join(lines[:10], "\n"))
	}

	recordCount := strings.Count(string(jsonl), "\n")
	log.Printf("Raw query with JOIN returned %d records", recordCount)

	// Verify the record structure
	for i, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Errorf("Failed to parse line %d: %v", i, err)
			continue
		}

		// Check that aliased columns exist
		if _, ok := record["user_id"]; !ok {
			t.Errorf("user_id should exist in line %d", i)
		}
		if _, ok := record["user_name"]; !ok {
			t.Errorf("user_name should exist in line %d", i)
		}
		if _, ok := record["same_age_count"]; !ok {
			t.Errorf("same_age_count should exist in line %d", i)
		}
	}
}
