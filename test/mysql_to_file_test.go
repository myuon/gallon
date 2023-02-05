package test

import (
	"database/sql"
	"fmt"
	"github.com/myuon/gallon/cmd"
	"github.com/ory/dockertest/v3"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	pool.MaxWait = time.Minute * 2
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	resource, err := pool.Run(
		"mysql",
		"5.7",
		[]string{"MYSQL_ROOT_PASSWORD=root", "MYSQL_DATABASE=test", "MYSQL_CHARSET=utf8mb4"},
	)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	if err = pool.Retry(func() error {
		log.Println("Trying to connect to mysql...")

		var err error
		db, err = sql.Open("mysql", fmt.Sprintf("root:root@(localhost:%s)/test?parseTime=true", resource.GetPort("3306/tcp")))
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	log.Println("Connected to mysql")

	// Migrate data
	stdout, err := exec.Command("go1.20", "run", "./data_to_mysql/main.go").Output()
	log.Println(string(stdout))

	if err != nil {
		log.Fatalf("Could not migrate data: %s", err)
	}

	log.Println("Migrated data")

	log.Println("Starting tests...")

	exitVal := m.Run()

	// When you're done, kill and remove the container
	if err = pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(exitVal)
}

func Test_mysql_to_file(t *testing.T) {
	configYml := `
in:
  type: sql
  driver: mysql
  table: users
  database_url: root:root@tcp(localhost:3306)/test
  schema:
    id:
      type: string
    name:
      type: string
    age:
      type: int
    created_at:
      type: int
out:
  type: file
  filepath: ./output.jsonl
  format: jsonl
`
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
