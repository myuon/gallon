package test

import (
	"database/sql"
	"fmt"
	"github.com/myuon/gallon/cmd"
	"github.com/ory/dockertest/v3"
	"log"
	"os"
	"os/exec"
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
	if err := exec.Command("go", "run", "./data_to_mysql/main.go").Run(); err != nil {
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

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Fatalf("Could not run command: %s", err)
	}
}
