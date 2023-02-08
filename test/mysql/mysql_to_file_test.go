package mysql

import (
	"context"
	"database/sql"
	"fmt"
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
	HasPartner *bool     `json:"hasPartner"`
}

func NewFakeUserTable() (UserTable, error) {
	v := UserTable{}
	if err := gofakeit.Struct(&v); err != nil {
		return v, err
	}

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
		"has_partner BOOLEAN,",
		"PRIMARY KEY (id)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	}, "\n"))
	if err != nil {
		return err
	}
	queryCreateTable.Close()

	query, err := conn.PrepareContext(
		ctx,
		"INSERT INTO users (id, name, age, created_at, birthday, has_partner) VALUES (?, ?, ?, ?, ?, ?)",
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

		if _, err := query.Exec(v.ID, v.Name, v.Age, v.CreatedAt, v.Birthday, v.HasPartner); err != nil {
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
			Tag:        "5.7",
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
	fmt.Printf("%v\n", strings.Join(strings.Split(string(jsonl), "\n")[0:10], "\n"))

	if strings.Count(string(jsonl), "\n") != 1000 {
		t.Errorf("Expected 1000 lines, got %d", strings.Count(string(jsonl), "\n"))
	}
}
