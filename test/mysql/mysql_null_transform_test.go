package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/myuon/gallon/cmd"
	"github.com/stretchr/testify/assert"
)

func Test_mysql_to_file_with_null_transform(t *testing.T) {
	// Create a table with nullable column that has transforms
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Could not get connection: %s", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS test_null_transform (
			id VARCHAR(255) NOT NULL,
			nullable_datetime DATETIME,
			PRIMARY KEY (id)
		)
	`)
	if err != nil {
		t.Fatalf("Could not create table: %s", err)
	}
	defer func() {
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS test_null_transform")
		if err != nil {
			t.Errorf("Could not drop table: %s", err)
		}
	}()

	// Insert test data with NULL values
	_, err = conn.ExecContext(ctx, `
		INSERT INTO test_null_transform (id, nullable_datetime) VALUES
		('1', '2024-01-01 10:00:00'),
		('2', NULL),
		('3', '2024-01-03 15:30:00')
	`)
	if err != nil {
		t.Fatalf("Could not insert test data: %s", err)
	}

	// Test with transform on nullable column
	configYml := fmt.Sprintf(`
in:
  type: sql
  driver: mysql
  table: test_null_transform
  database_url: %v
  schema:
    id:
      type: string
    nullable_datetime:
      type: time
      transforms:
        - type: string
          format: "2006-01-02 15:04:05"
out:
  type: file
  filepath: ./output_null_transform.jsonl
  format: jsonl
`, databaseUrl)

	defer func() {
		if err := os.Remove("./output_null_transform.jsonl"); err != nil {
			t.Logf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output_null_transform.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	lines := strings.Split(string(jsonl), "\n")
	assert.Equal(t, 3, len(lines)-1, "Expected 3 records")

	// Parse and check each record
	for i, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Errorf("Failed to parse line %d: %v", i, err)
			continue
		}

		id := record["id"].(string)
		if id == "2" {
			// NULL value should remain as null
			if record["nullable_datetime"] != nil {
				t.Errorf("Expected nullable_datetime to be null for id=2, got %v", record["nullable_datetime"])
			}
		} else {
			// Non-NULL values should be transformed to string
			datetime, ok := record["nullable_datetime"].(string)
			if !ok {
				t.Errorf("Expected nullable_datetime to be string for id=%s, got %T", id, record["nullable_datetime"])
			} else if datetime == "" {
				t.Errorf("Expected nullable_datetime to be non-empty for id=%s", id)
			}
		}
	}
}
