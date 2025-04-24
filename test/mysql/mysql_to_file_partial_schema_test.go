package mysql

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/myuon/gallon/cmd"
)

func Test_mysql_to_file_partial_schema(t *testing.T) {
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
out:
  type: file
  filepath: ./output_partial.jsonl
  format: jsonl
`, databaseUrl)
	defer func() {
		if err := os.Remove("./output_partial.jsonl"); err != nil {
			t.Errorf("Could not remove output file: %s", err)
		}
	}()

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	jsonl, err := os.ReadFile("./output_partial.jsonl")
	if err != nil {
		t.Errorf("Could not read output file: %s", err)
	}

	// 出力されたJSONLの最初の行を確認
	firstLine := strings.Split(string(jsonl), "\n")[0]
	if !strings.Contains(firstLine, "id") || !strings.Contains(firstLine, "name") || !strings.Contains(firstLine, "age") {
		t.Errorf("Expected output to contain id, name, and age fields, got: %s", firstLine)
	}

	// 指定していないフィールドが含まれていないことを確認
	if strings.Contains(firstLine, "created_at") || strings.Contains(firstLine, "birthday") ||
		strings.Contains(firstLine, "has_partner") || strings.Contains(firstLine, "metadata") ||
		strings.Contains(firstLine, "balance") {
		t.Errorf("Output should not contain unspecified fields, got: %s", firstLine)
	}

	if strings.Count(string(jsonl), "\n") != 1000 {
		t.Errorf("Expected 1000 lines, got %d", strings.Count(string(jsonl), "\n"))
	}
}
