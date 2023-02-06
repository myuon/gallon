package random

import (
	"github.com/myuon/gallon/cmd"
	"go.uber.org/zap"
	"testing"
)

func init() {
	zapLog := zap.Must(zap.NewDevelopment())
	defer zapLog.Sync()
	zap.ReplaceGlobals(zapLog)
}

func Test_random_to_stdout(t *testing.T) {
	configYml := `
in:
  type: random
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
  type: stdout
  format: json
`

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}
}
