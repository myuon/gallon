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
  type: stdout
  format: json
`

	if err := cmd.RunGallon([]byte(configYml)); err != nil {
		t.Errorf("Could not run command: %s", err)
	}
}
