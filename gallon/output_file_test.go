package gallon

import (
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"testing"
)

var logger = zapr.NewLogger(zap.Must(zap.NewDevelopment()))

func Test_format_csv(t *testing.T) {
	configYml := `
format: csv
filepath: ./virtual
header: true
`

	plugin, err := NewOutputPluginFileFromConfig([]byte(configYml))
	if err != nil {
		t.Errorf("Could not create plugin: %s", err)
	}

	g := Gallon{
		Logger: logger,
		Input: NewInputPluginStub([][]map[string]interface{}{
			{
				{
					"id":         "1",
					"name":       "foo",
					"age":        20,
					"created_at": 1234567890,
				},
				{
					"id":         "2",
					"name":       "bar",
					"age":        30,
					"created_at": 1234567890,
				},
			},
			{
				{
					"id":         "3",
					"name":       "baz",
					"age":        40,
					"created_at": 1234567890,
				},
			},
		}),
		Output: plugin,
	}
	if err := g.Run(); err != nil {
		t.Errorf("Could not run command: %s", err)
	}
}
