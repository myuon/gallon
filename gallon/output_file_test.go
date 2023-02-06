package gallon

import (
	"bufio"
	"bytes"
	"context"
	"github.com/docker/docker/pkg/ioutils"
	"github.com/go-logr/zapr"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io"
	"testing"
)

var logger = zapr.NewLogger(zap.Must(zap.NewDevelopment()))

func Test_format_csv(t *testing.T) {
	configYml := `
format: csv
filepath: ./virtual
header: true
`

	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)

	plugin, err := NewOutputPluginFileFromConfig([]byte(configYml))
	if err != nil {
		t.Errorf("Could not create plugin: %s", err)
	}
	plugin.newWriter = func() (io.WriteCloser, error) {
		return ioutils.NewWriteCloserWrapper(writer, func() error {
			return writer.Flush()
		}), nil
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
	if err := g.Run(context.Background()); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	if err := writer.Flush(); err != nil {
		t.Errorf("Could not flush: %s", err)
	}

	expected := `20,1234567890,1,foo
30,1234567890,2,bar
40,1234567890,3,baz
`

	assert.Equal(t, expected, buf.String())
}
