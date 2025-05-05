package gallon

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/go-logr/zapr"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var logger = zapr.NewLogger(zap.Must(zap.NewDevelopment()))

type BufioWriteCloser struct {
	*bufio.Writer
}

func NewNopWriteCloser(w *bufio.Writer) BufioWriteCloser {
	return BufioWriteCloser{w}
}

func (b BufioWriteCloser) Close() error {
	return b.Flush()
}

func Test_format_csv(t *testing.T) {
	configYml := `
out:
  type: file
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
		return NewNopWriteCloser(writer), nil
	}

	r1 := NewGallonRecord()
	r1.Set("id", "1")
	r1.Set("name", "foo")
	r1.Set("age", 20)
	r1.Set("created_at", 1234567890)

	r2 := NewGallonRecord()
	r2.Set("id", "2")
	r2.Set("name", "bar")
	r2.Set("age", 30)
	r2.Set("created_at", 1234567890)

	r3 := NewGallonRecord()
	r3.Set("id", "3")
	r3.Set("name", "baz")
	r3.Set("age", 40)
	r3.Set("created_at", 1234567890)

	g := Gallon{
		Logger: logger,
		Input: NewInputPluginStub([][]GallonRecord{
			{r1, r2},
			{r3},
		}),
		Output: plugin,
	}
	if err := g.Run(context.Background()); err != nil {
		t.Errorf("Could not run command: %s", err)
	}

	expected := `1,foo,20,1234567890
2,bar,30,1234567890
3,baz,40,1234567890
`

	assert.Equal(t, expected, buf.String())
}
