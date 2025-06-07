package gallon

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
)

func Test_too_many_errors(t *testing.T) {
	output, err := NewOutputPluginStdoutFromConfig([]byte(`
format: json
`))
	if err != nil {
		t.Errorf("Could not create plugin: %s", err)
	}

	output.deserialize = func(i GallonRecord) ([]byte, error) {
		return nil, errors.New("error")
	}

	data := [][]GallonRecord{}
	for i := 0; i < 10; i++ {
		page := []GallonRecord{}
		for j := 0; j < 10; j++ {
			r := NewGallonRecord()
			r.Set("id", "1")
			r.Set("name", "foo")
			r.Set("age", 20)
			r.Set("created_at", 1234567890)

			page = append(page, r)
		}

		data = append(data, page)
	}

	g := Gallon{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		Input:  NewInputPluginStub(data),
		Output: output,
	}

	ctx := context.Background()

	if err := g.Run(ctx); err != ErrTooManyErrors {
		t.Errorf("Could not run command: %s", err)
	}
}
