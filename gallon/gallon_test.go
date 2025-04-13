package gallon

import (
	"context"
	"errors"
	"testing"
)

func Test_too_many_errors(t *testing.T) {
	output, err := NewOutputPluginStdoutFromConfig([]byte(`
format: json
`))
	if err != nil {
		t.Errorf("Could not create plugin: %s", err)
	}

	output.deserialize = func(i any) ([]byte, error) {
		return nil, errors.New("error")
	}

	data := [][]map[string]any{}
	for i := 0; i < 10; i++ {
		page := []map[string]any{}
		for j := 0; j < 10; j++ {
			page = append(page, map[string]any{
				"id":         "1",
				"name":       "foo",
				"age":        20,
				"created_at": 1234567890,
			})
		}

		data = append(data, page)
	}

	g := Gallon{
		Logger: logger,
		Input:  NewInputPluginStub(data),
		Output: output,
	}

	ctx := context.Background()

	if err := g.Run(ctx); err != ErrTooManyErrors {
		t.Errorf("Could not run command: %s", err)
	}
}
