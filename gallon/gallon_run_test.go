package gallon

import (
	"context"
	"errors"
	"testing"
)

func testPages(pages int, perPage int) [][]GallonRecord {
	data := [][]GallonRecord{}
	for i := 0; i < pages; i++ {
		page := []GallonRecord{}
		for j := 0; j < perPage; j++ {
			r := NewGallonRecord()
			r.Set("id", "1")
			page = append(page, r)
		}
		data = append(data, page)
	}
	return data
}

// Run used to log a Load failure and return nil, which made a failed migration
// indistinguishable from a successful one.
func Test_Run_returns_load_error(t *testing.T) {
	loadErr := errors.New("load failed")

	g := Gallon{
		Logger: logger,
		Input:  NewInputPluginStub(testPages(3, 10)),
		Output: &OutputPluginStub{loadErr: loadErr},
	}

	err := g.Run(context.Background())
	if !errors.Is(err, loadErr) {
		t.Errorf("expected the load error, got %v", err)
	}
}

func Test_Run_returns_extract_error(t *testing.T) {
	extractErr := errors.New("extract failed")

	g := Gallon{
		Logger: logger,
		Input:  &InputPluginStubFailing{data: testPages(3, 10), extractErr: extractErr},
		Output: &OutputPluginStub{},
	}

	err := g.Run(context.Background())
	if !errors.Is(err, extractErr) {
		t.Errorf("expected the extract error, got %v", err)
	}
}

func Test_Run_returns_nil_on_success(t *testing.T) {
	output := &OutputPluginStub{}

	g := Gallon{
		Logger: logger,
		Input:  NewInputPluginStub(testPages(3, 10)),
		Output: output,
	}

	if err := g.Run(context.Background()); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if output.loaded != 30 {
		t.Errorf("expected 30 loaded records, got %v", output.loaded)
	}
}
