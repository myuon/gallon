package gallon

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
)

type InputPluginStub struct {
	data [][]map[string]any
}

func NewInputPluginStub(
	data [][]map[string]any,
) *InputPluginStub {
	return &InputPluginStub{
		data: data,
	}
}

var _ InputPlugin = &InputPluginStub{}

func (i InputPluginStub) ReplaceLogger(logger logr.Logger) {
}

func (i InputPluginStub) Extract(
	ctx context.Context,
	messages chan any,
	errs chan error,
) error {
	p := 0

	for p < len(i.data) {
		select {
		case <-ctx.Done():
			return nil
		default:
			records := []any{}
			for _, record := range i.data[p] {
				records = append(records, record)
			}

			if len(records) > 0 {
				messages <- records
				logger.Info(fmt.Sprintf("extracted %v records", len(records)), "page", p)
			}
			p++
		}
	}

	return nil
}
