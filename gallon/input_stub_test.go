package gallon

import (
	"context"
	"github.com/go-logr/logr"
)

type InputPluginStub struct {
	data [][]map[string]interface{}
}

func NewInputPluginStub(
	data [][]map[string]interface{},
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
	messages chan interface{},
	errs chan error,
) error {
	for _, page := range i.data {
		records := []interface{}{}
		for _, record := range page {
			records = append(records, record)
		}

		messages <- records
	}

	return nil
}
