package gallon

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
)

type InputPluginStub struct {
	data [][]GallonRecord
}

func NewInputPluginStub(
	data [][]GallonRecord,
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
	messages chan []GallonRecord,
	errs chan error,
) error {
	p := 0

	for p < len(i.data) {
		select {
		case <-ctx.Done():
			return nil
		default:
			if len(i.data[p]) > 0 {
				messages <- i.data[p]
				logger.Info(fmt.Sprintf("extracted %v records", len(i.data[p])), "page", p)
			}
			p++
		}
	}

	return nil
}
