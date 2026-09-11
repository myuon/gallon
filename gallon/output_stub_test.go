package gallon

import (
	"context"

	"github.com/go-logr/logr"
)

// OutputPluginStub drains messages and can be made to fail, so tests can check
// what Run does with a Load error.
type OutputPluginStub struct {
	loadErr error
	loaded  int
}

var _ OutputPlugin = &OutputPluginStub{}

func (o *OutputPluginStub) ReplaceLogger(logger logr.Logger) {}

func (o *OutputPluginStub) Cleanup() error { return nil }

func (o *OutputPluginStub) Load(
	ctx context.Context,
	messages chan []GallonRecord,
	errs chan error,
) error {
	if o.loadErr != nil {
		return o.loadErr
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msgs, ok := <-messages:
			if !ok {
				return nil
			}
			o.loaded += len(msgs)
		}
	}
}

// InputPluginStubFailing fails after handing over the pages it was given.
type InputPluginStubFailing struct {
	data       [][]GallonRecord
	extractErr error
}

var _ InputPlugin = &InputPluginStubFailing{}

func (i *InputPluginStubFailing) ReplaceLogger(logger logr.Logger) {}

func (i *InputPluginStubFailing) Cleanup() error { return nil }

func (i *InputPluginStubFailing) Extract(
	ctx context.Context,
	messages chan []GallonRecord,
	errs chan error,
) error {
	for _, page := range i.data {
		select {
		case <-ctx.Done():
			return nil
		case messages <- page:
		}
	}

	return i.extractErr
}
