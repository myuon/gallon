package gallon

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"
)

type OutputPluginStdout struct {
	logger      logr.Logger
	deserialize func(interface{}) ([]byte, error)
}

func NewOutputPluginStdout(
	deserialize func(interface{}) ([]byte, error),
) *OutputPluginStdout {
	return &OutputPluginStdout{
		deserialize: deserialize,
	}
}

var _ OutputPlugin = &OutputPluginStdout{}

func (p *OutputPluginStdout) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *OutputPluginStdout) Load(
	ctx context.Context,
	messages chan interface{},
	errs chan error,
) error {
	loadedTotal := 0

loop:
	for {
		select {
		case <-ctx.Done():
			println("done in load")
			break loop
		case msgs, ok := <-messages:
			if !ok {
				break loop
			}

			msgSlice := msgs.([]interface{})

			for _, msg := range msgSlice {
				bs, err := p.deserialize(msg)
				if err != nil {
					errs <- fmt.Errorf("failed to deserialize message: %v (error: %w)", msg, err)
					continue
				}

				p.logger.Info(string(bs))
			}

			if len(msgSlice) > 0 {
				loadedTotal += len(msgSlice)
				p.logger.Info(fmt.Sprintf("loaded %v records", loadedTotal))
			}
		}
	}

	p.logger.Info(fmt.Sprintf("loaded total: %v", loadedTotal))

	return nil
}

type OutputPluginStdoutConfig struct {
	Format string `yaml:"format"`
}

func NewOutputPluginStdoutFromConfig(configYml []byte) (*OutputPluginStdout, error) {
	var config OutputPluginStdoutConfig

	if err := yaml.Unmarshal(configYml, &config); err != nil {
		return nil, err
	}

	return NewOutputPluginStdout(
		func(msg interface{}) ([]byte, error) {
			if config.Format == "json" {
				return json.Marshal(msg)
			} else {
				return []byte(fmt.Sprintf("%v", msg)), nil
			}
		},
	), nil
}
