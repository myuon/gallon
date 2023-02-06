package gallon

import (
	"context"
	"encoding/json"
	"errors"
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

func (p *OutputPluginStdout) Load(ctx context.Context, messages chan interface{}) error {
	loadedTotal := 0

	var tracedErr error

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case msgs, ok := <-messages:
			if !ok {
				break loop
			}

			msgSlice := msgs.([]interface{})

			for _, msg := range msgSlice {
				bs, err := p.deserialize(msg)
				if err != nil {
					tracedErr = errors.Join(tracedErr, fmt.Errorf("failed to deserialize message: %v (error: %w)", msg, err))
				}

				p.logger.Info(string(bs))
			}

			loadedTotal += len(msgSlice)
			p.logger.Info(fmt.Sprintf("loaded %v records", loadedTotal))
		}
	}

	p.logger.Info(fmt.Sprintf("loaded total: %v", loadedTotal))

	return tracedErr
}

type OutputPluginStdoutConfig struct {
	Format string `yaml:"format"`
}

func NewOutputPluginStdoutFromConfig(configYml []byte) (OutputPlugin, error) {
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
