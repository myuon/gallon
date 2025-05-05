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
	deserialize func(GallonRecord) ([]byte, error)
}

func NewOutputPluginStdout(
	deserialize func(GallonRecord) ([]byte, error),
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
	messages chan []GallonRecord,
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

			for _, msg := range msgs {
				bs, err := p.deserialize(msg)
				if err != nil {
					errs <- fmt.Errorf("failed to deserialize message: %v (error: %v)", msg, err)
					continue
				}

				p.logger.Info(string(bs))
			}

			if len(msgs) > 0 {
				loadedTotal += len(msgs)
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
	var outConfig GallonConfig[any, OutputPluginStdoutConfig]
	if err := yaml.Unmarshal(configYml, &outConfig); err != nil {
		return nil, err
	}

	config := outConfig.Out

	return NewOutputPluginStdout(
		func(msg GallonRecord) ([]byte, error) {
			if config.Format == "json" {
				return json.Marshal(&msg)
			} else {
				return []byte(fmt.Sprintf("%v", msg)), nil
			}
		},
	), nil
}
