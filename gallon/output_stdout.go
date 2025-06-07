package gallon

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"gopkg.in/yaml.v3"
)

type OutputPluginStdout struct {
	logger      *slog.Logger
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

func (p *OutputPluginStdout) ReplaceLogger(logger *slog.Logger) {
	p.logger = logger
}

func (p *OutputPluginStdout) Cleanup() error {
	return nil
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
				p.logger.Info("loaded records", slog.Int("count", loadedTotal))
			}
		}
	}

	p.logger.Info("loaded total", slog.Int("count", loadedTotal))

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
