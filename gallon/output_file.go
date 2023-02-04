package gallon

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

type OutputPluginFile struct {
	logger      logr.Logger
	filepath    string
	deserialize func(interface{}) ([]byte, error)
}

func NewOutputPluginFile(
	filepath string,
	deserialize func(interface{}) ([]byte, error),
) *OutputPluginFile {
	return &OutputPluginFile{
		filepath:    filepath,
		deserialize: deserialize,
	}
}

var _ OutputPlugin = &OutputPluginFile{}

func (p *OutputPluginFile) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *OutputPluginFile) Load(messages chan interface{}) error {
	fs, osErr := os.Create(p.filepath)
	if osErr != nil {
		return osErr
	}

	p.logger.Info(fmt.Sprintf("created file: %v", p.filepath))

	defer func() {
		if err := fs.Close(); err != nil {
			p.logger.Error(err, "failed to close file")
		}
	}()

	loadedTotal := 0

	var err error

loop:
	for {
		select {
		case msgs, ok := <-messages:
			if !ok {
				break loop
			}

			msgSlice := msgs.([]interface{})

			for _, msg := range msgSlice {
				bs, err := p.deserialize(msg)
				if err != nil {
					err = errors.Join(err, errors.New("failed to deserialize dynamodb record: "+fmt.Sprintf("%v", msg)))
				}

				if _, err := fs.Write(bs); err != nil {
					return err
				}
				if _, err := fs.Write([]byte("\n")); err != nil {
					return err
				}
			}

			loadedTotal += len(msgSlice)
			p.logger.Info(fmt.Sprintf("loaded %v records", loadedTotal))
		}
	}

	return err
}

type OutputPluginFileConfig struct {
	Filepath string `yaml:"filepath"`
	Format   string `yaml:"format"`
}

func NewOutputPluginFileFromConfig(configYml []byte) (OutputPlugin, error) {
	config := OutputPluginFileConfig{}
	if err := yaml.Unmarshal(configYml, &config); err != nil {
		return nil, err
	}

	deserializer, err := defineDeserializer(config.Format)
	if err != nil {
		return nil, err
	}

	return NewOutputPluginFile(
		config.Filepath,
		deserializer,
	), nil
}

func defineDeserializer(format string) (func(interface{}) ([]byte, error), error) {
	switch strings.ToLower(format) {
	case "jsonl":
		return func(i interface{}) ([]byte, error) {
			return json.Marshal(i)
		}, nil
	default:
		return nil, errors.New("unknown format: " + format)
	}
}
