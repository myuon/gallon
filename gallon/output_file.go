package gallon

import (
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

type OutputPluginFile struct {
	filepath    string
	deserialize func(interface{}) ([]byte, error)
}

func NewOutputPluginFile(
	filepath string,
	deserialize func(interface{}) ([]byte, error),
) OutputPluginFile {
	return OutputPluginFile{
		filepath:    filepath,
		deserialize: deserialize,
	}
}

var _ OutputPlugin = OutputPluginFile{}

func (p OutputPluginFile) Load(messages chan interface{}) error {
	fs, osErr := os.Create(p.filepath)
	if osErr != nil {
		return osErr
	}

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
			fmt.Printf("Loaded %d items\n", loadedTotal)
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
