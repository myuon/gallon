package gallon

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"
	"os"
	"sort"
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

func (p *OutputPluginFile) Load(ctx context.Context, messages chan interface{}) error {
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
					err = errors.Join(err, errors.New("failed to deserialize dynamodb record: "+fmt.Sprintf("%v", msg)))
				}

				if _, err := fs.Write(bs); err != nil {
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
	Header   *bool  `yaml:"header"`
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
			j, err := json.Marshal(i)
			if err != nil {
				return nil, err
			}

			return []byte(fmt.Sprintf("%v\n", j)), nil
		}, nil
	case "csv":
		return func(i interface{}) ([]byte, error) {
			m, ok := i.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("failed to convert to map[string]interface{}: %v", i)
			}

			keys := []string{}
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			cells := []string{}
			for _, k := range keys {
				cells = append(cells, fmt.Sprintf("%v", m[k]))
			}

			buf := new(bytes.Buffer)
			writer := csv.NewWriter(buf)
			if err := writer.WriteAll([][]string{cells}); err != nil {
				return nil, err
			}

			return buf.Bytes(), nil
		}, nil
	default:
		return nil, errors.New("unknown format: " + format)
	}
}
