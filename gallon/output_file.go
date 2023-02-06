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
	"io"
	"log"
	"os"
	"sort"
	"strings"
)

type OutputPluginFile struct {
	logger      logr.Logger
	deserialize func(interface{}) ([]byte, error)
	newWriter   func() (io.WriteCloser, error)
}

func NewOutputPluginFile(
	deserialize func(interface{}) ([]byte, error),
	newWriter func() (io.WriteCloser, error),
) *OutputPluginFile {
	return &OutputPluginFile{
		deserialize: deserialize,
		newWriter:   newWriter,
	}
}

var _ OutputPlugin = &OutputPluginFile{}

func (p *OutputPluginFile) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *OutputPluginFile) Load(
	ctx context.Context,
	messages chan interface{},
	errs chan error,
) error {
	fs, err := p.newWriter()
	if err != nil {
		return err
	}

	defer func() {
		if err := fs.Close(); err != nil {
			p.logger.Error(err, "failed to close file")
		}
	}()

	loadedTotal := 0

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
					errs <- errors.New("failed to deserialize dynamodb record: " + fmt.Sprintf("%v", msg))
					continue
				}

				if _, err := fs.Write(bs); err != nil {
					return err
				}
			}

			if len(msgSlice) > 0 {
				loadedTotal += len(msgSlice)
				p.logger.Info(fmt.Sprintf("loaded %v records", loadedTotal))
			}
		}
	}

	return nil
}

type OutputPluginFileConfig struct {
	Filepath string `yaml:"filepath"`
	Format   string `yaml:"format"`
	Header   *bool  `yaml:"header"`
}

func NewOutputPluginFileFromConfig(configYml []byte) (*OutputPluginFile, error) {
	config := OutputPluginFileConfig{}
	if err := yaml.Unmarshal(configYml, &config); err != nil {
		return nil, err
	}

	deserializer, err := defineDeserializer(config.Format)
	if err != nil {
		return nil, err
	}

	return NewOutputPluginFile(
		deserializer,
		func() (io.WriteCloser, error) {
			fs, err := os.Create(config.Filepath)
			if err != nil {
				return nil, err
			}

			log.Println(fmt.Sprintf("created file: %v", config.Filepath))

			return fs, nil
		},
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

			return []byte(fmt.Sprintf("%v\n", string(j))), nil
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
