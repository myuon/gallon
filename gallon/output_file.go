package gallon

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"
)

type OutputPluginFile struct {
	logger      logr.Logger
	filepath    string
	deserialize func(GallonRecord) ([]byte, error)
	newWriter   func() (io.WriteCloser, error)
}

func NewOutputPluginFile(
	filepath string,
	deserialize func(GallonRecord) ([]byte, error),
	newWriter func() (io.WriteCloser, error),
) *OutputPluginFile {
	return &OutputPluginFile{
		filepath:    filepath,
		deserialize: deserialize,
		newWriter:   newWriter,
	}
}

var _ OutputPlugin = &OutputPluginFile{}

func (p *OutputPluginFile) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *OutputPluginFile) Cleanup() error {
	return nil
}

func (p *OutputPluginFile) Load(
	ctx context.Context,
	messages chan []GallonRecord,
	errs chan error,
) error {
	fs, err := p.newWriter()
	if err != nil {
		return err
	}

	defer func() {
		if err := fs.Close(); err != nil {
			p.logger.Error(err, "failed to close file", "filepath", p.filepath)
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

			for _, msg := range msgs {
				bs, err := p.deserialize(msg)
				if err != nil {
					errs <- errors.New("failed to deserialize record: " + fmt.Sprintf("%v", msg))
					continue
				}

				if _, err := fs.Write(bs); err != nil {
					return err
				}
			}

			if len(msgs) > 0 {
				loadedTotal += len(msgs)
				p.logger.Info(fmt.Sprintf("loaded %v records", loadedTotal), "filepath", p.filepath)
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
	var outConfig GallonConfig[any, OutputPluginFileConfig]
	if err := yaml.Unmarshal(configYml, &outConfig); err != nil {
		return nil, err
	}

	config := outConfig.Out

	deserializer, err := defineDeserializer(config.Format)
	if err != nil {
		return nil, err
	}

	return NewOutputPluginFile(
		config.Filepath,
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

func defineDeserializer(format string) (func(GallonRecord) ([]byte, error), error) {
	switch strings.ToLower(format) {
	case "jsonl":
		return func(i GallonRecord) ([]byte, error) {
			j, err := i.MarshalJSON()
			if err != nil {
				return nil, err
			}

			return []byte(fmt.Sprintf("%v\n", string(j))), nil
		}, nil
	case "csv":
		return func(i GallonRecord) ([]byte, error) {
			cells := []string{}
			for _, k := range i.Keys() {
				value, ok := i.Get(k)
				if !ok {
					return nil, fmt.Errorf("failed to get value for column: %v", k)
				}

				cells = append(cells, fmt.Sprintf("%v", value))
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
