package gallon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/go-logr/logr"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

type InputPluginRandom struct {
	logger    logr.Logger
	pageSize  int
	pageLimit int
	generate  func(int) (any, error)
}

func NewInputPluginRandom(
	pageSize int,
	pageLimit int,
	generate func(int) (any, error),
) *InputPluginRandom {
	return &InputPluginRandom{
		pageSize:  pageSize,
		pageLimit: pageLimit,
		generate:  generate,
	}
}

var _ InputPlugin = &InputPluginRandom{}

func (p *InputPluginRandom) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *InputPluginRandom) Extract(
	ctx context.Context,
	messages chan any,
	errs chan error,
) error {
	for i := 0; i < p.pageLimit; i++ {
		records := []any{}

		for j := 0; j < p.pageSize; j++ {
			record, err := p.generate(i)
			if err != nil {
				errs <- fmt.Errorf("failed to generate record: %v", err)
				continue
			}

			records = append(records, record)
		}

		if len(records) > 0 {
			messages <- records
		}
	}

	return nil
}

type InputPluginRandomConfig struct {
	PageSize  int                                            `yaml:"pageSize"`
	PageLimit int                                            `yaml:"pageLimit"`
	Schema    map[string]InputPluginRandomConfigSchemaColumn `yaml:"schema"`
}

type InputPluginRandomConfigSchemaColumn struct {
	Type   string                                         `yaml:"type"`
	Min    *int                                           `yaml:"min"`
	Max    *int                                           `yaml:"max"`
	Format *string                                        `yaml:"format"`
	Fields map[string]InputPluginRandomConfigSchemaColumn `yaml:"fields"`
}

func (c InputPluginRandomConfigSchemaColumn) generateValue(index int) (any, error) {
	switch c.Type {
	case "string":
		return gofakeit.LetterN(uint(gofakeit.Number(0, 40))), nil
	case "int":
		if c.Min != nil && c.Max != nil {
			return gofakeit.Number(*c.Min, *c.Max), nil
		}

		return gofakeit.Int32(), nil
	case "float":
		return gofakeit.Float32(), nil
	case "bool":
		return gofakeit.Bool(), nil
	case "name":
		return gofakeit.Name(), nil
	case "url":
		return gofakeit.URL(), nil
	case "email":
		return gofakeit.Email(), nil
	case "uuid":
		return gofakeit.UUID(), nil
	case "time":
		if c.Format != nil {
			if *c.Format == "rfc3339" {
				return gofakeit.Date().Format(time.RFC3339), nil
			}
		}

		return gofakeit.Date().String(), nil
	case "unixtime":
		return gofakeit.Date().Unix(), nil
	case "record":
		record := map[string]any{}
		for k, v := range c.Fields {
			value, err := v.generateValue(index)
			if err != nil {
				return nil, fmt.Errorf("failed to generate field %s: %v", k, err)
			}
			record[k] = value
		}
		return record, nil
	default:
		return nil, fmt.Errorf("unknown column type: %v", c.Type)
	}
}

func NewInputPluginRandomFromConfig(configYml []byte) (*InputPluginRandom, error) {
	var config InputPluginRandomConfig
	if err := yaml.Unmarshal(configYml, &config); err != nil {
		return nil, err
	}

	if config.PageSize == 0 {
		config.PageSize = 10
	}
	if config.PageLimit == 0 {
		config.PageLimit = 10
	}

	return NewInputPluginRandom(
		config.PageSize,
		config.PageLimit,
		func(index int) (any, error) {
			record := map[string]any{}

			for k, v := range config.Schema {
				value, err := v.generateValue(index)
				if err != nil {
					return nil, errors.Join(err, fmt.Errorf("failed to get value for column: %v", k))
				}

				record[k] = value
			}

			return record, nil
		},
	), nil
}
