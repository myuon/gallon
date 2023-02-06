package gallon

import (
	"errors"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/go-logr/logr"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
	"time"
)

type InputPluginRandom struct {
	logger    logr.Logger
	pageSize  int
	pageLimit int
	generate  func(int) (interface{}, error)
}

func NewInputPluginRandom(
	pageSize int,
	pageLimit int,
	generate func(int) (interface{}, error),
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
	messages chan interface{},
) error {
	var tracedError error

	for i := 0; i < p.pageLimit; i++ {
		records := []interface{}{}

		for j := 0; j < p.pageSize; j++ {
			record, err := p.generate(i)
			if err != nil {
				tracedError = errors.Join(tracedError, fmt.Errorf("failed to generate record: %v", err))
				continue
			}

			records = append(records, record)
		}

		messages <- records
	}

	return tracedError
}

type InputPluginRandomConfig struct {
	PageSize  int                                            `yaml:"pageSize"`
	PageLimit int                                            `yaml:"pageLimit"`
	Schema    map[string]InputPluginRandomConfigSchemaColumn `yaml:"schema"`
}

type InputPluginRandomConfigSchemaColumn struct {
	Type   string  `yaml:"type"`
	Min    *int    `yaml:"min"`
	Max    *int    `yaml:"max"`
	Format *string `yaml:"format"`
}

func (c InputPluginRandomConfigSchemaColumn) generateValue(index int) (interface{}, error) {
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
		func(index int) (interface{}, error) {
			record := map[string]interface{}{}

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
