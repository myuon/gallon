package gallon

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"
)

type InputPluginMySql struct {
	logger    logr.Logger
	client    *sql.DB
	tableName string
	serialize func(map[string]interface{}) (interface{}, error)
}

func NewInputPluginMySql(
	client *sql.DB,
	tableName string,
	serialize func(map[string]interface{}) (interface{}, error),
) *InputPluginMySql {
	return &InputPluginMySql{
		client:    client,
		tableName: tableName,
		serialize: serialize,
	}
}

var _ InputPlugin = &InputPluginMySql{}

func (p *InputPluginMySql) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *InputPluginMySql) Extract(
	messages chan interface{},
) error {
	hasNext := true
	page := 0

	var tracedError error
	extractedTotal := 0

	for hasNext {
		rows, err := p.client.Query(fmt.Sprintf(
			"SELECT * FROM %v LIMIT 100 OFFSET %v",
			p.tableName,
			page*100,
		))
		if err != nil {
			return err
		}

		msgs := []interface{}{}
		for rows.Next() {
			var record map[string]interface{}
			if err := rows.Scan(&record); err != nil {
				tracedError = errors.Join(tracedError, fmt.Errorf("failed to scan mysql table: %v (error: %v)", p.tableName, err))
				continue
			}

			msgs = append(msgs, record)
		}

		if len(msgs) > 0 {
			messages <- msgs
			extractedTotal += len(msgs)

			p.logger.Info(fmt.Sprintf("extracted %v records", extractedTotal))
		} else {
			hasNext = false
		}

		page++
	}

	close(messages)

	return tracedError
}

type InputPluginMySqlConfig struct {
	Table       string                                        `yaml:"table"`
	DatabaseUrl string                                        `yaml:"database_url"`
	Driver      string                                        `yaml:"driver"`
	Schema      map[string]InputPluginMySqlConfigSchemaColumn `yaml:"schema"`
}

type InputPluginMySqlConfigSchemaColumn struct {
	Type string `yaml:"type"`
}

func (c InputPluginMySqlConfigSchemaColumn) getValue(value interface{}) (interface{}, error) {
	switch c.Type {
	case "string":
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value is not string: %v", value)
		}

		return v, nil
	case "int":
		v, ok := value.(int)
		if !ok {
			return nil, fmt.Errorf("value is not int: %v", value)
		}

		return v, nil
	case "float":
		v, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("value is not float: %v", value)
		}

		return v, nil
	case "bool":
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("value is not bool: %v", value)
		}

		return v, nil
	default:
		return nil, fmt.Errorf("unknown column type: %v", c.Type)
	}
}

func NewInputPluginMySqlFromConfig(configYml []byte) (*InputPluginMySql, error) {
	var dbConfig InputPluginMySqlConfig
	if err := yaml.Unmarshal(configYml, &dbConfig); err != nil {
		return nil, err
	}

	db, err := sql.Open(dbConfig.Driver, dbConfig.DatabaseUrl)
	if err != nil {
		return nil, err
	}

	return NewInputPluginMySql(
		db,
		dbConfig.Table,
		func(item map[string]interface{}) (interface{}, error) {
			record := map[string]interface{}{}

			for k, v := range item {
				value, err := dbConfig.Schema[k].getValue(v)
				if err != nil {
					return nil, err
				}

				record[k] = value
			}

			return record, nil
		},
	), nil
}
