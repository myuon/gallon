package gallon

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type InputPluginSql struct {
	logger    logr.Logger
	client    *sql.DB
	tableName string
	serialize func(map[string]interface{}) (interface{}, error)
}

func NewInputPluginSql(
	client *sql.DB,
	tableName string,
	serialize func(map[string]interface{}) (interface{}, error),
) *InputPluginSql {
	return &InputPluginSql{
		client:    client,
		tableName: tableName,
		serialize: serialize,
	}
}

var _ InputPlugin = &InputPluginSql{}

func (p *InputPluginSql) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *InputPluginSql) Extract(
	messages chan interface{},
) error {
	hasNext := true
	page := 0

	var tracedError error
	extractedTotal := 0

	query, err := p.client.Prepare(fmt.Sprintf(
		"SELECT * FROM %v LIMIT 100 OFFSET ?",
		p.tableName,
	))
	if err != nil {
		return err
	}
	defer func() {
		if err := query.Close(); err != nil {
			tracedError = errors.Join(tracedError, fmt.Errorf("failed to close sql query: %v (error: %v)", p.tableName, err))
		}
	}()

	for hasNext {
		rows, err := query.Query(page * 100)
		if err != nil {
			return err
		}

		cols, err := rows.Columns()
		if err != nil {
			return err
		}

		msgs := []interface{}{}
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}

			if err := rows.Scan(columnPointers...); err != nil {
				tracedError = errors.Join(tracedError, fmt.Errorf("failed to scan sql table: %v (error: %v)", p.tableName, err))
				continue
			}

			record := map[string]interface{}{}
			for i, colName := range cols {
				val := columnPointers[i].(*interface{})
				record[colName] = *val
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

type InputPluginSqlConfig struct {
	Table       string                                      `yaml:"table"`
	DatabaseUrl string                                      `yaml:"database_url"`
	Driver      string                                      `yaml:"driver"`
	Schema      map[string]InputPluginSqlConfigSchemaColumn `yaml:"schema"`
}

type InputPluginSqlConfigSchemaColumn struct {
	Type string `yaml:"type"`
}

func (c InputPluginSqlConfigSchemaColumn) getValue(value interface{}) (interface{}, error) {
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

func NewInputPluginSqlFromConfig(configYml []byte) (*InputPluginSql, error) {
	var dbConfig InputPluginSqlConfig
	if err := yaml.Unmarshal(configYml, &dbConfig); err != nil {
		return nil, err
	}

	db, err := sql.Open(dbConfig.Driver, dbConfig.DatabaseUrl)
	if err != nil {
		return nil, err
	}

	return NewInputPluginSql(
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