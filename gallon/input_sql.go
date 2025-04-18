package gallon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

type InputPluginSql struct {
	logger    logr.Logger
	client    *sql.DB
	tableName string
	driver    string
	serialize func(map[string]any) (any, error)
}

func NewInputPluginSql(
	client *sql.DB,
	tableName string,
	driver string,
	serialize func(map[string]any) (any, error),
) *InputPluginSql {
	return &InputPluginSql{
		client:    client,
		tableName: tableName,
		driver:    driver,
		serialize: serialize,
	}
}

var _ InputPlugin = &InputPluginSql{}

func (p *InputPluginSql) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *InputPluginSql) Extract(
	ctx context.Context,
	messages chan any,
	errs chan error,
) error {
	hasNext := true
	page := 0

	extractedTotal := 0

	pagedQueryStatement := ""
	if p.driver == "mysql" {
		pagedQueryStatement = fmt.Sprintf(
			"SELECT * FROM %v LIMIT 100 OFFSET ?",
			p.tableName,
		)
	} else if p.driver == "postgres" {
		pagedQueryStatement = fmt.Sprintf(
			"SELECT * FROM %v LIMIT 100 OFFSET $1",
			p.tableName,
		)
	} else {
		return fmt.Errorf("unsupported driver: %v", p.driver)
	}

	query, err := p.client.Prepare(pagedQueryStatement)
	if err != nil {
		return err
	}
	defer func() {
		if err := query.Close(); err != nil {
			errs <- fmt.Errorf("failed to close sql query: %v (error: %v)", p.tableName, err)
		}
	}()

loop:
	for hasNext {
		select {
		case <-ctx.Done():
			break loop
		default:
			rows, err := query.Query(page * 100)
			if err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
			}

			cols, err := rows.Columns()
			if err != nil {
				return err
			}

			msgs := []any{}
			for rows.Next() {
				columns := make([]any, len(cols))
				columnPointers := make([]any, len(cols))
				for i := range columns {
					columnPointers[i] = &columns[i]
				}

				if err := rows.Scan(columnPointers...); err != nil {
					errs <- fmt.Errorf("failed to scan sql table: %v (error: %v)", p.tableName, err)
					continue
				}

				record := map[string]any{}
				for i, colName := range cols {
					val := columnPointers[i].(*any)
					record[colName] = *val
				}

				r, err := p.serialize(record)
				if err != nil {
					errs <- fmt.Errorf("failed to serialize sql table: %v (error: %v)", p.tableName, err)
					continue
				}

				msgs = append(msgs, r)
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
	}
	if extractedTotal == 0 {
		p.logger.Info(fmt.Sprintf("no records found in %v", p.tableName))
	}

	return nil
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

func (c InputPluginSqlConfigSchemaColumn) getValue(value any) (any, error) {
	// if value is nil, returns nil anyway
	if value == nil {
		return nil, nil
	}

	switch c.Type {
	case "string":
		v, ok := value.(string)
		if ok {
			return v, nil
		}

		// Since mysql driver returns []byte for string, we need to convert it to string
		b, ok := value.([]byte)
		if ok {
			return string(b), nil
		}

		return nil, fmt.Errorf("value is not string: %v", value)
	case "int":
		v, ok := value.(int64)
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
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("value is not bool: %v", value)
		}

		if v == 0 {
			return false, nil
		} else {
			return true, nil
		}
	case "time":
		v, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("value is not time: %v", value)
		}

		return v, nil
	case "json":
		b, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("value is not json: %v", value)
		}

		var result any
		if err := json.Unmarshal(b, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal json: %v", err)
		}

		return result, nil
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
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return NewInputPluginSql(
		db,
		dbConfig.Table,
		dbConfig.Driver,
		func(item map[string]any) (any, error) {
			record := map[string]any{}

			for k, v := range item {
				value, err := dbConfig.Schema[k].getValue(v)
				if err != nil {
					return nil, errors.Join(err, fmt.Errorf("failed to get value for column: %v", k))
				}

				record[k] = value
			}

			return record, nil
		},
	), nil
}
