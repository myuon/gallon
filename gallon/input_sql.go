package gallon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	orderedmap "github.com/wk8/go-ordered-map/v2"
	"gopkg.in/yaml.v3"
)

type InputPluginSql struct {
	logger    logr.Logger
	client    *sql.DB
	tableName string
	driver    string
	serialize func(orderedmap.OrderedMap[string, any]) (GallonRecord, error)
}

func NewInputPluginSql(
	client *sql.DB,
	tableName string,
	driver string,
	serialize func(orderedmap.OrderedMap[string, any]) (GallonRecord, error),
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

func (p *InputPluginSql) Cleanup() error {
	return p.client.Close()
}

func (p *InputPluginSql) Extract(
	ctx context.Context,
	messages chan []GallonRecord,
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

			msgs := []GallonRecord{}
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

				record := *orderedmap.New[string, any]()
				for i, colName := range cols {
					val := columnPointers[i].(*any)
					record.Set(colName, *val)
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

func (p *InputPluginSql) CloseConnection() error {
	return p.client.Close()
}

type InputPluginSqlConfig struct {
	Table       string                                                          `yaml:"table"`
	DatabaseUrl string                                                          `yaml:"database_url"`
	Driver      string                                                          `yaml:"driver"`
	Schema      orderedmap.OrderedMap[string, InputPluginSqlConfigSchemaColumn] `yaml:"schema"`
}

type InputPluginSqlConfigSchemaColumn struct {
	Type       string                                      `yaml:"type"`
	Transforms []InputPluginSqlConfigSchemaColumnTransform `yaml:"transforms"`
	Rename     *string                                     `yaml:"rename"`
}

type InputPluginSqlConfigSchemaColumnTransform struct {
	// Operation: type conversion
	Type   string  `yaml:"type"`
	Format *string `yaml:"format"`
	As     *string `yaml:"as"`
}

func (c InputPluginSqlConfigSchemaColumnTransform) Transform(sourceType string, value any) (any, error) {
	switch sourceType {
	case "time":
		v, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("value is not time: %v", value)
		}

		if c.Type == "string" {
			if c.Format != nil {
				return v.Format(*c.Format), nil
			}

			return v.Format(time.RFC3339), nil
		}
	case "int":
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("value is not int: %v", value)
		}

		if c.Type == "time" {
			if c.As == nil || *c.As == "unix" {
				return time.Unix(v, 0), nil
			}
		}
	}

	return nil, fmt.Errorf("unsupported transform: %v -> %v", sourceType, c.Type)
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
	case "decimal":
		// MySQLのdecimal型は[]byteとして返されることがあるため、文字列に変換してからfloat64に変換
		switch v := value.(type) {
		case float64:
			return v, nil
		case []byte:
			str := string(v)
			f, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse decimal: %v", err)
			}
			return f, nil
		default:
			return nil, fmt.Errorf("value is not decimal: %v", value)
		}
	case "bool":
		switch v := value.(type) {
		case bool:
			return v, nil
		case int64:
			// tinyint(1)
			return v != 0, nil
		case []byte:
			if len(v) == 1 {
				// bit(1)
				return v[0] != 0, nil
			}
			return nil, fmt.Errorf("value is not bool: %v", value)
		default:
			return nil, fmt.Errorf("value is not bool: %v", value)
		}
	case "date":
		b, ok := value.([]byte)
		if ok {
			v, err := time.Parse("2006-01-02", string(b))
			if err != nil {
				return nil, fmt.Errorf("failed to parse date: %v", err)
			}

			return v.Format(time.DateOnly), nil
		}

		v, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("value is not date: %v", value)
		}

		return v.Format(time.DateOnly), nil
	case "time":
		// when parseTime not specified, mysql returns []byte
		b, ok := value.([]byte)
		if ok {
			v, err := time.Parse("2006-01-02 15:04:05", string(b))
			if err != nil {
				return nil, fmt.Errorf("failed to parse time: %v", err)
			}

			return v, nil
		}

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
	var inConfig GallonConfig[InputPluginSqlConfig, any]
	if err := yaml.Unmarshal(configYml, &inConfig); err != nil {
		return nil, err
	}

	dbConfig := inConfig.In

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
		func(item orderedmap.OrderedMap[string, any]) (GallonRecord, error) {
			record := NewGallonRecord()

			for pair := dbConfig.Schema.Oldest(); pair != nil; pair = pair.Next() {
				value, ok := item.Get(pair.Key)
				if !ok {
					continue
				}

				v, err := pair.Value.getValue(value)
				if err != nil {
					return GallonRecord{}, errors.Join(err, fmt.Errorf("failed to get value for column: %v", pair.Key))
				}

				sourceType := pair.Value.Type

				for _, transform := range pair.Value.Transforms {
					v, err = transform.Transform(sourceType, v)
					if err != nil {
						return GallonRecord{}, errors.Join(err, fmt.Errorf("failed to transform value for column: %v", pair.Key))
					}

					sourceType = transform.Type
				}

				columnName := pair.Key
				if pair.Value.Rename != nil {
					columnName = *pair.Value.Rename
				}

				record.Set(columnName, v)
			}

			return record, nil
		},
	), nil
}
