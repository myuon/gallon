package gallon

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"
)

type OutputPluginBigQuery struct {
	logger               logr.Logger
	client               *bigquery.Client
	endpoint             *string
	datasetId            string
	tableId              string
	schema               bigquery.Schema
	deserialize          func(any) ([]bigquery.Value, error)
	deleteTemporaryTable bool
}

func NewOutputPluginBigQuery(
	client *bigquery.Client,
	endpoint *string,
	datasetId string,
	tableId string,
	schema bigquery.Schema,
	deserialize func(any) ([]bigquery.Value, error),
	deleteTemporaryTable bool,
) *OutputPluginBigQuery {
	return &OutputPluginBigQuery{
		client:               client,
		endpoint:             endpoint,
		datasetId:            datasetId,
		tableId:              tableId,
		schema:               schema,
		deserialize:          deserialize,
		deleteTemporaryTable: deleteTemporaryTable,
	}
}

type bqRecordWrapper map[string]bigquery.Value

var _ bigquery.ValueSaver = bqRecordWrapper{}

func (w bqRecordWrapper) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return w, "", nil
}

var _ OutputPlugin = &OutputPluginBigQuery{}

func (p *OutputPluginBigQuery) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *OutputPluginBigQuery) waitUntilTableCreation(ctx context.Context, tableId string) error {
	timeout := time.After(300 * time.Second)
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout while waiting for table %v to be created", p.tableId)
		case <-ticker.C:
			if meta, err := p.client.Dataset(p.datasetId).Table(tableId).Metadata(ctx); err != nil {
				p.logger.Info(fmt.Sprintf("waiting for table %v to be created, %v", p.tableId, meta))
				continue
			}

			return nil
		}
	}
}

func (p *OutputPluginBigQuery) Load(
	ctx context.Context,
	messages chan any,
	errs chan error,
) error {
	temporaryTableId := fmt.Sprintf("LOAD_TEMP_%s_%s", p.tableId, uuid.New().String())
	temporaryTable := p.client.Dataset(p.datasetId).Table(temporaryTableId)
	if err := temporaryTable.Create(ctx, &bigquery.TableMetadata{
		Schema: p.schema,
	}); err != nil {
		return err
	}

	defer func() {
		if p.deleteTemporaryTable {
			if err := temporaryTable.Delete(ctx); err != nil {
				p.logger.Error(err, "failed to delete temporary table", "tableId", temporaryTable.TableID)
			} else {
				p.logger.Info("temporary table deleted", "tableId", temporaryTable.TableID)
			}
		}
	}()

	if err := p.waitUntilTableCreation(ctx, temporaryTableId); err != nil {
		return err
	}

	p.logger.Info(fmt.Sprintf("created temporary table %v", temporaryTable.TableID))

	loadedTotal := 0

	temporaryJsonlFilePath := fmt.Sprintf("%v.jsonl.gz", temporaryTableId)
	temporaryFile, err := os.CreateTemp("", temporaryJsonlFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer func() {
		if err := os.Remove(temporaryFile.Name()); err != nil {
			p.logger.Error(err, "failed to remove temporary file", "path", temporaryFile.Name())
		}
	}()

	temporaryFileGzipWriter := gzip.NewWriter(temporaryFile)
	temporaryFileWriter := json.NewEncoder(temporaryFileGzipWriter)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case msgs, ok := <-messages:
			if !ok {
				break loop
			}

			msgsSlice, ok := msgs.([]any)
			if !ok {
				errs <- fmt.Errorf("unexpected type: %T", reflect.TypeOf(msgs))
				continue
			}

			for _, msg := range msgsSlice {
				values, err := p.deserialize(msg)
				if err != nil {
					errs <- fmt.Errorf("failed to deserialize: %v, %v", msg, err)
					continue
				}

				mp := map[string]any{}
				for i, v := range p.schema {
					mp[v.Name] = values[i]
				}

				if err := temporaryFileWriter.Encode(mp); err != nil {
					errs <- fmt.Errorf("failed to write to temporary file: %v, %v", values, err)
					continue
				}
			}

			if len(msgsSlice) > 0 {
				loadedTotal += len(msgsSlice)
				p.logger.Info(fmt.Sprintf("loaded %v rows", loadedTotal))
			}
		}
	}

	if err := temporaryFileGzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %v", err)
	}

	if err := temporaryFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %v", err)
	}

	p.logger.Info(fmt.Sprintf("loading into %v", temporaryTable.TableID))

	temporaryFile, err = os.Open(temporaryFile.Name())
	if err != nil {
		return fmt.Errorf("failed to open temporary file: %v", err)
	}

	p.logger.Info(fmt.Sprintf("opened temporary file %v", temporaryFile.Name()))

	reader, err := gzip.NewReader(temporaryFile)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}

	p.logger.Info(fmt.Sprintf("created gzip reader"))

	source := bigquery.NewReaderSource(reader)
	source.SourceFormat = bigquery.JSON
	source.Schema = p.schema

	loader := temporaryTable.LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to load: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for job: %v", err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("job failed: %v (details: %v)", err, status.Errors)
	}

	p.logger.Info(fmt.Sprintf("loaded into %v", temporaryTable.TableID))

	// NOTE: CopierFrom is not supported by bigquery-emulator
	// copier := p.client.Dataset(p.datasetId).Table(p.tableId).CopierFrom(temporaryTable)

	copier := p.client.Query(fmt.Sprintf("SELECT * FROM `%v.%v`", temporaryTable.DatasetID, temporaryTable.TableID))
	copier.WriteDisposition = bigquery.WriteTruncate
	copier.Dst = p.client.Dataset(p.datasetId).Table(p.tableId)

	job, err = copier.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to copy: %v", err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for job: %v", err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("job failed: %v", err)
	}

	p.logger.Info(fmt.Sprintf("copied from %v to %v", temporaryTable.TableID, p.tableId))

	return nil
}

type OutputPluginBigQueryConfig struct {
	ProjectId            string                                            `yaml:"projectId"`
	DatasetId            string                                            `yaml:"datasetId"`
	TableId              string                                            `yaml:"tableId"`
	Endpoint             *string                                           `yaml:"endpoint"`
	Schema               map[string]OutputPluginBigQueryConfigSchemaColumn `yaml:"schema"`
	DeleteTemporaryTable *bool                                             `yaml:"deleteTemporaryTable"`
}

type OutputPluginBigQueryConfigSchemaColumn struct {
	Type   string                                            `yaml:"type"`
	Fields map[string]OutputPluginBigQueryConfigSchemaColumn `yaml:"fields,omitempty"`
}

func NewOutputPluginBigQueryFromConfig(configYml []byte) (*OutputPluginBigQuery, error) {
	var config OutputPluginBigQueryConfig
	if err := yaml.Unmarshal(configYml, &config); err != nil {
		return nil, err
	}

	options := []option.ClientOption{}

	if config.Endpoint != nil {
		options = append(options, option.WithEndpoint(*config.Endpoint))
	}

	client, err := bigquery.NewClient(context.Background(), config.ProjectId, options...)
	if err != nil {
		return nil, err
	}

	schema, err := getSchemaFromConfig(config.Schema)
	if err != nil {
		return nil, err
	}

	deleteTemporaryTable := true
	if config.DeleteTemporaryTable != nil {
		deleteTemporaryTable = *config.DeleteTemporaryTable
	}

	return NewOutputPluginBigQuery(
		client,
		config.Endpoint,
		config.DatasetId,
		config.TableId,
		schema,
		func(item any) ([]bigquery.Value, error) {
			values := []bigquery.Value{}
			for _, v := range schema {
				value := item.(map[string]any)[v.Name]
				if v.Type == bigquery.RecordFieldType {
					if value == nil {
						values = append(values, nil)
						continue
					}
					recordValue, err := deserializeRecord(value.(map[string]any), v.Schema)
					if err != nil {
						return nil, err
					}
					values = append(values, recordValue)
				} else if v.Type == bigquery.StringFieldType {
					// If the field is a string, and the value is a JSON object, we need to deserialize it
					switch value.(type) {
					case string:
						values = append(values, value)
					default:
						jsonBytes, err := json.Marshal(value)
						if err != nil {
							return nil, err
						}

						values = append(values, string(jsonBytes))
					}
				} else {
					values = append(values, value)
				}
			}
			return values, nil
		},
		deleteTemporaryTable,
	), nil
}

func deserializeRecord(data map[string]any, schema bigquery.Schema) ([]bigquery.Value, error) {
	values := []bigquery.Value{}
	for _, field := range schema {
		value := data[field.Name]
		if field.Type == bigquery.RecordFieldType {
			if value == nil {
				values = append(values, nil)
				continue
			}
			recordValue, err := deserializeRecord(value.(map[string]any), field.Schema)
			if err != nil {
				return nil, err
			}
			values = append(values, recordValue)
		} else {
			values = append(values, value)
		}
	}
	return values, nil
}

func getType(t string) (bigquery.FieldType, error) {
	switch strings.ToUpper(t) {
	case "STRING":
		return bigquery.StringFieldType, nil
	case "INTEGER":
		return bigquery.IntegerFieldType, nil
	case "FLOAT":
		return bigquery.FloatFieldType, nil
	case "BOOLEAN":
		return bigquery.BooleanFieldType, nil
	case "TIMESTAMP":
		return bigquery.TimestampFieldType, nil
	case "RECORD":
		return bigquery.RecordFieldType, nil
	case "JSON":
		return bigquery.JSONFieldType, nil
	}

	return "", errors.New("unknown type: " + t)
}

func getSchemaFromConfig(config map[string]OutputPluginBigQueryConfigSchemaColumn) (bigquery.Schema, error) {
	schema := bigquery.Schema{}
	for name, column := range config {
		t, err := getType(column.Type)
		if err != nil {
			return nil, err
		}

		field := &bigquery.FieldSchema{
			Name: name,
			Type: t,
		}

		if t == bigquery.RecordFieldType {
			if column.Fields == nil {
				return nil, fmt.Errorf("record type field %s must have fields defined", name)
			}
			subSchema, err := getSchemaFromConfig(column.Fields)
			if err != nil {
				return nil, err
			}
			field.Schema = subSchema
		}

		schema = append(schema, field)
	}

	return schema, nil
}
