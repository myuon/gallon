package gallon

import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"
	"strings"
)

type OutputPluginBigQuery struct {
	logger      logr.Logger
	client      *bigquery.Client
	datasetId   string
	tableId     string
	schema      bigquery.Schema
	deserialize func(interface{}) ([]bigquery.Value, error)
}

func NewOutputPluginBigQuery(
	client *bigquery.Client,
	datasetId string,
	tableId string,
	schema bigquery.Schema,
	deserialize func(interface{}) ([]bigquery.Value, error),
) *OutputPluginBigQuery {
	return &OutputPluginBigQuery{
		client:      client,
		datasetId:   datasetId,
		tableId:     tableId,
		schema:      schema,
		deserialize: deserialize,
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

func (p *OutputPluginBigQuery) Load(
	ctx context.Context,
	messages chan interface{},
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
		if err := temporaryTable.Delete(ctx); err != nil {
			p.logger.Error(err, "failed to delete temporary table", "tableId", temporaryTable.TableID)
		} else {
			p.logger.Info("temporary table deleted", "tableId", temporaryTable.TableID)
		}
	}()

	loadedTotal := 0
	buffer := new(bytes.Buffer)
	temporaryTableInserter := temporaryTable.Inserter()

	saveRecords := func(msgSlice []interface{}) {
		for _, msg := range msgSlice {
			values, err := p.deserialize(msg)
			if err != nil {
				errs <- fmt.Errorf("failed to deserialize dynamodb record: %v (error: %v)", msg, err)
				continue
			}

			bqRecords := bqRecordWrapper{}
			for i, v := range values {
				bqRecords[p.schema[i].Name] = v
			}

			if err := temporaryTableInserter.Put(ctx, bqRecords); err != nil {
				errs <- fmt.Errorf("failed to insert into temporary table: %v (error: %v)", values, err)
				continue
			}
		}

		if len(msgSlice) > 0 {
			loadedTotal += len(msgSlice)
			p.logger.Info(fmt.Sprintf("loaded %v records", loadedTotal))
		}
	}

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case msgs, ok := <-messages:
			if !ok {
				break loop
			}
			saveRecords(msgs.([]interface{}))
		}
	}

	for msgs := range messages {
		saveRecords(msgs.([]interface{}))
	}

	p.logger.Info(fmt.Sprintf("loading into %v", temporaryTable.TableID))

	source := bigquery.NewReaderSource(buffer)

	loader := temporaryTable.LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteTruncate

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}

	p.logger.Info(fmt.Sprintf("loaded into %v", temporaryTable.TableID))

	copier := p.client.Dataset(p.datasetId).Table(p.tableId).CopierFrom(temporaryTable)

	copier.WriteDisposition = bigquery.WriteTruncate

	job, err = copier.Run(ctx)
	if err != nil {
		return err
	}
	status, err = job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}

	p.logger.Info(fmt.Sprintf("copied from %v to %v", temporaryTable.TableID, p.tableId))

	return nil
}

type OutputPluginBigQueryConfig struct {
	ProjectId string                                            `yaml:"projectId"`
	DatasetId string                                            `yaml:"datasetId"`
	TableId   string                                            `yaml:"tableId"`
	Endpoint  *string                                           `yaml:"endpoint"`
	Schema    map[string]OutputPluginBigQueryConfigSchemaColumn `yaml:"schema"`
}

type OutputPluginBigQueryConfigSchemaColumn struct {
	Type string `yaml:"type"`
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

	schema := bigquery.Schema{}
	for name, column := range config.Schema {
		t, err := getType(column.Type)
		if err != nil {
			return nil, err
		}

		schema = append(schema, &bigquery.FieldSchema{
			Name: name,
			Type: t,
		})
	}

	return NewOutputPluginBigQuery(
		client,
		config.DatasetId,
		config.TableId,
		schema,
		func(item interface{}) ([]bigquery.Value, error) {
			values := []bigquery.Value{}
			for _, v := range schema {
				values = append(values, item.(map[string]interface{})[v.Name])
			}

			return values, nil
		},
	), nil
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
	}

	return "", errors.New("unknown type: " + t)
}
