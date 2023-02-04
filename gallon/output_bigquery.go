package gallon

import (
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"
	"log"
	"strings"
)

type OutputPluginBigQuery struct {
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
) OutputPluginBigQuery {
	return OutputPluginBigQuery{
		client:      client,
		datasetId:   datasetId,
		tableId:     tableId,
		schema:      schema,
		deserialize: deserialize,
	}
}

var _ OutputPlugin = OutputPluginBigQuery{}

func (p OutputPluginBigQuery) Load(
	messages chan interface{},
) error {
	ctx := context.Background()

	temporaryTable := p.client.Dataset(p.datasetId).Table(fmt.Sprintf("LOAD_TEMP_%s_%s", p.tableId, uuid.New().String()))
	if err := temporaryTable.Create(ctx, &bigquery.TableMetadata{
		Schema: p.schema,
	}); err != nil {
		return err
	}

	inserter := temporaryTable.Inserter()

	loadedTotal := 0

	var tracedError error

loop:
	for {
		select {
		case msgs, ok := <-messages:
			if !ok {
				break loop
			}

			msgSlice := msgs.([]interface{})

			saver := []*bigquery.ValuesSaver{}
			for _, msg := range msgSlice {
				values, err := p.deserialize(msg)
				if err != nil {
					tracedError = errors.Join(tracedError, fmt.Errorf("failed to deserialize dynamodb record: %v (error: %v)", msg, err))
				}

				saver = append(saver, &bigquery.ValuesSaver{
					Schema:   p.schema,
					InsertID: uuid.New().String(),
					Row:      values,
				})
			}

			if err := inserter.Put(context.Background(), saver); err != nil {
				tracedError = errors.Join(tracedError, fmt.Errorf("failed to insert records: %v (error: %v)", saver, err))
			}

			loadedTotal += len(msgSlice)
			log.Printf("loaded %v records\n", loadedTotal)
		}
	}

	if tracedError != nil {
		return tracedError
	}

	log.Printf("loaded into %v\n", temporaryTable.TableID)

	copier := p.client.Dataset(p.datasetId).Table(p.tableId).CopierFrom(temporaryTable)
	copier.WriteDisposition = bigquery.WriteTruncate

	log.Printf("copying from %v to %v\n", temporaryTable.TableID, p.tableId)

	job, err := copier.Run(ctx)
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

	log.Printf("copied\n")

	if err := temporaryTable.Delete(ctx); err != nil {
		return err
	}

	log.Printf("deleted temporary table %v\n", temporaryTable.TableID)

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

func NewOutputPluginBigQueryFromConfig(configYml []byte) (OutputPluginBigQuery, error) {
	var config OutputPluginBigQueryConfig
	if err := yaml.Unmarshal(configYml, &config); err != nil {
		return OutputPluginBigQuery{}, err
	}

	options := []option.ClientOption{}

	if config.Endpoint != nil {
		options = append(options, option.WithEndpoint(*config.Endpoint))
	}

	client, err := bigquery.NewClient(context.Background(), config.ProjectId, options...)
	if err != nil {
		return OutputPluginBigQuery{}, err
	}

	schema := bigquery.Schema{}
	for name, column := range config.Schema {
		t, err := getType(column.Type)
		if err != nil {
			return OutputPluginBigQuery{}, err
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
