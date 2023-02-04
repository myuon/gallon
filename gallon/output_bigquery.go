package gallon

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"
	"log"
	"os"
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

	temporaryTableId := fmt.Sprintf("LOAD_TEMP_%s_%s", p.tableId, uuid.New().String())
	temporaryTable := p.client.Dataset(p.datasetId).Table(temporaryTableId)
	if err := temporaryTable.Create(ctx, &bigquery.TableMetadata{
		Schema: p.schema,
	}); err != nil {
		return err
	}
	defer func() {
		if err := temporaryTable.Delete(ctx); err != nil {
			log.Printf("failed to delete temporary table: %v", err)
		} else {
			log.Printf("deleted temporary table %v\n", temporaryTable.TableID)
		}
	}()

	loadedTotal := 0

	temporaryCsvFilePath := fmt.Sprintf("%v.csv", temporaryTableId)
	temporaryFile, err := os.Create(temporaryCsvFilePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := temporaryFile.Close(); err != nil {
			log.Printf("failed to close temporary file: %v", err)
		}

		if err := os.Remove(temporaryFile.Name()); err != nil {
			log.Printf("failed to remove temporary file: %v", err)
		}
	}()

	temporaryFileWriter := csv.NewWriter(temporaryFile)

	var tracedError error

loop:
	for {
		select {
		case msgs, ok := <-messages:
			if !ok {
				break loop
			}

			msgSlice := msgs.([]interface{})

			for _, msg := range msgSlice {
				values, err := p.deserialize(msg)
				if err != nil {
					tracedError = errors.Join(tracedError, fmt.Errorf("failed to deserialize dynamodb record: %v (error: %v)", msg, err))
				}

				cells := []string{}
				for _, value := range values {
					cells = append(cells, fmt.Sprintf("%v", value))
				}

				if err := temporaryFileWriter.Write(cells); err != nil {
					tracedError = errors.Join(tracedError, fmt.Errorf("failed to write csv record: %v (error: %v)", cells, err))
				}
			}

			loadedTotal += len(msgSlice)
			log.Printf("loaded %v records\n", loadedTotal)
		}
	}

	if tracedError != nil {
		return tracedError
	}

	temporaryFileWriter.Flush()

	log.Printf("loading into %v\n", temporaryTable.TableID)

	temporaryFile, err = os.Open(temporaryCsvFilePath)
	if err != nil {
		return err
	}
	source := bigquery.NewReaderSource(temporaryFile)

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

	log.Printf("loaded\n")

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
