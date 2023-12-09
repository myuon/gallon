package gallon

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"gopkg.in/yaml.v3"
	"strings"
	"time"
)

type OutputPluginBigQuery struct {
	logger               logr.Logger
	client               *bigquery.Client
	datasetId            string
	tableId              string
	schema               bigquery.Schema
	deserialize          func(interface{}) ([]bigquery.Value, error)
	deleteTemporaryTable bool
}

func NewOutputPluginBigQuery(
	client *bigquery.Client,
	datasetId string,
	tableId string,
	schema bigquery.Schema,
	deserialize func(interface{}) ([]bigquery.Value, error),
	deleteTemporaryTable bool,
) *OutputPluginBigQuery {
	return &OutputPluginBigQuery{
		client:               client,
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
	messages chan interface{},
	errs chan error,
) error {
	projectId := p.client.Project()

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

	mwriter, err := managedwriter.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	defer mwriter.Close()

	pendingStream, err := mwriter.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectId, p.datasetId, temporaryTableId),
		WriteStream: &storagepb.WriteStream{
			Type: storagepb.WriteStream_PENDING,
		},
	})
	if err != nil {
		return err
	}

	tableSchema, err := adapt.BQSchemaToStorageTableSchema(p.schema)
	if err != nil {
		return err
	}
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(tableSchema, "gallon")
	if err != nil {
		return err
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return fmt.Errorf("failed to convert to message descriptor")
	}

	managedStream, err := mwriter.NewManagedStream(
		ctx,
		managedwriter.WithStreamName(pendingStream.GetName()),
		managedwriter.WithSchemaDescriptor(protodesc.ToDescriptorProto(messageDescriptor)),
	)
	if err != nil {
		return err
	}
	defer managedStream.Close()

	saveRecords := func(msgSlice []interface{}) {
		rows := [][]byte{}
		for _, msg := range msgSlice {
			values, err := p.deserialize(msg)
			if err != nil {
				errs <- fmt.Errorf("failed to deserialize dynamodb record: %v (error: %v)", msg, err)
				continue
			}

			mp := map[string]interface{}{}
			for i, v := range values {
				mp[p.schema[i].Name] = v
			}

			j, err := json.Marshal(&mp)
			if err != nil {
				errs <- fmt.Errorf("failed to marshal: %v (error: %v)", mp, err)
				continue
			}

			message := dynamicpb.NewMessage(messageDescriptor)

			if err := protojson.Unmarshal(j, message); err != nil {
				errs <- fmt.Errorf("failed to unmarshal: %v (error: %v)", mp, err)
				continue
			}

			bs, err := proto.Marshal(message)
			if err != nil {
				errs <- fmt.Errorf("failed to marshal: %v (error: %v)", mp, err)
				continue
			}

			rows = append(rows, bs)
		}

		if _, err := managedStream.AppendRows(ctx, rows); err != nil {
			errs <- fmt.Errorf("failed to append rows: %v (error: %v)", rows, err)
			return
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

	rowCount, err := managedStream.Finalize(ctx)
	if err != nil {
		return err
	}

	p.logger.Info(fmt.Sprintf("%v: finalized %v rows", managedStream.StreamName(), rowCount))

	resp, err := mwriter.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       managedwriter.TableParentFromStreamName(managedStream.StreamName()),
		WriteStreams: []string{managedStream.StreamName()},
	})
	if err != nil {
		return err
	}
	if len(resp.GetStreamErrors()) > 0 {
		return fmt.Errorf("failed to commit stream: %v", resp.GetStreamErrors())
	}

	p.logger.Info(fmt.Sprintf("Table data committed at %v", resp.GetCommitTime().AsTime().Format(time.RFC3339Nano)))

	query := p.client.Query(fmt.Sprintf("SELECT * FROM `%v.%v` LIMIT %v", p.datasetId, temporaryTableId, rowCount))
	query.WriteDisposition = bigquery.WriteTruncate
	query.Dst = p.client.Dataset(p.datasetId).Table(p.tableId)

	job, err := query.Run(ctx)
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
	if !status.Done() {
		return fmt.Errorf("copier job is not done: %v", status)
	}

	/* NOTE: CopierFrom is not working...?

	copier := p.client.Dataset(p.datasetId).Table(p.tableId).CopierFrom(temporaryTable)
	copier.WriteDisposition = bigquery.WriteTruncate
	*/

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

	deleteTemporaryTable := true
	if config.DeleteTemporaryTable != nil {
		deleteTemporaryTable = *config.DeleteTemporaryTable
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
		deleteTemporaryTable,
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
