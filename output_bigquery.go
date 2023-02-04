package gallon

import (
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
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
) *OutputPluginBigQuery {
	return &OutputPluginBigQuery{
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

	var err error

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
					err = errors.Join(err, errors.New("failed to deserialize dynamodb record: "+fmt.Sprintf("%v", msg)))
				}

				saver = append(saver, &bigquery.ValuesSaver{
					Schema:   p.schema,
					InsertID: uuid.New().String(),
					Row:      values,
				})
			}

			if err := inserter.Put(context.Background(), saver); err != nil {
				return err
			}

			loadedTotal += len(msgSlice)
			fmt.Println("loaded", loadedTotal, "items")
		}
	}

	if err != nil {
		return err
	}

	copier := p.client.Dataset(p.datasetId).Table(p.tableId).CopierFrom(temporaryTable)
	copier.WriteDisposition = bigquery.WriteTruncate

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

	return nil
}
