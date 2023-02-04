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
	schema      bigquery.Schema
	deserialize func(interface{}) ([]bigquery.Value, error)
}

func NewOutputPluginBigQuery(
	client *bigquery.Client,
	schema bigquery.Schema,
	deserialize func(interface{}) ([]bigquery.Value, error),
) *OutputPluginBigQuery {
	return &OutputPluginBigQuery{
		client:      client,
		schema:      schema,
		deserialize: deserialize,
	}
}

func (p *OutputPluginBigQuery) Load(
	messages chan interface{},
	datasetId string,
	tableId string,
) error {
	inserter := p.client.Dataset(datasetId).Table(tableId).Inserter()

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

			fmt.Println("wrote", len(msgSlice), "items")
		}
	}

	return err
}
