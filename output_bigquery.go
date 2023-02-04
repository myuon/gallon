package gallon

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/google/uuid"
)

type OutputPluginBigQuery struct {
	client *bigquery.Client
}

func NewOutputPluginBigQuery(client *bigquery.Client) *OutputPluginBigQuery {
	return &OutputPluginBigQuery{
		client: client,
	}
}

func (p *OutputPluginBigQuery) Load(
	messages chan interface{},
	datasetId string,
	tableId string,
	schema bigquery.Schema,
) error {
	inserter := p.client.Dataset(datasetId).Table(tableId).Inserter()

	for {
		select {
		case msgs, ok := <-messages:
			if !ok {
				return nil
			}

			msgSlice := msgs.([]interface{})

			saver := []*bigquery.ValuesSaver{}
			for _, msg := range msgSlice {
				values := []bigquery.Value{}
				for _, v := range schema {
					values = append(values, msg.(map[string]interface{})[v.Name])
				}

				saver = append(saver, &bigquery.ValuesSaver{
					Schema:   schema,
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
}
