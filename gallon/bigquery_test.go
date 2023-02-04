package gallon

import (
	"cloud.google.com/go/bigquery"
	"context"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"net/http"
)

func NewBigQueryLocalClient() *bigquery.Client {
	client, err := bigquery.NewClient(context.Background(), "test", option.WithEndpoint("http://localhost:9050"))
	if err != nil {
		panic(err)
	}

	return client
}

func BigQueryCheckIfTableExists(table *bigquery.Table) (bool, error) {
	_, err := table.Metadata(context.Background())
	if err != nil {
		if err, ok := err.(*googleapi.Error); ok {
			if err.Code == http.StatusNotFound {
				return false, nil
			}
		}
		return false, err
	}

	return true, nil
}

func CreateBigQueryTableIfNotExists(
	bigqueryClient *bigquery.Client,
	datasetId string,
	tableId string,
	schema bigquery.Schema,
) error {
	exists, err := BigQueryCheckIfTableExists(bigqueryClient.Dataset(datasetId).Table(tableId))
	if err != nil {
		return err
	}

	if !exists {
		if err := bigqueryClient.Dataset(datasetId).Create(context.Background(), &bigquery.DatasetMetadata{
			Location: "asia-northeast1",
		}); err != nil {
			return err
		}

		if err := bigqueryClient.Dataset(datasetId).Table(tableId).Create(context.Background(), &bigquery.TableMetadata{
			Schema: schema,
		}); err != nil {
			return err
		}
	}

	return nil
}
