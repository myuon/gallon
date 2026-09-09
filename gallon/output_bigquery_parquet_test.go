package gallon

import (
	"bytes"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_convertValuesForParquet(t *testing.T) {
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
		{Name: "balance", Type: bigquery.FloatFieldType},
		{Name: "active", Type: bigquery.BooleanFieldType},
		{Name: "birthday", Type: bigquery.TimestampFieldType},
		{Name: "metadata", Type: bigquery.JSONFieldType},
		{Name: "address", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "city", Type: bigquery.StringFieldType},
		}},
	}

	birthday := time.Date(2024, 1, 15, 1, 30, 0, 0, time.UTC)
	row, err := convertValuesForParquet(schema, []bigquery.Value{
		"user-1",
		"30",
		1.5,
		true,
		birthday,
		map[string]any{"theme": "dark"},
		map[string]bigquery.Value{"city": "Tokyo"},
	})
	require.NoError(t, err)

	assert.Equal(t, "user-1", row["id"])
	assert.Equal(t, int64(30), row["age"])
	assert.Equal(t, 1.5, row["balance"])
	assert.Equal(t, true, row["active"])
	assert.Equal(t, birthday.UnixMicro(), row["birthday"])
	assert.JSONEq(t, `{"theme":"dark"}`, row["metadata"].(string))
	assert.Equal(t, map[string]any{"city": "Tokyo"}, row["address"])
}

func Test_parquetSchemaAndWriter(t *testing.T) {
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
	}

	parquetSchema, err := parquetSchemaFromBigQuery(schema)
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[map[string]any](
		buf,
		parquetSchema,
		parquet.Compression(&parquet.Zstd),
	)

	row, err := convertValuesForParquet(schema, []bigquery.Value{"user-1", int(30)})
	require.NoError(t, err)

	_, err = writer.Write([]map[string]any{row})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	assert.Equal(t, int64(1), file.NumRows())
	assert.Equal(t, parquet.Zstd.CompressionCodec(), file.Metadata().RowGroups[0].Columns[0].MetaData.Codec)
}
