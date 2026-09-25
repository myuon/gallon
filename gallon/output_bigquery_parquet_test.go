package gallon

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/go-logr/logr"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parquetTestSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
		{Name: "balance", Type: bigquery.FloatFieldType},
		{Name: "active", Type: bigquery.BooleanFieldType},
		{Name: "birthday", Type: bigquery.TimestampFieldType},
		{Name: "address", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "city", Type: bigquery.StringFieldType},
		}},
	}
}

func Test_convertValuesForParquet(t *testing.T) {
	schema := parquetTestSchema()

	birthday := time.Date(2024, 1, 15, 1, 30, 0, 0, time.UTC)
	row, err := convertValuesForParquet(schema, []bigquery.Value{
		"user-1",
		"30",
		1.5,
		true,
		birthday,
		map[string]bigquery.Value{"city": "Tokyo"},
	})
	require.NoError(t, err)

	// Leaf values are pointers: parquet-go writes a bare Go zero value into an
	// optional column as null, so the pointer is what distinguishes 0 from NULL.
	assert.Equal(t, "user-1", *row["id"].(*string))
	assert.Equal(t, int64(30), *row["age"].(*int64))
	assert.Equal(t, 1.5, *row["balance"].(*float64))
	assert.Equal(t, true, *row["active"].(*bool))
	assert.Equal(t, birthday.UnixMicro(), *row["birthday"].(*int64))

	address := row["address"].(map[string]any)
	assert.Equal(t, "Tokyo", *address["city"].(*string))
}

func Test_convertValuesForParquet_zeroValuesAreNotNull(t *testing.T) {
	schema := parquetTestSchema()

	row, err := convertValuesForParquet(schema, []bigquery.Value{
		"",
		int64(0),
		float64(0),
		false,
		time.Unix(0, 0).UTC(),
		map[string]bigquery.Value{"city": ""},
	})
	require.NoError(t, err)

	for _, name := range []string{"id", "age", "balance", "active", "birthday"} {
		assert.NotNil(t, row[name], "%s must be a non-nil pointer, not an untyped nil", name)
	}
	assert.Equal(t, "", *row["id"].(*string))
	assert.Equal(t, int64(0), *row["age"].(*int64))
	assert.Equal(t, float64(0), *row["balance"].(*float64))
	assert.Equal(t, false, *row["active"].(*bool))
	assert.Equal(t, int64(0), *row["birthday"].(*int64))
	assert.Equal(t, "", *row["address"].(map[string]any)["city"].(*string))
}

func Test_convertValuesForParquet_nullsStayNull(t *testing.T) {
	schema := parquetTestSchema()

	row, err := convertValuesForParquet(schema, []bigquery.Value{nil, nil, nil, nil, nil, nil})
	require.NoError(t, err)

	for name := range row {
		assert.Nil(t, row[name], "%s must stay nil", name)
	}
}

// Test_parquetRoundTrip is the regression test for the zero-value bug: a row of
// zero values and a row of nulls must not read back the same.
func Test_parquetRoundTrip(t *testing.T) {
	schema := parquetTestSchema()
	parquetSchema, err := parquetSchemaFromBigQuery(schema)
	require.NoError(t, err)

	zero, err := convertValuesForParquet(schema, []bigquery.Value{
		"", int64(0), float64(0), false, time.Unix(0, 0).UTC(),
		map[string]bigquery.Value{"city": ""},
	})
	require.NoError(t, err)
	set, err := convertValuesForParquet(schema, []bigquery.Value{
		"user-1", int64(30), 1.5, true, time.Unix(1, 0).UTC(),
		map[string]bigquery.Value{"city": "Tokyo"},
	})
	require.NoError(t, err)
	null, err := convertValuesForParquet(schema, []bigquery.Value{nil, nil, nil, nil, nil, nil})
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[map[string]any](
		buf,
		parquetSchema,
		parquet.Compression(&parquet.Zstd),
	)
	_, err = writer.Write([]map[string]any{zero, set, null})
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, int64(3), file.NumRows())
	assert.Equal(t, parquet.Zstd.CompressionCodec(), file.Metadata().RowGroups[0].Columns[0].MetaData.Codec)

	// Only the null row may be null in any column.
	for _, column := range file.Metadata().RowGroups[0].Columns {
		assert.Equal(t, int64(1), column.MetaData.Statistics.NullCount,
			"column %v: only the null row must be null", column.MetaData.PathInSchema)
	}

	reader := parquet.NewGenericReader[map[string]any](bytes.NewReader(buf.Bytes()), parquetSchema)
	defer reader.Close()
	rows := make([]map[string]any, 3)
	for i := range rows {
		rows[i] = map[string]any{}
	}
	n, err := reader.Read(rows)
	require.Equal(t, 3, n)
	_ = err // io.EOF is expected once the last row is read

	assert.Equal(t, "", rows[0]["id"])
	assert.Equal(t, int64(0), rows[0]["age"])
	assert.Equal(t, float64(0), rows[0]["balance"])
	assert.Equal(t, false, rows[0]["active"])
	assert.Equal(t, int64(0), rows[0]["birthday"])

	assert.Equal(t, "user-1", rows[1]["id"])
	assert.Equal(t, int64(30), rows[1]["age"])

	for _, name := range []string{"id", "age", "balance", "active", "birthday"} {
		assert.Nil(t, rows[2][name], "%s of the null row must read back as nil", name)
	}
}

func Test_parquetSchemaFromBigQuery_sortsFieldsByName(t *testing.T) {
	// parquet.Group is a map, so the physical column order is alphabetical and
	// not the YAML definition order. BigQuery matches by name, so this is safe,
	// but the test pins the behaviour we verified against real BigQuery.
	parquetSchema, err := parquetSchemaFromBigQuery(bigquery.Schema{
		{Name: "z_name", Type: bigquery.StringFieldType},
		{Name: "a_code", Type: bigquery.StringFieldType},
		{Name: "m_ref", Type: bigquery.StringFieldType},
	})
	require.NoError(t, err)

	names := []string{}
	for _, field := range parquetSchema.Fields() {
		names = append(names, field.Name())
		assert.True(t, field.Optional(), "%s must be optional", field.Name())
	}
	assert.Equal(t, []string{"a_code", "m_ref", "z_name"}, names)
}

func Test_validateParquetSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  bigquery.Schema
		wantErr string
	}{
		{
			name:   "supported types",
			schema: parquetTestSchema(),
		},
		{
			// A Parquet load job rejects a JSON field in the schema outright,
			// and with schema inference the JSON logical type reads back as BYTES.
			name:    "json is rejected",
			schema:  bigquery.Schema{{Name: "metadata", Type: bigquery.JSONFieldType}},
			wantErr: "field metadata has type JSON",
		},
		{
			name: "nested json is rejected with a path",
			schema: bigquery.Schema{
				{Name: "address", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "extra", Type: bigquery.JSONFieldType},
				}},
			},
			wantErr: "field address.extra has type JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateParquetSchema(tt.schema, "")
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func Test_NewOutputPluginBigQueryFromConfig_rejectsJSONForParquet(t *testing.T) {
	configYml := []byte(`
out:
  type: bigquery
  projectId: test
  datasetId: dataset1
  tableId: users
  format: parquet
  schema:
    id:
      type: string
    metadata:
      type: json
`)
	_, err := NewOutputPluginBigQueryFromConfig(configYml)
	assert.ErrorContains(t, err, "field metadata has type JSON")
}

func Test_toInt64(t *testing.T) {
	// The JSON path accepts whatever an input plugin produces, so this one must
	// not be narrower. input_random's `type: int` yields an int32.
	for _, value := range []any{
		int(7), int8(7), int16(7), int32(7), int64(7),
		uint(7), uint8(7), uint16(7), uint32(7), uint64(7),
		float32(7), float64(7), "7",
	} {
		got, err := toInt64(value)
		require.NoErrorf(t, err, "%T", value)
		assert.Equalf(t, int64(7), got, "%T", value)
	}

	_, err := toInt64(uint64(1) << 63)
	assert.ErrorContains(t, err, "overflows int64")

	_, err = toInt64(struct{}{})
	assert.ErrorContains(t, err, "cannot convert")
}

func Test_toFloat64(t *testing.T) {
	for _, value := range []any{
		int(2), int8(2), int16(2), int32(2), int64(2),
		uint(2), uint8(2), uint16(2), uint32(2), uint64(2),
		float32(2), float64(2), "2",
	} {
		got, err := toFloat64(value)
		require.NoErrorf(t, err, "%T", value)
		assert.Equalf(t, float64(2), got, "%T", value)
	}

	_, err := toFloat64(struct{}{})
	assert.ErrorContains(t, err, "cannot convert")
}

func Test_writeParquetLoadFile_splitsRowGroups(t *testing.T) {
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
	}
	p := NewOutputPluginBigQuery(nil, nil, "dataset1", "users", schema,
		func(record GallonRecord) ([]bigquery.Value, error) {
			id, _ := record.Get("id")
			return []bigquery.Value{id}, nil
		},
		true,
	)
	p.ReplaceLogger(logr.Discard())
	p.format = bqFormatParquet
	p.parquetMaxRowsPerRowGroup = 3

	messages := make(chan []GallonRecord, 1)
	batch := []GallonRecord{}
	for i := range 10 {
		record := NewGallonRecord()
		record.Set("id", int64(i))
		batch = append(batch, record)
	}
	messages <- batch
	close(messages)

	temporaryFile, err := os.CreateTemp(t.TempDir(), "load-*.parquet")
	require.NoError(t, err)
	t.Cleanup(func() { _ = temporaryFile.Close() })

	errs := make(chan error, 10)
	require.NoError(t, p.writeParquetLoadFile(context.Background(), temporaryFile, messages, errs))
	require.Empty(t, errs)

	stat, err := temporaryFile.Stat()
	require.NoError(t, err)
	file, err := parquet.OpenFile(temporaryFile, stat.Size())
	require.NoError(t, err)
	assert.Equal(t, int64(10), file.NumRows())
	// 10 rows at 3 rows per group: 3 + 3 + 3 + 1.
	require.Len(t, file.RowGroups(), 4)
	for i, want := range []int64{3, 3, 3, 1} {
		assert.Equal(t, want, file.RowGroups()[i].NumRows(), "row group %d", i)
	}

	parquetSchema, err := parquetSchemaFromBigQuery(schema)
	require.NoError(t, err)
	reader := parquet.NewGenericReader[map[string]any](temporaryFile, parquetSchema)
	defer reader.Close()
	rows := make([]map[string]any, 10)
	for i := range rows {
		rows[i] = map[string]any{}
	}
	n, _ := reader.Read(rows) // io.EOF is expected once the last row is read
	require.Equal(t, 10, n)
	for i, row := range rows {
		assert.Equal(t, int64(i), row["id"])
	}
}

func Test_NewOutputPluginBigQueryFromConfig_parquetMaxRowsPerRowGroup(t *testing.T) {
	baseConfig := `
out:
  type: bigquery
  projectId: test
  datasetId: dataset1
  tableId: users
  endpoint: http://localhost:9050
  schema:
    id:
      type: string
`

	tests := []struct {
		name    string
		extra   string
		want    int64
		wantErr string
	}{
		{
			name:  "defaults when omitted",
			extra: "  format: parquet\n",
			want:  defaultParquetMaxRowsPerRowGroup,
		},
		{
			name:  "overrides the default",
			extra: "  format: parquet\n  parquetMaxRowsPerRowGroup: 5000\n",
			want:  5000,
		},
		{
			name:    "zero is rejected",
			extra:   "  format: parquet\n  parquetMaxRowsPerRowGroup: 0\n",
			wantErr: "parquetMaxRowsPerRowGroup must be positive",
		},
		{
			name:    "negative is rejected",
			extra:   "  format: parquet\n  parquetMaxRowsPerRowGroup: -1\n",
			wantErr: "parquetMaxRowsPerRowGroup must be positive",
		},
		{
			name:    "json format is rejected",
			extra:   "  format: json\n  parquetMaxRowsPerRowGroup: 5000\n",
			wantErr: "only supported with format: parquet",
		},
		{
			name:    "default format is rejected",
			extra:   "  parquetMaxRowsPerRowGroup: 5000\n",
			wantErr: "only supported with format: parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewOutputPluginBigQueryFromConfig([]byte(baseConfig + tt.extra))
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { _ = p.Cleanup() })
			assert.Equal(t, tt.want, p.parquetMaxRowsPerRowGroup)
		})
	}
}
