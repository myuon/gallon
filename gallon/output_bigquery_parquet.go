package gallon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	parquet "github.com/parquet-go/parquet-go"
)

func (p *OutputPluginBigQuery) writeParquetLoadFile(
	ctx context.Context,
	temporaryFile *os.File,
	messages chan []GallonRecord,
	errs chan error,
) error {
	parquetSchema, err := parquetSchemaFromBigQuery(p.schema)
	if err != nil {
		return fmt.Errorf("failed to create parquet schema: %v", err)
	}

	writer := parquet.NewGenericWriter[map[string]any](
		temporaryFile,
		parquetSchema,
		parquet.Compression(&parquet.Zstd),
	)
	loadedTotal := 0

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case msgs, ok := <-messages:
			if !ok {
				break loop
			}

			rows := make([]map[string]any, 0, len(msgs))
			for _, msg := range msgs {
				values, err := p.deserialize(msg)
				if err != nil {
					errs <- fmt.Errorf("failed to deserialize: %v, %v", msg, err)
					continue
				}

				row, err := convertValuesForParquet(p.schema, values)
				if err != nil {
					errs <- fmt.Errorf("failed to convert to parquet: %v, %v", values, err)
					continue
				}

				rows = append(rows, row)
			}

			if len(rows) > 0 {
				if _, err := writer.Write(rows); err != nil {
					return fmt.Errorf("failed to write to temporary parquet file: %v", err)
				}

				loadedTotal += len(rows)
				p.logger.Info(fmt.Sprintf("loaded %v rows", loadedTotal))
			}
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %v", err)
	}

	return nil
}

func parquetSchemaFromBigQuery(schema bigquery.Schema) (*parquet.Schema, error) {
	group, err := parquetGroupFromBigQuery(schema)
	if err != nil {
		return nil, err
	}
	return parquet.NewSchema("record", group), nil
}

func parquetGroupFromBigQuery(schema bigquery.Schema) (parquet.Group, error) {
	group := parquet.Group{}
	for _, field := range schema {
		node, err := parquetNodeFromBigQueryField(field)
		if err != nil {
			return nil, err
		}
		group[field.Name] = parquet.Optional(node)
	}
	return group, nil
}

func parquetNodeFromBigQueryField(field *bigquery.FieldSchema) (parquet.Node, error) {
	switch field.Type {
	case bigquery.StringFieldType:
		return parquet.String(), nil
	case bigquery.IntegerFieldType:
		return parquet.Int(64), nil
	case bigquery.FloatFieldType:
		return parquet.Leaf(parquet.DoubleType), nil
	case bigquery.BooleanFieldType:
		return parquet.Leaf(parquet.BooleanType), nil
	case bigquery.TimestampFieldType:
		return parquet.Timestamp(parquet.Microsecond), nil
	case bigquery.JSONFieldType:
		return parquet.JSON(), nil
	case bigquery.RecordFieldType:
		return parquetGroupFromBigQuery(field.Schema)
	default:
		return nil, fmt.Errorf("unsupported bigquery type for parquet: %s", field.Type)
	}
}

func convertValuesForParquet(schema bigquery.Schema, values []bigquery.Value) (map[string]any, error) {
	if len(values) != len(schema) {
		return nil, fmt.Errorf("value count %d does not match schema length %d", len(values), len(schema))
	}

	row := make(map[string]any, len(schema))
	for i, field := range schema {
		converted, err := convertValueForParquet(field, values[i])
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %v", field.Name, err)
		}
		row[field.Name] = converted
	}
	return row, nil
}

func convertValueForParquet(field *bigquery.FieldSchema, value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch field.Type {
	case bigquery.StringFieldType, bigquery.JSONFieldType:
		return toParquetBytes(value)
	case bigquery.IntegerFieldType:
		return toInt64(value)
	case bigquery.FloatFieldType:
		return toFloat64(value)
	case bigquery.BooleanFieldType:
		b, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to bool", value)
		}
		return b, nil
	case bigquery.TimestampFieldType:
		t, err := toTime(value)
		if err != nil {
			return nil, err
		}
		return t.UTC().UnixMicro(), nil
	case bigquery.RecordFieldType:
		return convertRecordForParquet(field.Schema, value)
	default:
		return nil, fmt.Errorf("unsupported bigquery type for parquet: %s", field.Type)
	}
}

func convertRecordForParquet(schema bigquery.Schema, value any) (map[string]any, error) {
	record, ok := value.(map[string]bigquery.Value)
	if !ok {
		if m, ok := value.(map[string]any); ok {
			converted := make(map[string]any, len(schema))
			for _, field := range schema {
				fieldValue, err := convertValueForParquet(field, m[field.Name])
				if err != nil {
					return nil, err
				}
				converted[field.Name] = fieldValue
			}
			return converted, nil
		}
		return nil, fmt.Errorf("value is not a record: %T", value)
	}

	converted := make(map[string]any, len(schema))
	for _, field := range schema {
		fieldValue, err := convertValueForParquet(field, record[field.Name])
		if err != nil {
			return nil, err
		}
		converted[field.Name] = fieldValue
	}
	return converted, nil
}

func toParquetBytes(value any) (any, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return string(jsonBytes), nil
	}
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func toTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	default:
		unix, err := toInt64(value)
		if err != nil {
			return time.Time{}, fmt.Errorf("cannot convert %T to timestamp", value)
		}
		return time.Unix(unix, 0), nil
	}
}
