package gallon

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"
)

type InputPluginDynamoDb struct {
	logger    logr.Logger
	client    *dynamodb.Client
	tableName string
	pageSize  int
	serialize func(map[string]types.AttributeValue) (GallonRecord, error)
}

func NewInputPluginDynamoDb(
	client *dynamodb.Client,
	tableName string,
	pageSize int,
	serialize func(map[string]types.AttributeValue) (GallonRecord, error),
) *InputPluginDynamoDb {
	return &InputPluginDynamoDb{
		client:    client,
		tableName: tableName,
		pageSize:  pageSize,
		serialize: serialize,
	}
}

var _ InputPlugin = &InputPluginDynamoDb{}

func (p *InputPluginDynamoDb) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *InputPluginDynamoDb) Cleanup() error {
	return nil
}

func (p *InputPluginDynamoDb) Extract(
	ctx context.Context,
	messages chan []GallonRecord,
	errs chan error,
) error {
	hasNext := true
	lastEvaluatedKey := map[string]types.AttributeValue(nil)

	extractedTotal := 0

loop:
	for hasNext {
		select {
		case <-ctx.Done():
			break loop
		default:
			resp, err := p.client.Scan(
				context.TODO(),
				&dynamodb.ScanInput{
					TableName:         aws.String(p.tableName),
					ExclusiveStartKey: lastEvaluatedKey,
					Limit:             aws.Int32(int32(p.pageSize)),
				},
			)
			if err != nil {
				errs <- fmt.Errorf("failed to scan dynamodb table: %v (error: %v)", p.tableName, err)
				break
			}

			if resp.LastEvaluatedKey != nil {
				hasNext = true
				lastEvaluatedKey = resp.LastEvaluatedKey
			} else {
				hasNext = false
			}

			var msgs []GallonRecord
			for _, item := range resp.Items {
				record, err := p.serialize(item)
				if err != nil {
					errs <- fmt.Errorf("failed to serialize dynamodb record: %v (error: %w)", item, err)
					continue
				}

				msgs = append(msgs, record)
			}

			if len(msgs) > 0 {
				messages <- msgs

				extractedTotal += len(msgs)
				p.logger.Info(fmt.Sprintf("extracted %d records", extractedTotal))
			}
		}
	}

	return nil
}

type InputPluginDynamoDbConfig struct {
	Table    string                                           `yaml:"table"`
	Schema   map[string]InputPluginDynamoDbConfigSchemaColumn `yaml:"schema"`
	Region   string                                           `yaml:"region"`
	Endpoint *string                                          `yaml:"endpoint"`
	PageSize int                                              `yaml:"pageSize"`
}

type InputPluginDynamoDbConfigSchemaColumn struct {
	Type       string                                           `yaml:"type"`
	Properties map[string]InputPluginDynamoDbConfigSchemaColumn `yaml:"properties,omitempty"`
	Items      *InputPluginDynamoDbConfigSchemaColumn           `yaml:"items,omitempty"`
	Rename     *string                                          `yaml:"rename"`
}

func (c InputPluginDynamoDbConfigSchemaColumn) getValue(v types.AttributeValue) (any, error) {
	switch c.Type {
	case "string":
		value, ok := v.(*types.AttributeValueMemberS)
		if !ok {
			return nil, fmt.Errorf("invalid type: %v for value: %v", c.Type, v)
		}

		return value.Value, nil
	case "number":
		value, ok := v.(*types.AttributeValueMemberN)
		if !ok {
			return nil, fmt.Errorf("invalid type: %v for value: %v", c.Type, v)
		}

		return value.Value, nil
	case "boolean":
		value, ok := v.(*types.AttributeValueMemberBOOL)
		if !ok {
			return nil, fmt.Errorf("invalid type: %v for value: %v", c.Type, v)
		}

		return value.Value, nil
	case "object":
		value, ok := v.(*types.AttributeValueMemberM)
		if !ok {
			return nil, fmt.Errorf("invalid type: %v for value: %v", c.Type, v)
		}

		result := map[string]any{}
		for k, v := range value.Value {
			prop, ok := c.Properties[k]
			if !ok {
				continue
			}

			val, err := prop.getValue(v)
			if err != nil {
				return nil, err
			}

			result[k] = val
		}

		return result, nil
	case "array":
		value, ok := v.(*types.AttributeValueMemberL)
		if !ok {
			return nil, fmt.Errorf("invalid type: %v for value: %v", c.Type, v)
		}

		if c.Items == nil {
			return nil, fmt.Errorf("items schema is required for array type")
		}

		result := []any{}
		for _, item := range value.Value {
			val, err := c.Items.getValue(item)
			if err != nil {
				return nil, err
			}

			result = append(result, val)
		}

		return result, nil
	case "any":
		if v == nil {
			return nil, nil
		}

		switch v := v.(type) {
		case *types.AttributeValueMemberS:
			return v.Value, nil
		case *types.AttributeValueMemberN:
			return strconv.ParseFloat(v.Value, 64)
		case *types.AttributeValueMemberBOOL:
			return v.Value, nil
		case *types.AttributeValueMemberNULL:
			return nil, nil
		case *types.AttributeValueMemberM:
			result := map[string]any{}
			anySchema := InputPluginDynamoDbConfigSchemaColumn{Type: "any"}
			for k, v := range v.Value {
				val, err := anySchema.getValue(v)
				if err != nil {
					return nil, err
				}
				result[k] = val
			}
			return result, nil
		case *types.AttributeValueMemberL:
			result := []any{}
			anySchema := InputPluginDynamoDbConfigSchemaColumn{Type: "any"}
			for _, item := range v.Value {
				val, err := anySchema.getValue(item)
				if err != nil {
					return nil, err
				}
				result = append(result, val)
			}
			return result, nil
		default:
			return nil, fmt.Errorf("unsupported type: %T", v)
		}
	default:
		return nil, fmt.Errorf("unsupported type: %v", c.Type)
	}
}

func NewInputPluginDynamoDbFromConfig(configYml []byte) (*InputPluginDynamoDb, error) {
	var inConfig GallonConfig[InputPluginDynamoDbConfig, any]
	if err := yaml.Unmarshal(configYml, &inConfig); err != nil {
		return nil, err
	}

	dbConfig := inConfig.In
	if dbConfig.PageSize == 0 {
		dbConfig.PageSize = 1000
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	cfg.Region = dbConfig.Region

	if dbConfig.Endpoint != nil {
		cfg.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...any) (aws.Endpoint, error) {
				return aws.Endpoint{URL: *dbConfig.Endpoint}, nil
			})
		cfg.Credentials = credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}
	}

	client := dynamodb.NewFromConfig(cfg)

	if dbConfig.Table == "" {
		return nil, fmt.Errorf("table_name is required")
	}

	return NewInputPluginDynamoDb(
		client,
		dbConfig.Table,
		dbConfig.PageSize,
		func(item map[string]types.AttributeValue) (GallonRecord, error) {
			record := NewGallonRecord()

			for k, v := range item {
				schema, ok := dbConfig.Schema[k]
				if !ok {
					continue
				}

				value, err := schema.getValue(v)
				if err != nil {
					return GallonRecord{}, err
				}

				columnName := k
				if schema.Rename != nil {
					columnName = *schema.Rename
				}

				record.Set(columnName, value)
			}

			return record, nil
		},
	), nil
}
