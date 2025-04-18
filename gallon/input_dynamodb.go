package gallon

import (
	"context"
	"fmt"

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
	serialize func(map[string]types.AttributeValue) (any, error)
}

func NewInputPluginDynamoDb(
	client *dynamodb.Client,
	tableName string,
	serialize func(map[string]types.AttributeValue) (any, error),
) *InputPluginDynamoDb {
	return &InputPluginDynamoDb{
		client:    client,
		tableName: tableName,
		serialize: serialize,
	}
}

var _ InputPlugin = &InputPluginDynamoDb{}

func (p *InputPluginDynamoDb) ReplaceLogger(logger logr.Logger) {
	p.logger = logger
}

func (p *InputPluginDynamoDb) Extract(
	ctx context.Context,
	messages chan any,
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
					Limit:             aws.Int32(100),
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

			var msgs []any
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
}

type InputPluginDynamoDbConfigSchemaColumn struct {
	Type       string                                           `yaml:"type"`
	Properties map[string]InputPluginDynamoDbConfigSchemaColumn `yaml:"properties,omitempty"`
	Items      *InputPluginDynamoDbConfigSchemaColumn           `yaml:"items,omitempty"`
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
	default:
		return nil, fmt.Errorf("unsupported type: %v", c.Type)
	}
}

func NewInputPluginDynamoDbFromConfig(configYml []byte) (*InputPluginDynamoDb, error) {
	var dbConfig InputPluginDynamoDbConfig
	if err := yaml.Unmarshal(configYml, &dbConfig); err != nil {
		return nil, err
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
		func(item map[string]types.AttributeValue) (any, error) {
			record := map[string]any{}

			for k, v := range item {
				schema, ok := dbConfig.Schema[k]
				if !ok {
					continue
				}

				value, err := schema.getValue(v)
				if err != nil {
					return nil, err
				}

				record[k] = value
			}

			return record, nil
		},
	), nil
}
