package gallon

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"gopkg.in/yaml.v3"
)

type InputPluginDynamoDb struct {
	client    *dynamodb.Client
	tableName string
	serialize func(map[string]types.AttributeValue) (interface{}, error)
}

func NewInputPluginDynamoDb(
	client *dynamodb.Client,
	tableName string,
	serialize func(map[string]types.AttributeValue) (interface{}, error),
) InputPluginDynamoDb {
	return InputPluginDynamoDb{
		client:    client,
		tableName: tableName,
		serialize: serialize,
	}
}

var _ InputPlugin = InputPluginDynamoDb{}

func (p InputPluginDynamoDb) Extract(
	messages chan interface{},
) error {
	hasNext := true
	lastEvaluatedKey := map[string]types.AttributeValue(nil)

	var err error

	for hasNext {
		resp, err := p.client.Scan(
			context.TODO(),
			&dynamodb.ScanInput{
				TableName:         aws.String(p.tableName),
				ExclusiveStartKey: lastEvaluatedKey,
				Limit:             aws.Int32(100),
			},
		)
		if err != nil {
			err = errors.Join(err, errors.New("failed to scan dynamodb table: "+p.tableName))
			break
		}

		if resp.LastEvaluatedKey != nil {
			hasNext = true
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasNext = false
		}

		var msgs []interface{}
		for _, item := range resp.Items {
			record, err := p.serialize(item)
			if err != nil {
				err = errors.Join(err, errors.New("failed to serialize dynamodb record: "+fmt.Sprintf("%v", item)))
				continue
			}

			msgs = append(msgs, record)
		}

		if len(msgs) > 0 {
			messages <- msgs
		}
	}

	close(messages)

	return err
}

type InputPluginDynamoDbConfig struct {
	TableName string                                           `yaml:"table_name"`
	Schema    map[string]InputPluginDynamoDbConfigSchemaColumn `yaml:"schema"`
	Region    string                                           `yaml:"region"`
	Endpoint  *string                                          `yaml:"endpoint"`
}

type InputPluginDynamoDbConfigSchemaColumn struct {
	Type string `yaml:"type"`
}

func NewInputPluginDynamoDbFromConfig(configYml []byte) (InputPluginDynamoDb, error) {
	var dbConfig InputPluginDynamoDbConfig
	if err := yaml.Unmarshal(configYml, &dbConfig); err != nil {
		return InputPluginDynamoDb{}, err
	}
	
	cfg := aws.Config{
		Region: dbConfig.Region,
	}

	if dbConfig.Endpoint != nil {
		cfg.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: *dbConfig.Endpoint}, nil
			})
	}

	client := dynamodb.NewFromConfig(cfg)

	return NewInputPluginDynamoDb(
		client,
		dbConfig.TableName,
		func(item map[string]types.AttributeValue) (interface{}, error) {
			record := map[string]interface{}{}

			for k, v := range item {
				value, err := getValue(dbConfig.Schema[k], v)
				if err != nil {
					return nil, err
				}

				record[k] = value
			}

			return record, nil
		},
	), nil
}

func getValue(t InputPluginDynamoDbConfigSchemaColumn, v types.AttributeValue) (interface{}, error) {
	switch t.Type {
	case "string":
		value, ok := v.(*types.AttributeValueMemberS)
		if !ok {
			return nil, errors.New("invalid type: " + t.Type + " for value: " + fmt.Sprintf("%v", v))
		}

		return value.Value, nil
	case "number":
		value, ok := v.(*types.AttributeValueMemberN)
		if !ok {
			return nil, errors.New("invalid type: " + t.Type + " for value: " + fmt.Sprintf("%v", v))
		}

		return value.Value, nil
	case "boolean":
		value, ok := v.(*types.AttributeValueMemberBOOL)
		if !ok {
			return nil, errors.New("invalid type: " + t.Type + " for value: " + fmt.Sprintf("%v", v))
		}

		return value.Value, nil
	default:
		return nil, errors.New("unknown type: " + t.Type)
	}
}
