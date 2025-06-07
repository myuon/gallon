package gallon

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"gopkg.in/yaml.v3"
)

type InputPluginDynamoDb struct {
	logger        *slog.Logger
	client        *dynamodb.Client
	tableName     string
	serialize     func(map[string]types.AttributeValue) (GallonRecord, error)
	totalSegments int32
}

func NewInputPluginDynamoDb(
	client *dynamodb.Client,
	tableName string,
	serialize func(map[string]types.AttributeValue) (GallonRecord, error),
	totalSegments int32,
) *InputPluginDynamoDb {
	if totalSegments <= 0 {
		totalSegments = 1 // Default to single segment if not specified
	}
	return &InputPluginDynamoDb{
		client:        client,
		tableName:     tableName,
		serialize:     serialize,
		totalSegments: totalSegments,
	}
}

var _ InputPlugin = &InputPluginDynamoDb{}

func (p *InputPluginDynamoDb) ReplaceLogger(logger *slog.Logger) {
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
	if p.totalSegments == 1 {
		return p.extractSingleSegment(ctx, messages, errs)
	}
	return p.extractParallelSegments(ctx, messages, errs)
}

func (p *InputPluginDynamoDb) extractSingleSegment(
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
				ctx,
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

type segmentResult struct {
	segment int32
	records []GallonRecord
	err     error
}

func (p *InputPluginDynamoDb) extractParallelSegments(
	ctx context.Context,
	messages chan []GallonRecord,
	errs chan error,
) error {
	p.logger.Info(fmt.Sprintf("starting parallel scan with %d segments", p.totalSegments))

	// Channel to collect results from all segments
	segmentResults := make(chan segmentResult, p.totalSegments*10)
	var wg sync.WaitGroup

	// Start one goroutine per segment
	for segment := int32(0); segment < p.totalSegments; segment++ {
		wg.Add(1)
		go func(segmentID int32) {
			defer wg.Done()
			p.scanSegment(ctx, segmentID, segmentResults)
		}(segment)
	}

	// Close results channel when all segments are done
	go func() {
		wg.Wait()
		close(segmentResults)
	}()

	// Process results as they arrive
	extractedTotal := 0
	for result := range segmentResults {
		if result.err != nil {
			errs <- result.err
			continue
		}

		if result.records != nil && len(result.records) > 0 {
			messages <- result.records
			extractedTotal += len(result.records)
			p.logger.Info(fmt.Sprintf("extracted %d records (segment %d)", extractedTotal, result.segment))
		}
	}

	return nil
}

func (p *InputPluginDynamoDb) scanSegment(
	ctx context.Context,
	segment int32,
	results chan<- segmentResult,
) {
	hasNext := true
	lastEvaluatedKey := map[string]types.AttributeValue(nil)

	for hasNext {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := p.client.Scan(
				ctx,
				&dynamodb.ScanInput{
					TableName:         aws.String(p.tableName),
					ExclusiveStartKey: lastEvaluatedKey,
					Limit:             aws.Int32(100),
					TotalSegments:     aws.Int32(p.totalSegments),
					Segment:           aws.Int32(segment),
				},
			)
			if err != nil {
				results <- segmentResult{
					segment: segment,
					err:     fmt.Errorf("failed to scan segment %d of table %s: %w", segment, p.tableName, err),
				}
				return
			}

			if resp.LastEvaluatedKey != nil {
				hasNext = true
				lastEvaluatedKey = resp.LastEvaluatedKey
			} else {
				hasNext = false
			}

			if len(resp.Items) > 0 {
				var msgs []GallonRecord
				for _, item := range resp.Items {
					record, err := p.serialize(item)
					if err != nil {
						results <- segmentResult{
							segment: segment,
							err:     fmt.Errorf("failed to serialize record in segment %d: %w", segment, err),
						}
						continue
					}
					msgs = append(msgs, record)
				}

				if len(msgs) > 0 {
					results <- segmentResult{
						segment: segment,
						records: msgs,
					}
				}
			}
		}
	}
}

type InputPluginDynamoDbConfig struct {
	Table         string                                           `yaml:"table"`
	Schema        map[string]InputPluginDynamoDbConfigSchemaColumn `yaml:"schema"`
	Region        string                                           `yaml:"region"`
	Endpoint      *string                                          `yaml:"endpoint"`
	TotalSegments *int32                                           `yaml:"totalSegments,omitempty"`
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

	totalSegments := int32(1)
	if dbConfig.TotalSegments != nil {
		totalSegments = *dbConfig.TotalSegments
	}

	return NewInputPluginDynamoDb(
		client,
		dbConfig.Table,
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
		totalSegments,
	), nil
}
