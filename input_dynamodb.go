package gallon

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
) *InputPluginDynamoDb {
	return &InputPluginDynamoDb{
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
			}

			msgs = append(msgs, record)
		}

		messages <- msgs
	}

	close(messages)

	return err
}
