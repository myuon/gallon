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
	client *dynamodb.Client
}

func NewInputPluginDynamoDb(client *dynamodb.Client) *InputPluginDynamoDb {
	return &InputPluginDynamoDb{
		client: client,
	}
}

func (p *InputPluginDynamoDb) Extract(
	messages chan interface{},
	tableName string,
	serialize func(map[string]types.AttributeValue) (interface{}, error),
) error {
	hasNext := true
	lastEvaluatedKey := map[string]types.AttributeValue(nil)

	var err error

	for hasNext {
		resp, err := p.client.Scan(
			context.TODO(),
			&dynamodb.ScanInput{
				TableName:         aws.String(tableName),
				ExclusiveStartKey: lastEvaluatedKey,
				Limit:             aws.Int32(100),
			},
		)
		if err != nil {
			err = errors.Join(err, errors.New("failed to scan dynamodb table: "+tableName))
		}

		if resp.LastEvaluatedKey != nil {
			hasNext = true
			lastEvaluatedKey = resp.LastEvaluatedKey
		} else {
			hasNext = false
		}

		var msgs []interface{}
		for _, item := range resp.Items {
			record, err := serialize(item)
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
