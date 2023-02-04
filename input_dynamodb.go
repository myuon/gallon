package gallon

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
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
			log.Fatal(err)
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
				log.Fatal(err)
			}

			msgs = append(msgs, record)
		}

		messages <- msgs
	}

	close(messages)

	return nil
}
