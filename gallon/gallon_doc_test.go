package gallon

import (
	"context"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

func ExampleGallon_Run() {
	// If you don't want to write a config yaml, you can use NewInputPluginDynamoDb to instantiate an input plugin.
	input, err := NewInputPluginDynamoDbFromConfig([]byte(`
type: dynamodb
table: users
schema:
	id:
	  type: string
	name:
	  type: string
	age:
	  type: number
	created_at:
	  type: number
`))
	if err != nil {
		panic(err)
	}

	output, err := NewOutputPluginFileFromConfig([]byte(`
type: file
filepath: ./output.jsonl
format: jsonl
`))
	if err != nil {
		panic(err)
	}

	g := Gallon{
		Logger: zapr.NewLogger(zap.L()),
		Input:  input,
		Output: output,
	}
	if err := g.Run(context.Background()); err != nil {
		panic(err)
	}
}
