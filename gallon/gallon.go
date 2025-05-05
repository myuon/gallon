// This package is a core library of gallon.
// It provides the interface of InputPlugin and OutputPlugin, and the struct of Gallon.
//
// The package also contains input and output plugins:
//   - input: DynamoDB, MySQL, PostgreSQL and gofakeit generator (See InputPluginRandom)
//   - output: BigQuery, Stdout, File (JSONL, CSV)
package gallon

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/go-logr/logr"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

type GallonRecord orderedmap.OrderedMap[string, any]

func NewGallonRecord() GallonRecord {
	return GallonRecord(*orderedmap.New[string, any]())
}

func (r *GallonRecord) asOrderdMap() *orderedmap.OrderedMap[string, any] {
	return (*orderedmap.OrderedMap[string, any])(r)
}

func (r *GallonRecord) Set(key string, value any) {
	r.asOrderdMap().Set(key, value)
}

func (r *GallonRecord) Get(key string) (any, bool) {
	return r.asOrderdMap().Get(key)
}

func (r *GallonRecord) Keys() []string {
	keys := []string{}
	for pair := r.asOrderdMap().Oldest(); pair != nil; pair = pair.Next() {
		keys = append(keys, pair.Key)
	}

	return keys
}

func (r *GallonRecord) Values() []any {
	values := []any{}
	for pair := r.asOrderdMap().Oldest(); pair != nil; pair = pair.Next() {
		values = append(values, pair.Value)
	}

	return values
}

var _ json.Marshaler = &GallonRecord{}

func (r *GallonRecord) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.asOrderdMap())
}

type InputPlugin interface {
	// ReplaceLogger replaces the logger of the plugin.
	// It is called in Gallon.Run() at the beginning.
	ReplaceLogger(logr.Logger)
	// Extract extracts data from the source and sends it to the messages channel.
	// If an error occurs, send it to the errs channel.
	Extract(ctx context.Context, messages chan []GallonRecord, errs chan error) error
}

type OutputPlugin interface {
	// ReplaceLogger replaces the logger of the plugin.
	// It is called in Gallon.Run() at the beginning.
	ReplaceLogger(logr.Logger)
	// Load loads data from the messages channel and sends it to the destination.
	// If an error occurs, send it to the errs channel.
	Load(ctx context.Context, messages chan []GallonRecord, errs chan error) error
}

// Gallon is a struct that runs a migration.
type Gallon struct {
	// Logger will be used for logging. For gallon-cli, zap logger (and the `logr.Logger` interface of it) is used.
	Logger logr.Logger
	Input  InputPlugin
	Output OutputPlugin
}

// Run starts goroutines for extract and load, and waits for them to finish.
//
// If too many errors are occurred, it will cancel the context and return ErrTooManyErrors.
func (g *Gallon) Run(ctx context.Context) error {
	g.Input.ReplaceLogger(g.Logger)
	g.Output.ReplaceLogger(g.Logger)

	messages := make(chan []GallonRecord)

	errs := make(chan error, 10)
	tooManyErrorsLimit := 50

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	go func(ctx context.Context) {
		defer func() {
			g.Logger.Info("end extract")

			defer close(messages)
		}()

		g.Logger.Info("start extract")

		if err := g.Input.Extract(ctx, messages, errs); err != nil {
			g.Logger.Error(err, "failed to extract")
		}
	}(ctx)

	go func(ctx context.Context) {
		defer func() {
			g.Logger.Info("end load")

			cancel(nil)
		}()

		g.Logger.Info("start load")

		if err := g.Output.Load(ctx, messages, errs); err != nil {
			g.Logger.Error(err, "failed to load")
		}
	}(ctx)

	go func() {
		errorCount := 0

		for {
			select {
			case err := <-errs:
				if err != nil {
					errorCount++
					g.Logger.Error(err, "error in gallon", "errorCount", errorCount)
				}

				if errorCount > tooManyErrorsLimit {
					cancel(ErrTooManyErrors)
					g.Logger.Error(ErrTooManyErrors, "quit", "errorCount", errorCount)
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			if context.Cause(ctx) == ErrTooManyErrors {
				return ErrTooManyErrors
			}

			return nil
		}
	}
}

var ErrTooManyErrors = errors.New("too many errors")

// GallonConfig is the schema of gallon config yaml.
// Both `in` and `out` must contain `type` field. Plugins for input/output will be chosen by `type` field
type GallonConfig[InConfig any, OutConfig any] struct {
	In  InConfig  `yaml:"in"`
	Out OutConfig `yaml:"out"`
}
