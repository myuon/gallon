package gallon

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"sync"
)

type InputPlugin interface {
	ReplaceLogger(logr.Logger)
	Extract(ctx context.Context, messages chan interface{}) error
}

type OutputPlugin interface {
	ReplaceLogger(logr.Logger)
	Load(ctx context.Context, messages chan interface{}) error
}

type Gallon struct {
	Logger logr.Logger
	Input  InputPlugin
	Output OutputPlugin
}

func (g *Gallon) Run() error {
	g.Input.ReplaceLogger(g.Logger)
	g.Output.ReplaceLogger(g.Logger)

	messages := make(chan interface{}, 1000)
	wg := sync.WaitGroup{}
	ctx, _ := context.WithCancel(context.Background())

	var gallonError error

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(messages)

		g.Logger.Info("start extract")

		if err := g.Input.Extract(ctx, messages); err != nil {
			gallonError = errors.Join(gallonError, fmt.Errorf("failed to extract: %w", err))
			g.Logger.Error(err, "failed to extract")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		g.Logger.Info("start load")

		if err := g.Output.Load(ctx, messages); err != nil {
			gallonError = errors.Join(gallonError, fmt.Errorf("failed to load: %w", err))
			g.Logger.Error(err, "failed to load")
		}
	}()

	wg.Wait()

	return gallonError
}
