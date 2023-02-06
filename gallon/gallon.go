package gallon

import (
	"context"
	"github.com/go-logr/logr"
	"sync"
)

type InputPlugin interface {
	ReplaceLogger(logr.Logger)
	Extract(ctx context.Context, messages chan interface{}, errs chan error) error
}

type OutputPlugin interface {
	ReplaceLogger(logr.Logger)
	Load(ctx context.Context, messages chan interface{}, errs chan error) error
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
	errs := make(chan error, 10)
	tooManyErrorsLimit := 50

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(messages)

		g.Logger.Info("start extract")

		if err := g.Input.Extract(ctx, messages, errs); err != nil {
			g.Logger.Error(err, "failed to extract")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		g.Logger.Info("start load")

		if err := g.Output.Load(ctx, messages, errs); err != nil {
			g.Logger.Error(err, "failed to load")
		}
	}()

	go func() {
		errorCount := 0

		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errs:
				if err != nil {
					errorCount++
					g.Logger.Error(err, "error in gallon")
				}

				if errorCount > tooManyErrorsLimit {
					g.Logger.Error(err, "too many errs, quit")
					cancel()
					return
				}
			}
		}
	}()

	wg.Wait()

	return nil
}
