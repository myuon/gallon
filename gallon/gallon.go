package gallon

import (
	"context"
	"errors"
	"github.com/go-logr/logr"
)

type InputPlugin interface {
	ReplaceLogger(logr.Logger)
	Extract(ctx context.Context, messages chan interface{}, errs chan error) error
}

type OutputPlugin interface {
	ReplaceLogger(logr.Logger)
	Load(ctx context.Context, messages chan interface{}, errs chan error) error
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

	messages := make(chan interface{})

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

type WaitGroupChan struct {
	counter int
	waiter  chan struct{}
}

var ErrTooManyErrors = errors.New("too many errors")
