package gallon

import (
	"github.com/go-logr/logr"
	"sync"
)

type InputPlugin interface {
	ReplaceLogger(logr.Logger)
	Extract(messages chan interface{}) error
}

type OutputPlugin interface {
	ReplaceLogger(logr.Logger)
	Load(messages chan interface{}) error
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

	wg.Add(1)
	go func() {
		defer wg.Done()

		g.Logger.Info("start extract")

		if err := g.Input.Extract(messages); err != nil {
			g.Logger.Error(err, "failed to extract")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		g.Logger.Info("start load")

		if err := g.Output.Load(messages); err != nil {
			g.Logger.Error(err, "failed to load")
		}
	}()

	wg.Wait()

	return nil
}
