package gallon

import (
	"log"
	"sync"
)

type InputPlugin interface {
	Extract(messages chan interface{}) error
}

type OutputPlugin interface {
	Load(messages chan interface{}) error
}

type Gallon struct {
	Input  InputPlugin
	Output OutputPlugin
}

func (g Gallon) Run() error {
	messages := make(chan interface{}, 1000)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Println("start extract")

		if err := g.Input.Extract(messages); err != nil {
			log.Println(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Println("start load")

		if err := g.Output.Load(messages); err != nil {
			log.Println(err)
		}
	}()

	wg.Wait()

	return nil
}
