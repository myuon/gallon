package gallon

import "log"

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

	go func() {
		if err := g.Input.Extract(messages); err != nil {
			log.Fatal(err)
		}
	}()

	if err := g.Output.Load(messages); err != nil {
		log.Fatal(err)
	}

	return nil
}
