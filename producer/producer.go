package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/bitly/go-nsq"
)

//Request request
type Request struct {
	Name   string
	Number int
}

func main() {
	publisher()
}

func publisher() {
	// defer wg.Done()

	config := nsq.NewConfig()
	p, err := nsq.NewProducer("127.0.0.1:4150", config)
	if err != nil {
		log.Panic("NSQ not running")
	}

	emp := &Request{Name: "Rocky", Number: 5454}

	dataRequest, err := json.Marshal(emp)
	if err != nil {
		log.Fatal("cannot json marshal")
	}

	fmt.Println(string(dataRequest))

	err = p.Publish("write_test", []byte(dataRequest))
	if err != nil {
		log.Panic("Could not connect")
	}

	p.Stop()
}
