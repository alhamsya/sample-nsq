package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/bitly/go-nsq"
)

//MessageHandler struct message handler
type MessageHandler struct {
	Consumer *nsq.Consumer
}

//Response struct user
type Response struct {
	Name   string
	Number int
}

func main() {
	wg := &sync.WaitGroup{}

	wg.Add(1)

	config := nsq.NewConfig()

	consumer, err := nsq.NewConsumer("write_test", "ch", config)
	if err != nil {
		log.Fatal("NSQ not running", err)
	}

	consumer.AddHandler(&MessageHandler{Consumer: consumer})

	nsqlds := []string{"127.0.0.1:4161"}
	err = consumer.ConnectToNSQLookupds(nsqlds)
	if err != nil {
		log.Panic("Could not connect")
	}

	wg.Wait()

}

//HandleMessage handle message
func (h *MessageHandler) HandleMessage(m *nsq.Message) error {
	var response Response

	if len(m.Body) == 0 {
		return errors.New("body is blank re-enqueue message")
	}

	err := json.Unmarshal(m.Body, &response)
	if err != nil {
		nsq.Requeue(m.ID, 10*time.Second)
		return errors.New("cannot json unmarshal")
	}

	fmt.Println(response.Name)

	return nil
}
