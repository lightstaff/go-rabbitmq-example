package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/lightstaff/go-rabbitmq-example/protocol"
	"github.com/streadway/amqp"
)

var (
	rabbitmqURL = flag.String("rabbitmqUrl", "localhost:5672", "AMQP url for both the publisher and subscriber")
)

type Protocol struct {
	Message   string
	Timestamp int64
}

func main() {
	flag.Parse()

	if *rabbitmqURL == "" {
		log.Fatalln("[ERROR] require rabbitmqUrl")
	}

	log.Println("consumer start")

	url := fmt.Sprintf("amqp://%s", *rabbitmqURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		conn, err := amqp.Dial(url)
		if err != nil {
			log.Printf("[ERROR] %s", err.Error())
			return
		}
		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			log.Printf("[ERROR] %s", err.Error())
			return
		}
		defer ch.Close()

		if err := ch.ExchangeDeclare("test", "fanout", false, true, false, false, nil); err != nil {
			log.Printf("[ERROR] %s", err.Error())
			return
		}

		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			log.Printf("[ERROR] %s", err.Error())
			return
		}

		if err := ch.QueueBind(q.Name, "", "test", false, nil); err != nil {
			log.Printf("[ERROR] %s", err.Error())
			return
		}

		msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
		if err != nil {
			log.Printf("[ERROR] %s", err.Error())
			return
		}

	CONSUMER_FOR:
		for {
			select {
			case <-ctx.Done():
				break CONSUMER_FOR
			case m := <-msgs:
				var p protocol.Protocol
				if err := json.Unmarshal(m.Body, &p); err != nil {
					log.Printf("[ERROR] %s", err.Error())
					continue CONSUMER_FOR
				}

				log.Printf("[INFO] success consumed. tag: %d, body: %v", m.DeliveryTag, &p)
			}
		}
	}()

	<-signals

	log.Println("consumer stop")
}
