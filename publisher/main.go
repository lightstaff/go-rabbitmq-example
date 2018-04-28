package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/lightstaff/go-rabbitmq-example/protocol"
	"github.com/streadway/amqp"
)

var (
	rabbitmqURL = flag.String("rabbitmqUrl", "localhost:5672", "AMQP url for both the publisher and subscriber")
)

func main() {
	flag.Parse()

	if *rabbitmqURL == "" {
		log.Fatalln("[ERROR] require rabbitmqUrl")
	}

	log.Println("publisher start")

	url := fmt.Sprintf("amqp://%s", *rabbitmqURL)

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

	for i := 0; i < 3; i++ {
		p := &protocol.Protocol{
			Message:   fmt.Sprintf("Hello. No%d", i),
			Timestamp: time.Now().UnixNano(),
		}

		bytes, err := json.Marshal(p)
		if err != nil {
			log.Printf("[ERROR] %s", err.Error())
			continue
		}

		if err := ch.Publish("test", "", false, false, amqp.Publishing{
			ContentType: "text/plain",
			Body:        bytes,
		}); err != nil {
			log.Printf("[ERROR] %s", err.Error())
			continue
		}

		log.Printf("[INFO] send message. msg: %v", p)
	}

	log.Println("publisher stop")
}
