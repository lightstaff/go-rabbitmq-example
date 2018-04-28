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
	// RabbitMQのURLはパラメータで指定
	rabbitmqURL = flag.String("rabbitmqUrl", "localhost:5672", "Your RabbtMQ URL")
)

func main() {
	flag.Parse()

	if *rabbitmqURL == "" {
		log.Fatalln("[ERROR] require rabbitmqUrl")
	}

	log.Println("publisher start")

	// amqpだから・・・
	url := fmt.Sprintf("amqp://%s", *rabbitmqURL)

	// ダイアルして・・・
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Printf("[ERROR] %s", err.Error())
		return
	}
	defer conn.Close()

	// チャンネル開いて・・・
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("[ERROR] %s", err.Error())
		return
	}
	defer ch.Close()

	// Exchangeを作って・・・
	if err := ch.ExchangeDeclare("test", "fanout", false, true, false, false, nil); err != nil {
		log.Printf("[ERROR] %s", err.Error())
		return
	}

	// とりあえず3回・・・
	for i := 0; i < 3; i++ {
		// メッセージ作って・・・
		p := &protocol.Protocol{
			Message:   fmt.Sprintf("Hello. No%d", i),
			Timestamp: time.Now().UnixNano(),
		}

		// バイナリ化して・・・
		bytes, err := json.Marshal(p)
		if err != nil {
			log.Printf("[ERROR] %s", err.Error())
			continue
		}

		// Publish!!
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
