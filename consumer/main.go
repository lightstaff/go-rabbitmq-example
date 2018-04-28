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
	// RabbitMQのURLはパラメータで指定
	rabbitmqURL = flag.String("rabbitmqUrl", "localhost:5672", "Your RabbtMQ URL")
)

func main() {
	flag.Parse()

	if *rabbitmqURL == "" {
		log.Fatalln("[ERROR] require rabbitmqUrl")
	}

	log.Println("consumer start")

	// amqpだから・・・
	url := fmt.Sprintf("amqp://%s", *rabbitmqURL)

	// goroutineかけるので・・・
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 終了待機するので・・・
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

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

	// Queueを作って・・・
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		log.Printf("[ERROR] %s", err.Error())
		return
	}

	// QueueにExchangeをBindして・・・
	if err := ch.QueueBind(q.Name, "", "test", false, nil); err != nil {
		log.Printf("[ERROR] %s", err.Error())
		return
	}

	// Consume!!
	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		log.Printf("[ERROR] %s", err.Error())
		return
	}

	// メッセージ受付ルーチン
	go func() {
	CONSUMER_FOR:
		for {
			select {
			case <-ctx.Done():
				break CONSUMER_FOR
			case m, ok := <-msgs:
				if ok {
					// モデル化して・・・
					var p protocol.Protocol
					if err := json.Unmarshal(m.Body, &p); err != nil {
						log.Printf("[ERROR] %s", err.Error())
						continue CONSUMER_FOR
					}

					log.Printf("[INFO] success consumed. tag: %d, msg: %v", m.DeliveryTag, &p)
				}
			}
		}
	}()

	<-signals

	log.Println("consumer stop")
}
