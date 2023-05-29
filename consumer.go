package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/streadway/amqp"
)

type MessageConsumer interface {
	Consume()
}

type RabbitMQConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func NewRabbitMQConsumer(conn *amqp.Connection, channel *amqp.Channel, queue amqp.Queue) *RabbitMQConsumer {
	return &RabbitMQConsumer{
		conn:    conn,
		channel: channel,
		queue:   queue,
	}
}

type CallbackFunc func(...interface{}) error

func (c *RabbitMQConsumer) Consumer(callback CallbackFunc, args ...interface{}) {

	msgs, err := c.channel.Consume(
		c.queue.Name, // queue name
		"",           // consumer
		true,         // auto-acknowledge
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			args = append(args, d)
			callback(args...)

		}
	}()

	log.Println("Consumer started. Press CTRL+C to exit.")

	// Wait for termination signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	log.Println("Terminating...")
	c.conn.Close()
	close(forever)
}
