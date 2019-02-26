package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"os"
)

var AMQPConnection *amqp.Connection
var AMQPChannel *amqp.Channel

func failOnError(err error, msg string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	argsWithoutProg := os.Args[1:]

	if len(argsWithoutProg) < 6 {
		fmt.Println("Missing params. Format 127.0.0.1 5672 login pass vhost query")
		os.Exit(0)
	}

	host := argsWithoutProg[0]
	port := argsWithoutProg[1]
	login := argsWithoutProg[2]
	pass := argsWithoutProg[3]
	vhost := argsWithoutProg[4]
	query := argsWithoutProg[5]

	cs := fmt.Sprintf("amqp://%s:%s@%s:%s/%s",
		login,
		pass,
		host,
		port,
		vhost)

	connection, err := amqp.Dial(cs)
	failOnError(err, "Failed to connect to RabbitMQ")
	AMQPConnection = connection

	channel, err := AMQPConnection.Channel()
	failOnError(err, "Failed to open a channel")
	AMQPChannel = channel

	fmt.Println("Start")

	f, err := os.Create(fmt.Sprintf("var/%s.%s", query, "log"))

	if err != nil {
		failOnError(err, "Failed to create log file")
	}

	f, err = os.OpenFile(fmt.Sprintf("var/%s.%s", query, "log"), os.O_APPEND|os.O_WRONLY, 0600)

	if err != nil {
		failOnError(err, "Failed to open log file")
	}

	defer f.Close()

	msgs, err := AMQPChannel.Consume(
		query,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	for msg := range msgs {
		fmt.Println(string(msg.Body))

		if _, err = f.WriteString(fmt.Sprintf("%s\r\n",string(msg.Body))); err != nil {
			failOnError(err, "Failed write to log file")
		}

		msg.Ack(false)
	}
}