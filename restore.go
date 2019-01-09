package main

import (
	"bufio"
	"fmt"
	"github.com/streadway/amqp"
	"io/ioutil"
	"os"
	"strings"
	"time"
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

	f, err := os.OpenFile(fmt.Sprintf("var/%s.%s", query, "log"), os.O_RDONLY, 0600)

	if err != nil {
		failOnError(err, "Failed to open log file")
	}

	defer f.Close()

	reader := bufio.NewReader(f)
	contents, _ := ioutil.ReadAll(reader)
	lines := strings.Split(string(contents), "\r\n")

	fmt.Println(fmt.Sprintf("Read %d lines", len(lines)))

	for line := range lines {
		fmt.Println(fmt.Sprintf("%s", lines[line]))
		err = AMQPChannel.Publish(
			"",
			query,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Transient,
				ContentType:  "application/json",
				Body:         []byte(lines[line]),
				Timestamp:    time.Now(),
			})

		failOnError(err, "Failed to publish a message")
	}

	fmt.Println(fmt.Sprintf("Reatore %d lines", len(lines)))
}