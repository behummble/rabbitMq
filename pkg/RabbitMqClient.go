package rabbitmqClient

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMqClient struct {
	connection    *amqp.Connection
	settings      ConnectionSettings
	ConsumerCount int
	Vhost         string
}

type ConnectionSettings struct {
	Host     string
	Port     int
	Login    string
	Password string
}

func putAnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func (RabbitMqClient *RabbitMqClient) Connect() {
	connect, err := amqp.Dial(RabbitMqClient.settings.Host)
	putAnError(err, "Failed to connect")

	RabbitMqClient.connection = connect
}

func (RabbitMqClient *RabbitMqClient) getMessages() {
	//TODO :
}

func (RabbitMqClient *RabbitMqClient) publishMessages() {
	//TODO
}
