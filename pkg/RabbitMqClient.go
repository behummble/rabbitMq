package rabbitmqClient

import (
	"log"
	"context"
	"strings"
	"strconv"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMqClient struct {
	connection    *amqp.Connection
	settings      *ConnectionSettings
	channel *amqp.channel
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	done chan bool
	consumerCount int
	messageCount int
	logger *log.logger

}

type connectionSettings struct {
	host     string
	port     int
	login    string
	password string
	vhost string
	timeOut int
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

func (RabbitMqClient *RabbitMqClient) NewClient(headers map[string]string[]) {
	settings := parseSettings(headers)
}

func (RabbitMqClient *RabbitMqClient) getMessages() {
	//TODO :
	channel, err := RabbitMqClient.connection.channel()
	if err != nil {putAnError(err, "Failed to open channel")}
	messages, err := channel
}

func (RabbitMqClient *RabbitMqClient) publishMessages() {
	//TODO
}

func declareQueue() {

}

func (RabbitMqClient *RabbitMqClient) getConsumersSum() int {

}

func (RabbitMqClient *RabbitMqClient) getMessagesSum() int {

}

func parseSettings(headers map[string]string[]) *connectionSettings {
	var host, vhost, login, password string
	port := 15642
	timeOut := 20
	for k, v := range headers {
	/*	if strings.EqualFold(k, "host") {
			host = v[0]
		}
		if strings.EqualFold(k, "login") {
			login = v[0]
		}
		if strings.EqualFold(k, "password") {
			password = v[0]
		}
		if strings.EqualFold(k, "port") {
			port = strconv.ParseInt(v[0])
		} */
		switch k {
		case strings.EqualFold(k, "host"):
			host = v[0]
		case strings.EqualFold(k, "login"):
			login = v[0]
		case strings.EqualFold(k, "password"):
			password = v[0]
		case strings.EqualFold(k, "port"):
			port = strconv.ParseInt(v[0])
		case strings.EqualFold(k, "host"):
			vhost = v[0]
		case strings.EqualFold(k, "timeOut"):
			timeOut = v[0]	
		}
	}

	return &connectionSettings{host, port, login, password, vhost, timeOut}
}