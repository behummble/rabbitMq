package rabbitmqClient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMqClient struct {
	connection      *amqp.Connection
	settings        *connectionSettings
	channel         *amqp.Channel
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	done            chan bool
	consumerCount   int
	messageCount    int
	logger          *log.Logger
}

type connectionSettings struct {
	host      string
	port      string
	login     string
	password  string
	vhost     string
	timeOut   string
	queueName string
}

type responseStruct struct {
	success bool
	errors  []errorStruct
	msgId   string
}

type errorStruct struct {
	code        string
	description string
}

func putAnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func (client *RabbitMqClient) Connect() {

	connect, err := amqp.Dial(fmt.Sprintf(
		"%s:%s",
		client.settings.host,
		client.settings.port))

	if err != nil {
		client.logger.Println(err)
	}

	client.connection = connect
	client.changeConnection(client.connection)
}

func NewClient(headers map[string][]string) *RabbitMqClient {
	settings, err := parseSettings(headers)
	if err != nil {

	}

	client := RabbitMqClient{
		settings: settings,
		logger:   log.New(os.Stdout, "RabbitMq_log", log.LstdFlags),
		done:     make(chan bool),
	}

	return &client
}

func (client *RabbitMqClient) getMessages() {
	//TODO :
	channel, err := client.connection.Channel()
	if err != nil {
		putAnError(err, "Failed to open channel")
	}
	messages, err := channel.Consume()
}

func (client *RabbitMqClient) publishMessages() {
	//TODO
}

func declareQueue() {

}

func (RabbitMqClient *RabbitMqClient) getConsumersSum() int {

}

func (RabbitMqClient *RabbitMqClient) getMessagesSum() int {

}

func parseSettings(headers map[string][]string) (*connectionSettings, error) {
	var host, vhost, login, password, queue string
	port := "5642"
	timeOut := "20"
	for k, v := range headers {
		switch strings.ToLower(k) {
		case "host":
			host = v[0]
		case "login":
			login = v[0]
		case "password":
			password = v[0]
		case "port":
			port = v[0]
		case "vhost":
			vhost = v[0]
		case "timeOut":
			timeOut = v[0]
		case "queue":
			queue = v[0]
		}
	}

	if strings.EqualFold(host, "") ||
		strings.EqualFold(vhost, "") ||
		strings.EqualFold(login, "") ||
		strings.EqualFold(password, "") ||
		strings.EqualFold(queue, "") {
		return nil, errors.New("The necessary headers are missing")
	}

	return &connectionSettings{host, port, login, password, vhost, timeOut, queue}, nil
}

func (client *RabbitMqClient) changeConnection(connect *amqp.Connection) {
	client.notifyConnClose = make(chan *amqp.Error, 1)
	client.connection.NotifyClose(client.notifyConnClose)
}

func (client *RabbitMqClient) changeChannel(connect *amqp.Channel) {
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.channel.NotifyClose(client.notifyConnClose)
	client.channel.NotifyPublish(client.notifyConfirm)
}

func (client *RabbitMqClient) Close() {
	close(client.done)
	client.channel.Close()
	client.connection.Close()
}

func sendResponseRequest(data []responseStruct) {
	jsonResponse, err := json.Marshal(data)
	if err != nil {
		putAnError(err, "ConvertToJsonProblem")
	}
	resp, err := http.Post("", "application/json", bytes.NewBuffer(jsonResponse))

	if err != nil {
		putAnError(err, "ResponseSendingError")
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		answer, _ := io.ReadAll(resp.Body)
		putAnError(errors.New(string(answer)), "ResponseAnswerError")
	}
}
