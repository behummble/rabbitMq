package rabbitmqClient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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
	isReady         bool
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
	success    bool
	errors     []errorStruct
	properties properties
	headers    map[string]interface{}
	body       []byte
}

type properties struct {
	ContentType     string
	ContentEncoding string
	DeliveryMode    uint8
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
}

type errorStruct struct {
	code        string
	description string
}

type publishMessage struct {
	headers    map[string]interface{}
	properties properties
	message    string
}

func putAnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func NewClient(headers map[string][]string) (*RabbitMqClient, error) {
	settings, err := parseSettings(headers)
	if err != nil {
		putAnError(err, "ParseHeadersError")
		errors := []errorStruct{{code: "ParseHeadersError", description: err.Error()}}
		sendResponse(
			[]responseStruct{
				{success: false,
					errors: errors},
			},
		)
		return nil, err
	}

	client := RabbitMqClient{
		settings: settings,
		logger:   log.New(os.Stdout, "RabbitMq_log", log.LstdFlags),
		done:     make(chan bool),
		isReady:  false,
	}

	go client.connect()
	return &client, nil
}

func (client *RabbitMqClient) connect() {

	connect, err := amqp.Dial(fmt.Sprintf(
		"%s:%s",
		client.settings.host,
		client.settings.port))

	if err != nil {
		client.logger.Println(err)
	}

	client.connection = connect
	go client.changeConnection(client.connection)
	if initSuccessful := client.handleInit(); initSuccessful {
		client.isReady = true
	}
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

	if  host == ""  ||
		vhost == "" ||
		login == "" ||
		password == "" ||
		queue == "" 
	{
		return nil, errors.New("TheNecessaryHeadersAreMissing")
	}

	return &connectionSettings{host, port, login, password, vhost, timeOut, queue}, nil
}

func (client *RabbitMqClient) changeConnection(connect *amqp.Connection) {
	client.notifyConnClose = make(chan *amqp.Error, 1)
	client.connection.NotifyClose(client.notifyConnClose)
}

func (client *RabbitMqClient) changeChannel(channel *amqp.Channel) {
	client.channel = channel
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.channel.NotifyClose(client.notifyConnClose)
	client.channel.NotifyPublish(client.notifyConfirm)
}

func (client *RabbitMqClient) handleInit() bool {
	err := client.initialize()

	if err != nil {
		client.logger.Println("Failed to initialize: " + err.Error())
		errors := []errorStruct{{code: "ParseHeadersError", description: err.Error()}}
		sendResponse(
			[]responseStruct{
				{success: false,
					errors: errors},
			},
		)
		return false
	}
	select {
	case <-client.done:
		return true
	case <-client.notifyConnClose:
		client.logger.Println("Connection closed")
		return false
	case <-client.notifyChanClose:
		client.logger.Println("Channel closed")
		return false
	}
}

func (client *RabbitMqClient) initialize() error {
	ch, err := client.connection.Channel()
	if err != nil {
		return err
	}
	err = ch.Confirm(false)
	if err != nil {
		return err
	}
	_, err = ch.QueueDeclare(
		client.settings.queueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	client.changeChannel(ch)

	return nil
}

func (client *RabbitMqClient) getMessages() {
	//TODO :
	timeOut, _ := strconv.ParseInt(client.settings.timeOut, 10, 32)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeOut))
	defer cancel()
	if client.isReady {
		deliveries, err := client.Consume()
		if err != nil {
			client.logger.Println("Failed to start consume: " + err.Error())
			errors := []errorStruct{{code: "StartConsumeError", description: err.Error()}}
			sendResponse(
				[]responseStruct{
					{
						success: false,
						errors:  errors,
					},
				},
			)
		}
		chClosedCh := make(chan *amqp.Error, 1)
		client.channel.NotifyClose(chClosedCh)
		for {
			select {
			case <-ctx.Done():
				client.Close()
				return
			case amqErr := <-chClosedCh:
				client.logger.Println(amqErr)
			case delivery := <-deliveries:
				properties := getProperties(&delivery)
				isAck := sendResponse(
					[]responseStruct{
						{
							success:    true,
							properties: properties,
							headers:    delivery.Headers,
							body:       delivery.Body,
						},
					},
				)
				if isAck {
					if err := delivery.Ack(false); err != nil {
						client.logger.Println(err)
					}
				} else {
					if err := delivery.Reject(true); err != nil {
						client.logger.Println(err)
					}
				}
			}
		}
	}
}

func (client *RabbitMqClient) Consume() (<-chan amqp.Delivery, error) {
	if !client.isReady {
		return nil, errors.New("ClientIsNotReady")
	}

	if err := client.channel.Qos(
		1,
		0,
		false,
	); err != nil {
		return nil, err
	}

	return client.channel.Consume(
		client.settings.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

func (client *RabbitMqClient) publishMessages(body io.ReadCloser) {
	//TODO
	msgs := make(chan publishMessage)
	defer close(msgs)

	go parseMessages(body, msgs)
	if err != nil {
		return
	}
	timeOut, _ := strconv.ParseInt(client.settings.timeOut, 10, 32)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeOut) + time.Second *5)
	defer cancel()
	if client.isReady {
		for {
			select {
			case msg := <-msgs:

			case <-ctx.Done():
			}
		}
	}
}

func parseMessages(data io.ReadCloser, msgs chan publishMessage) {
	body, err := io.ReadAll(data)
	if err != nil {
		putAnError(err, "ExcecuteMessageError")
		return err
	}
	var publishMessages []publishMessage
	json.Unmarshal(body, &publishMessages)
	for _, msg := range publishMessages {
		msgs <- msg
	}
	return nil
}

func (RabbitMqClient *RabbitMqClient) getConsumersSum() (int, error) {
	return 0, nil
}

func (RabbitMqClient *RabbitMqClient) getMessagesSum() (int, error) {
	return 0, nil
}

func (client *RabbitMqClient) Close() {
	close(client.done)
	client.channel.Close()
	client.connection.Close()
}

func sendResponse(data []responseStruct) bool {
	jsonResponse, err := json.Marshal(data)
	if err != nil {
		putAnError(err, "ConvertToJsonError")
		return false
	}
	resp, err := http.Post("", "application/json", bytes.NewBuffer(jsonResponse))

	if err != nil {
		putAnError(err, "ResponseSendingError")
		return false
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		answer, _ := io.ReadAll(resp.Body)
		putAnError(errors.New(string(answer)), "ResponseAnswerError")
		return false
	}
	return true
}

func getProperties(delivery *amqp.Delivery) properties {
	return properties{
		ContentType:     delivery.ContentType,
		ContentEncoding: delivery.ContentEncoding,
		DeliveryMode:    delivery.DeliveryMode,
		Priority:        delivery.Priority,
		CorrelationId:   delivery.CorrelationId,
		ReplyTo:         delivery.ReplyTo,
		Expiration:      delivery.Expiration,
		MessageId:       delivery.MessageId,
		Timestamp:       delivery.Timestamp,
		Type:            delivery.Type,
		UserId:          delivery.UserId,
		AppId:           delivery.AppId,
	}
}
