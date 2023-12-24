package rabbitMqClient

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
	"time"

	"github.com/behummble/rabbitMq/src/pkg/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMqClient struct {
	connection      *amqp.Connection
	settings        *utils.ConnectionSettings
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

type responseStruct struct {
	success    bool
	errors     []ErrorStruct
	properties *utils.Properties
	headers    map[string]interface{}
	body       []byte
}

type ErrorStruct struct {
	Code        string
	Description string
}

func putAnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func NewClient(headers map[string][]string) (*RabbitMqClient, *ErrorStruct) {
	settings, err := utils.ParseSettings(headers)
	if err != nil {
		putAnError(err, "ParseHeadersError")
		return nil, &ErrorStruct{Code: "ParseHeadersError", Description: err.Error()}
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
		"amqp://%s:%s@%s:%s/%s",
		client.settings.Login,
		client.settings.Password,
		client.settings.Host,
		client.settings.Port,
		client.settings.Vhost))

	if err != nil {
		client.logger.Println(err)
	}

	client.connection = connect
	go client.changeConnection(client.connection)

	if err := client.handleInit(); err == nil {
		client.isReady = true
	}
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

func (client *RabbitMqClient) handleInit() error {
	err := client.initialize()

	if err != nil {
		client.logger.Println("Failed to initialize: " + err.Error())
		return err
	}

	select {
	case <-client.done:
		return nil
	case <-client.notifyConnClose:
		client.logger.Println("Connection with the server was closed")
		return errors.New("Connection with the server was closed")
	case <-client.notifyChanClose:
		client.logger.Println("The channel that works with messages has been closed")
		return errors.New("The channel that works with messages has been closed")
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
		client.settings.QueueName,
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

func (client *RabbitMqClient) GetMessages() *ErrorStruct {
	timeOut, _ := strconv.ParseInt(client.settings.TimeOut, 10, 32)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeOut))
	defer cancel()

	if client.isReady {
		deliveries, err := client.consume()
		if err != nil {
			client.logger.Println("Failed to start consume: " + err.Error())
			return &ErrorStruct{Code: "StartConsumeError", Description: err.Error()}
		}
		chClosedCh := make(chan *amqp.Error, 1)
		client.channel.NotifyClose(chClosedCh)
		for {
			select {
			case <-ctx.Done():
				client.Close()
				return &ErrorStruct{Code: "TimeOutError", Description: "Timeout error"}
			case amqErr := <-chClosedCh:
				client.logger.Println(amqErr)
				return &ErrorStruct{Code: "ChannelClosed", Description: amqErr.Error()}
			case delivery := <-deliveries:
				properties := utils.GetProperties(&delivery)
				isAck := sendResponse(
					responseStruct{
						success:    true,
						properties: properties,
						headers:    delivery.Headers,
						body:       delivery.Body,
					},
				)
				if isAck {
					if err := delivery.Ack(false); err != nil {
						client.logger.Println(err)
						return &ErrorStruct{Code: "MessageAckError", Description: err.Error()}
					}
				} else {
					if err := delivery.Reject(true); err != nil {
						client.logger.Println(err)
						return &ErrorStruct{Code: "MessageRejectError", Description: err.Error()}
					}
				}
			}
		}
	} else {
		return &ErrorStruct{Code: "ClientBuildError", Description: "Attemp to build RMQ-client was unsuccessful"}
	}
}

func (client *RabbitMqClient) consume() (<-chan amqp.Delivery, error) {
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
		client.settings.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

func (client *RabbitMqClient) PublishMessages(body io.ReadCloser) (*[]string, error) {
	msgs := make(chan *utils.PublishMessage)
	defer close(msgs)

	var confirmedMessages []string

	go utils.ParseMessages(body, msgs)

	timeOut, _ := strconv.ParseInt(client.settings.TimeOut, 10, 32)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeOut)+time.Second*5)
	defer cancel()

	if client.isReady {
		for {
			select {
			case msg := <-msgs:
				err := client.pushMessage(msg)
				if err != nil {
					client.logger.Println(err)
				} else {
					confirm := <-client.notifyConfirm
					if confirm.Ack {
						confirmedMessages = append(confirmedMessages, msg.Headers["messageId"].(string))
					}
				}

			case <-ctx.Done():
				break
			case <-client.done:
				break
			}
		}
	} else {
		client.logger.Println(errors.New("Attemp to build RMQ-client was unsuccessful"))
	}

	if len(confirmedMessages) == 0 {
		return nil, errors.New("Something go wrong, check the logs")
	}

	return &confirmedMessages, nil
}

func (client *RabbitMqClient) pushMessage(msg *utils.PublishMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	amqpMsg := utils.GetAmqpMsg(msg)
	return client.channel.PublishWithContext(
		ctx,
		"default",
		client.settings.QueueName,
		false,
		false,
		amqpMsg,
	)
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

func sendResponse(data ...responseStruct) bool {
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
