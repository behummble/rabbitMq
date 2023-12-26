package utils

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConnectionSettings struct {
	Host      string
	Port      string
	Login     string
	Password  string
	Vhost     string
	TimeOut   string
	QueueName string
}

type Properties struct {
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

type PublishMessage struct {
	Headers    map[string]interface{}
	properties Properties
	Message    string
}

func putAnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func ParseSettings(headers map[string][]string) (*ConnectionSettings, error) {
	var host, vhost, login, password, queue string
	port := "5642"
	timeOut := "20"

	for k, v := range headers {
		switch strings.ToLower(k) {
		case "rabbithost":
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

	if host == "" ||
		vhost == "" ||
		login == "" ||
		password == "" ||
		queue == "" {
		return nil, errors.New("TheNecessaryHeadersAreMissing")
	}

	return &ConnectionSettings{host, port, login, password, vhost, timeOut, queue}, nil
}

func GetProperties(delivery *amqp.Delivery) *Properties {
	return &Properties{
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

func ParseMessages(data io.ReadCloser, msgs chan *PublishMessage) {
	body, err := io.ReadAll(data)
	if err != nil {
		putAnError(err, "ExcecuteMessageError")
		return
	}
	var publishMessages []PublishMessage
	json.Unmarshal(body, &publishMessages)
	for _, msg := range publishMessages {
		msgs <- &msg
	}
}

func GetAmqpMsg(msg *PublishMessage) amqp.Publishing {

	return amqp.Publishing{
		Headers: msg.Headers,

		ContentType:     msg.properties.ContentType,
		ContentEncoding: msg.properties.ContentEncoding,
		DeliveryMode:    msg.properties.DeliveryMode,
		Priority:        msg.properties.Priority,
		CorrelationId:   msg.properties.CorrelationId,
		ReplyTo:         msg.properties.ReplyTo,
		Expiration:      msg.properties.Expiration,
		MessageId:       msg.properties.MessageId,
		Timestamp:       msg.properties.Timestamp,
		Type:            msg.properties.Type,
		UserId:          msg.properties.UserId,
		AppId:           msg.properties.AppId,

		Body: []byte(msg.Message),
	}
}
