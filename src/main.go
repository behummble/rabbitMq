package main

import "net/http"

type RabbitMqClient struct {
	connection *http.Client
}
