package handlers

import (
	"log"
	"net/http"
)

func RabbitMq_1C(writter http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodGet || request.Method != http.MethodPost {
		log.Fatal("Method declared in request isn't supported")
	} else {
		rabbitmqClient, err := rabbitmqClient.NewClient()
		if err == nil {
			if request.Method == http.MethodGet {
				rabbitmqClient.getMessages()
			} else {
				rabbitmqClient.publishMessages(request.Body)
			}
		}
	}

}

func referenceData(writter http.ResponseWriter, request *http.Request) {

}

func StartHandlers() {
	router := mux.newRouter()
	router.HandleFunc("/rabbitmq_1C/api/queues/messages", RabbitMq_1C)
	router.HandleFunc("/rabbitmq_1C/api/queues/referenceData", referenceData)
}
