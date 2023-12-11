package handlers

import (
	"log"
	"net/http"
)

func RabbitMq_1C(writter http.ResponseWriter, request *http.Request) {

	if request.Method != "GET" || request.Method != "POST" {
		log.Fatal("Method declared in request isn't supported")
	}

}

func referenceData(writter http.ResponseWriter, request *http.Request) {

}

func StartHandlers() {
	router := mux.newRouter()
	router.HandleFunc("/rabbitmq_1C/api/queues/messages", RabbitMq_1C)
	router.HandleFunc("/rabbitmq_1C/api/queues/referenceData", referenceData)
}
