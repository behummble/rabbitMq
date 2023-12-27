package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/behummble/rabbitMq/src/pkg/rabbitMqClient"
	"github.com/gorilla/mux"
)

func RabbitMq_1C(writer http.ResponseWriter, request *http.Request) {

	rabbitmqClient, err := rabbitMqClient.NewClient(request.Header)

	if err != nil {

		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadGateway)
		body, _ := json.Marshal(err)
		writer.Write(body)
		//log.Println(err)

	} else {

		if request.Method == http.MethodGet {
			msgs, errorStruct := rabbitmqClient.GetMessages()
			writer.Header().Set("Content-Type", "application/json")
			if errorStruct != nil {
				writer.WriteHeader(http.StatusBadRequest)
				body, _ := json.Marshal(errorStruct)
				writer.Write(body)
			} else {
				writer.WriteHeader(http.StatusAccepted)
				body, _ := json.Marshal(msgs)
				writer.Write(body)
			}
		} else {
			confirmedMessages, err := rabbitmqClient.PublishMessages(request.Body)
			writer.Header().Set("Content-Type", "application/json")
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
			} else {
				writer.WriteHeader(http.StatusAccepted)
				body, _ := json.Marshal(confirmedMessages)
				writer.Write(body)
			}

		}

	}
}

func referenceData(writter http.ResponseWriter, request *http.Request) {

}

func StartHandlers() {
	router := mux.NewRouter()
	router.HandleFunc("/rabbitmq_1C/api/queues/messages", RabbitMq_1C).Methods("GET", "POST")
	router.HandleFunc("/rabbitmq_1C/api/queues/referenceData", referenceData).Methods("GET")
	http.Handle("/", router)
	log.Fatal(http.ListenAndServe("localhost:9000", router))
}
