package handlers

import (
	"log"
	"net/http"
)

func RabbitMq_1C(writter http.ResponseWriter, request *http.Request) {

	/*_,foundH := header["Host"]; _,foundL := header["Login"];
	_,foundP := header["Password"]; _,foundPt := header["Port"];
	if foundH && foundL && foundP && foundPt {
		settings := ConnectionSettings{header[]}
	}

	switch request.Method {
	case "GET":
		dsds
	case "POST":

	} */

	if request.Method != "GET" || request.Method != "POST" {
		log.Fatal("Method declared in request isn't supported")
	}

}

func ListConsumers(writter http.ResponseWriter, request *http.Request) {

}

func StartHandlers() {

	router := mux.newRouter()
	router.HandleFunc("/rabbitmq_1C/api/messages", RabbitMq_1C)
	router.HandleFunc("/rabbitmq_1C/api/listConsumers", ListConsumers)
}
