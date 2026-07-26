package main

import (
	"log"
	"net/http"
)

func main() {
	broker := NewBroker()
	go broker.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(broker, w, r)
	})

	http.HandleFunc("/api/topics", broker.handleGetTopics)
	http.HandleFunc("/api/queues", broker.handleGetQueues)

	log.Println("Discovery server starting on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}
