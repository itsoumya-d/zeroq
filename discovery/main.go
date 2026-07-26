// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com | +91 7031648617

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
