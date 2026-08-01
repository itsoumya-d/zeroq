// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com

package main

import (
	"encoding/json"
	"net/http"
	"sync"
)

type Broker struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	topics     map[string]bool
	queues     map[string]bool
	mu         sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		topics:     make(map[string]bool),
		queues:     make(map[string]bool),
	}
}

func (b *Broker) Run() {
	for {
		select {
		case client := <-b.register:
			b.mu.Lock()
			b.clients[client] = true
			b.mu.Unlock()
		case client := <-b.unregister:
			b.mu.Lock()
			if _, ok := b.clients[client]; ok {
				delete(b.clients, client)
				close(client.send)
			}
			b.mu.Unlock()
		}
	}
}

func (b *Broker) handleGetTopics(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	topicsList := make([]string, 0, len(b.topics))
	for t := range b.topics {
		topicsList = append(topicsList, t)
	}
	json.NewEncoder(w).Encode(topicsList)
}

func (b *Broker) handleGetQueues(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	queuesList := make([]string, 0, len(b.queues))
	for q := range b.queues {
		queuesList = append(queuesList, q)
	}
	json.NewEncoder(w).Encode(queuesList)
}
