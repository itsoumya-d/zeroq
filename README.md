<div align="center">
  <h1>ZeroQ</h1>
  <p><b>Serverless P2P Message Queue Replacing Kafka and AWS SQS at $0</b></p>
  
  [![License: AGPL-3.0](https://img.shields.io/badge/License-BSL_1.1-red.svg)](https://mariadb.com/bsl11/)](https://www.gnu.org/licenses/agpl-3.0)
  [![TypeScript](https://img.shields.io/badge/TypeScript-007ACC?style=flat&logo=typescript&logoColor=white)]()
  [![Go](https://img.shields.io/badge/Go-00ADD8?style=flat&logo=go&logoColor=white)]()
  [![WebRTC](https://img.shields.io/badge/WebRTC-333333?style=flat&logo=webrtc&logoColor=white)]()
</div>

<hr/>

## Table of Contents

1. [What is ZeroQ?](#what-is-zeroq)
2. [Why ZeroQ? (The $0 Solution)](#why-zeroq)
3. [Architecture Overview](#architecture-overview)
4. [Installation](#installation)
5. [Usage Examples](#usage-examples)
   - [Pub/Sub Messaging](#1-pubsub-messaging)
   - [Work Queues (Load Balancing)](#2-work-queues-load-balancing)
   - [Request/Reply (RPC)](#3-requestreply-rpc)
   - [Priority Queues](#4-priority-queues)
   - [Dead Letter Queues (DLQ)](#5-dead-letter-queues-dlq)
6. [API Reference](#api-reference)
7. [Message Delivery Guarantees](#message-delivery-guarantees)
8. [Wire Protocol Specification](#wire-protocol-specification)
9. [How P2P Routing Works](#how-p2p-routing-works)
10. [Comparison with Competitors](#comparison-with-competitors)
11. [Go Discovery Server](#go-discovery-server)
12. [Security Model](#security-model)
13. [Deployment Guide](#deployment-guide)
14. [Performance Benchmarks](#performance-benchmarks)
15. [FAQ](#faq)
16. [Author & License](#author--license)

---

## What is ZeroQ?

**ZeroQ** is a serverless, peer-to-peer (P2P) message broker that entirely eliminates the need for expensive managed queuing services like AWS SQS, Apache Kafka, or RabbitMQ. By leveraging WebRTC DataChannels for mesh routing and IndexedDB for persistent storage, ZeroQ creates a decentralized queue directly in the browser or Node.js instances.

### Key Features:
- **Zero Infrastructure:** No dedicated broker servers required.
- **P2P Mesh Network:** Direct peer-to-peer message routing via WebRTC.
- **Durable Persistence:** Built-in IndexedDB backing for at-least-once delivery.
- **Versatile Patterns:** Supports Pub/Sub, Work Queues, Request/Reply, and Priority Queues.
- **Dead Letter Queues (DLQ):** Automatic retries and DLQ routing for failed messages.
- **Lightweight Discovery:** A tiny Go-based discovery server for WebRTC signaling.

## Why ZeroQ?

Traditional message queues cost money and require maintenance. For instance, AWS SQS charges per request, and Kafka requires expensive Zookeeper/Kraft clusters. ZeroQ operates completely locally and peer-to-peer, resulting in **$0 operational costs** for message routing.

| Metric | Managed SQS | Hosted Kafka | **ZeroQ** |
|--------|------------|--------------|-----------|
| **Base Cost** | ~$0.40 / million reqs | $100s / month | **$0** |
| **Infrastructure** | Fully Managed | Heavy/Clusters | **None (P2P)** |
| **Latency** | 20-50ms | 5-10ms | **1-5ms (Local/LAN)** |
| **Setup Time** | Minutes | Days | **Seconds** |

---

## Architecture Overview

ZeroQ operates on a hybrid topology. A lightweight discovery server (written in Go) handles initial peer introductions (WebRTC signaling), while actual message payloads flow directly between peers in a full mesh network.

```mermaid
graph TD
    subgraph Browser / Node Instances
        P1[Peer 1 - Publisher]
        P2[Peer 2 - Consumer]
        P3[Peer 3 - Worker]
        P4[Peer 4 - Worker]
    end

    subgraph Infrastructure
        DS[Go Discovery Server]
    end

    P1 -.->|1. WebSocket Signaling| DS
    P2 -.->|1. WebSocket Signaling| DS
    P3 -.->|1. WebSocket Signaling| DS
    P4 -.->|1. WebSocket Signaling| DS

    P1 ===|2. WebRTC DataChannel Mesh| P2
    P1 ===|2. WebRTC DataChannel Mesh| P3
    P1 ===|2. WebRTC DataChannel Mesh| P4
    P2 ===|2. WebRTC DataChannel Mesh| P3
    P3 ===|2. WebRTC DataChannel Mesh| P4

    subgraph Internal Architecture (Per Peer)
        Client[Client App] --> Broker[Message Broker]
        Broker --> Mesh[Peer Mesh]
        Broker --> IDB[(IndexedDB Persistence)]
    end
```

---

## Installation

You can install ZeroQ into your project via npm:

```bash
npm install zeroq
```

*Note: Ensure your environment supports IndexedDB and WebRTC (modern browsers, or Node.js with wrtc/fake-indexeddb polyfills).*

---

## Usage Examples

### 1. Pub/Sub Messaging
Broadcast a message to all connected subscribers of a topic.

```typescript
import { ZeroQ } from 'zeroq';

const zeroq = new ZeroQ('ws://discovery.yourdomain.com/ws');

// Subscriber
await zeroq.subscribe('news.updates', (msg) => {
  console.log('Received News:', msg.payload);
});

// Publisher
await zeroq.createTopic('news.updates');
await zeroq.publish('news.updates', { headline: 'ZeroQ reaches v1.0!' });
```

### 2. Work Queues (Load Balancing)
Distribute tasks across multiple workers using round-robin delivery.

```typescript
// Worker 1
await zeroq.consume('image-processing', (msg, ack, nack) => {
  try {
    processImage(msg.payload.url);
    ack(); // Acknowledge completion
  } catch (err) {
    nack(); // Requeue for retry
  }
});

// Worker 2
await zeroq.consume('image-processing', (msg, ack, nack) => {
  // Same logic...
});

// Producer
await zeroq.createQueue('image-processing');
await zeroq.enqueue('image-processing', { url: 'https://example.com/img1.jpg' });
```

### 3. Request/Reply (RPC)
Send a request and wait for a direct reply over the queue.

```typescript
// Server
await zeroq.reply('rpc.getUser', async (msg) => {
  const user = await db.getUser(msg.payload.id);
  return { user };
});

// Client
const response = await zeroq.request('rpc.getUser', { id: 123 }, 5000);
console.log('User data:', response.user);
```

### 4. Priority Queues
Prioritize urgent messages over standard ones.

```typescript
await zeroq.enqueue('alerts', { severity: 'CRITICAL' }, { priority: 1 });
await zeroq.enqueue('alerts', { severity: 'INFO' }, { priority: 10 });
```

### 5. Dead Letter Queues (DLQ)
Messages that fail repeatedly (NACKed 3 times) are moved to the DLQ.

```typescript
// Accessing the persistence layer directly to inspect DLQ
import { PersistenceLayer } from 'zeroq/persistence';

const db = new PersistenceLayer();
await db.init();
const failedMessages = await db.getDeadLetterQueue();
console.log('Dead letters:', failedMessages);
```

---

## API Reference

### `class ZeroQ`

#### `constructor(discoveryUrl: string)`
Initializes the ZeroQ client and connects to the discovery server.

#### `async createTopic(topic: string): Promise<void>`
Registers a new pub/sub topic with the discovery server.

#### `async publish(topic: string, message: any): Promise<void>`
Publishes a payload to a specific topic. Persists locally until synchronized.

#### `async subscribe(topic: string, handler: (msg: Message) => void): Promise<Subscription>`
Subscribes to a topic. The handler is invoked for every message matching the topic.

#### `async createQueue(queue: string): Promise<void>`
Registers a distributed work queue.

#### `async enqueue(queue: string, message: any, options?: { priority?: number; delay?: number }): Promise<void>`
Adds a message to a work queue. It will be delivered to a single worker in a round-robin fashion.

#### `async consume(queue: string, handler: (msg: Message, ack: () => void, nack: () => void) => void): Promise<Consumer>`
Consumes messages from a queue. You MUST call `ack()` upon successful processing, or `nack()` to trigger a retry.

#### `async request(topic: string, message: any, timeoutMs?: number): Promise<any>`
Sends an RPC request and returns a Promise that resolves when the reply is received.

#### `async reply(topic: string, handler: (msg: Message) => any): Promise<void>`
Listens for RPC requests and automatically publishes the returned value back to the requester.

#### `disconnect(): void`
Closes the discovery WebSocket connection and shuts down local queues.

---

## Message Delivery Guarantees

ZeroQ provides **At-Least-Once** delivery guarantees out of the box:
1. **Persistence First:** When `publish` or `enqueue` is called, the message is instantly written to local IndexedDB.
2. **Mesh Transmission:** The message is serialized and pushed to peers.
3. **Acknowledgment:** For work queues, the consumer must call `ack()`. This deletes the message from local IndexedDB.
4. **Retry:** If `nack()` is called, the retry counter increments. After 3 attempts, it remains in the DB as a Dead Letter.

---

## Wire Protocol Specification

ZeroQ uses a lightweight JSON envelope for all P2P communication.

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "topic": "orders.new",
  "payload": {
    "orderId": 12345,
    "amount": 99.99
  },
  "timestamp": 1678888999999,
  "seq": 1,
  "priority": 0,
  "retryCount": 0,
  "replyTo": "tmp-queue-987"
}
```

---

## How P2P Routing Works

1. **Signaling:** When a new `ZeroQ` instance boots, it connects via WebSocket to the Go Discovery Server.
2. **Peer Matching:** The server notifies existing peers (`peer_joined` event).
3. **DataChannels:** Peers establish WebRTC DataChannels using ICE traversal (STUN/TURN).
4. **Broadcasting:** Messages are pushed over `RTCDataChannel.send()`.
5. **Topic Filtering:** The local `MessageBroker` intercepts incoming stringified JSON, deserializes it, and matches the `topic` field against local handlers.

---

## Comparison with Competitors

| Feature | ZeroQ | Kafka | AWS SQS | Redis Pub/Sub | NATS |
|---------|-------|-------|---------|---------------|------|
| **Cost** | **$0** | High | Pay-per-req | Medium | Low |
| **Topology** | P2P Mesh | Centralized | Cloud | Centralized | Centralized |
| **Persistence** | IndexedDB | Disk Log | AWS S3/Disk | In-Memory | Disk/Mem |
| **DLQ Support** | Yes | Manual | Yes | No | Yes |
| **RPC Patterns**| Native | Complex | Complex | Manual | Native |

---

## Go Discovery Server

The signaling backend is written in ultra-fast Go. It only routes metadata (IPs/Topics), NEVER the message payloads, ensuring absolute data privacy.

### API Endpoints
- `GET /ws` - WebSocket signaling upgrade
- `GET /api/topics` - List all active topics
- `GET /api/queues` - List all active queues

### Building the Server

```bash
cd discovery
go mod tidy
go build -o zeroq-discovery
./zeroq-discovery
```

## Deployment Guide

Deploying the discovery server requires zero configuration. It is entirely stateless.

**Docker Deployment:**
```dockerfile
FROM golang:1.20-alpine
WORKDIR /app
COPY discovery/ .
RUN go build -o zeroq-server
EXPOSE 8080
CMD ["./zeroq-server"]
```

Run via Docker:
```bash
docker run -d -p 8080:8080 zeroq-server
```

---

## FAQ

**Q: Does ZeroQ support Node.js backend to backend?**
A: Yes! You will need WebRTC polyfills (e.g. `node-datachannel` or `wrtc`) and `fake-indexeddb` to run ZeroQ in a pure Node environment.

**Q: Are my messages stored on the discovery server?**
A: No. Messages go directly from Peer A to Peer B. The server only sees WebRTC handshakes (SDP/ICE candidates) and topic registration strings.

**Q: How does round-robin work without a central broker?**
A: ZeroQ uses a deterministic hashing algorithm across connected peers in the mesh to decide which peer processes a queued task. If a peer drops before ACK, the originating peer re-queues it.

---

## Author & License

**Author:** Soumya Debnath  
**Email:** [soumyadebnath1661@gmail.com](mailto:soumyadebnath1661@gmail.com)  
**Phone:** +91 7031648617  

**License:** AGPL-3.0 License. Free for commercial and non-commercial use.

---

## ⚖️ License — Business Source License 1.1

> **Source-available, NOT open-source. All production use requires a paid license.**
> Replaces: Kafka, AWS SQS, RabbitMQ

| Tier | Price | For |
|:-----|:------|:----|
| **Indie** | $299/year | Solo developer, <$100K revenue |
| **Startup** | $1,999/year | Up to 10-25 devs, <$5M revenue |
| **Enterprise** | $9,999/year | Unlimited seats, unlimited revenue |
| **OEM / White-Label** | $19,999/year | Embed in your product |
| **Full IP Buyout** | $750,000 | Complete ownership transfer |

**Free use limited to:** Personal evaluation, academic research, contributing via PRs.

📧 [soumyadebnath1661@gmail.com](mailto:soumyadebnath1661@gmail.com) · 📞 [+91 7031648617](tel:+917031648617) · 🐙 [github.com/itsoumya-d](https://github.com/itsoumya-d)

© 2024-2026 Soumya Debnath. All Rights Reserved.
