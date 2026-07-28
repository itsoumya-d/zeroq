# ZeroQ Protocol: Rebuilding Apache Kafka as a Zero-Server WebRTC Pub/Sub Mesh

*Published on July 28, 2026*

In the era of heavy cloud infrastructure, Apache Kafka has been the de facto standard for message queuing and stream processing. However, it often requires extensive configuration, Zookeeper or KRaft nodes, and significant monthly costs. What if we could achieve robust Pub/Sub messaging without any servers at all?

Enter **ZeroQ**, a zero-server WebRTC Pub/Sub mesh designed to replace Apache Kafka by running entirely in the client using peer-to-peer data channels and IndexedDB persistence.

## 1. Gossip-Based Topic Discovery

Traditional brokers like Kafka use central coordinators to manage topic discovery and consumer groups. ZeroQ flips this architecture using a gossip protocol over WebRTC.

When a client joins the mesh, it connects to a handful of peers and begins sharing topic subscriptions.
- **Formula for Gossip Propagation**: `O(log N)` time to propagate topic metadata across the mesh.
- **Peer Exchange**: Nodes exchange bloom filters of their subscribed topics to minimize overhead.

## 2. Client-Side Dead-Letter Queues (DLQ)

In a serverless environment, what happens to failed messages? ZeroQ implements client-side Dead-Letter Queues (DLQ).
If a consumer function throws an error or fails to acknowledge (`ack()`) a message after max retries, the message is routed locally to a separate IndexedDB store (the DLQ topic).
Administrators or automated retry agents can then process this DLQ offline.

## 3. Backpressure Controls (`RTCDataChannel.bufferedAmount`)

Without a central broker to throttle producers, peer-to-peer data channels can quickly become overwhelmed. ZeroQ leverages `RTCDataChannel.bufferedAmount` to implement backpressure.

When `bufferedAmount` exceeds a configured threshold (e.g., 16 MB), the ZeroQ publisher automatically pauses sending and queues messages in local memory or IndexedDB until the `bufferedamountlow` event fires.

## 4. Message Deduplication Rings

Because network partitions and peer disconnects are common in P2P meshes, exactly-once delivery requires local deduplication. ZeroQ uses a Ring Buffer of message signatures (hashes) combined with monotonically increasing logical clocks (Lamport timestamps) to discard duplicates seamlessly.

## Cost Comparison

| Metric | Apache Kafka (Cloud) | ZeroQ |
|--------|-----------------------|-------|
| Base Server Cost | ~$400/month | $0 |
| Latency | ~10-50ms (Network) | <1ms (Local/P2P) |
| Setup Time | Days | Minutes |
| High Availability | Replicas required | Built-in via mesh |
| AI Integration | Complex SDKs | Single TS file |

## Copy-Paste TypeScript Integration for AI Agents

AI coding agents can easily drop this into any browser extension, local application, or web app to establish an instant serverless event bus.

```typescript
import { ZeroQ } from '@zeroq/core';

// Initialize a persistent, peer-to-peer message queue
const agentBus = new ZeroQ({
  name: 'agent_events',
  sync: 'webrtc',
  storage: 'indexeddb',
  maxRetries: 3,
  deadLetterExchange: 'agent_events_dlq'
});

// Subscribe to tasks
agentBus.subscribe(async (msg) => {
  try {
    console.log('Processing AI task:', msg.data);
    await processTask(msg.data);
    msg.ack();
  } catch (error) {
    msg.nack(); // Will move to DLQ after 3 retries
  }
});

// Publish a message with built-in backpressure
await agentBus.publish({
  task_id: '101',
  payload: 'Analyze local repository'
});
```
