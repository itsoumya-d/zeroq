# ZeroQ Protocol: Pub/Sub and Work Queues over a WebRTC DataChannel Mesh

*Published on July 28, 2026. Last corrected on August 1, 2026 — see the changelog at the end.*

Apache Kafka is the de facto standard for message queuing and stream processing, but it requires
extensive configuration, KRaft (or formerly Zookeeper) nodes, and real monthly spend. For a narrow
class of problems — fan-out between a handful of browser clients where losing a message is
acceptable — you do not need any of that.

**ZeroQ** is a pre-release, browser-first pub/sub and work-queue library that moves messages directly
between peers over WebRTC DataChannels, with an IndexedDB store for local durability. It is **not** a
Kafka replacement: it has no log replay, no consumer offsets, no partitions, no replication and
at-most-once delivery. What follows describes what the code in this repository actually does.

## 1. Gossip-Based Peer Discovery

A signaling server introduces peers; after that, peers introduce each other. When a DataChannel opens,
each side sends a `__gossip__` frame containing up to 10 known peer ids, and the receiver dials any it
has not seen. That is the whole protocol.

- **What is gossiped:** peer ids only.
- **What is *not* gossiped:** topic metadata, subscription sets, or consumer-group membership. There
  are no bloom filters and no topic-level routing — every message is broadcast to every connected peer,
  which filters locally by topic.
- **Topology:** because each peer dials every peer it learns about, the mesh converges to a full mesh
  with O(N²) connections. There is no multi-hop forwarding, so a peer only ever receives messages from
  peers it is *directly* connected to.

## 2. Client-Side Dead Letters

If a consumer calls `nack()`, `retryCount` is incremented and the message is re-broadcast, up to three
attempts. After that it is left in the local IndexedDB store, and
`PersistenceLayer.getDeadLetterQueue()` returns the messages with `retryCount >= 3`.

Two caveats the original version of this post got wrong:

- Dead letters live in the **same** object store (`messages`) as everything else — the "DLQ" is a
  filter over that store, not a separate store or a separate topic.
- `PersistenceLayer` is internal and `ZeroQ` exposes no accessor for it, so dead letters are currently
  only reachable by opening the `zeroq-db` database directly.

Note also that a consumer which simply dies without calling `ack()` or `nack()` triggers no retry at
all: there is no visibility timeout and no redelivery timer.

## 3. Backpressure Handling (`RTCDataChannel.bufferedAmount`)

With no central broker to throttle producers, a DataChannel can be overwhelmed. ZeroQ checks
`dc.bufferedAmount` before every send, against a **hardcoded 65,536-byte (64 KiB) threshold**.

Be precise about what happens above that threshold: the message is **discarded**. ZeroQ does not pause
the producer, does not buffer to memory or IndexedDB, and does not wait for `bufferedamountlow` — that
event is not used anywhere in the codebase. `publish()` still resolves successfully. The only signal is
a `message_dropped` event on the mesh, which callers must subscribe to if they care. The same is true
for a payload larger than the DataChannel's `sctp.maxMessageSize` (64 KiB minimum per spec, 256 KiB in
Chrome), because there is no chunking.

## 4. Duplicate Suppression

Because a full mesh delivers the same broadcast along multiple edges, receivers need to suppress
duplicates. ZeroQ keeps a `Set` of the last 10,000 `id:retryCount` strings, evicted first-in-first-out,
and drops any frame whose key it has already seen.

This is duplicate *suppression*, not exactly-once delivery:

- The key is the sender-supplied UUID, not a content hash, so a malicious peer can suppress a message
  by claiming its id first.
- Eviction is FIFO, not LRU, and the window is a fixed 10,000 entries; beyond that a genuine duplicate
  can be redelivered.
- There are no Lamport timestamps or logical clocks. `Message.seq` exists in the type but is always `0`
  and is never read. Ordering comes solely from the ordered delivery guarantee of a single DataChannel.

## Where ZeroQ Fits

| Metric | Apache Kafka (managed) | ZeroQ |
|--------|-----------------------|-------|
| Broker infrastructure | Cluster required | None — but a signaling server is still required |
| Licence | Apache-2.0 | BSL 1.1; $299–$9,999/yr for any production use |
| Delivery guarantee | at-least-once / exactly-once | **at-most-once, best effort** |
| Replay / consumer offsets | Yes | No |
| Ordering | Per partition | Per DataChannel only |
| Latency | Network RTT to the broker and back | One network RTT peer-to-peer |
| Throughput | Very high | ~480 msg/s with IndexedDB persistence enabled; ~61,000 msg/s with it disabled, measured single-machine over an in-process loopback channel (an upper bound) |

Cost figures for managed Kafka vary by an order of magnitude with retention, throughput and vendor;
this post no longer quotes a single monthly number, because the earlier "$400/month" figure was not
sourced. Compare your own bill.

## Integration

There is no `@zeroq/core` package — that name does not exist on npm, and the name `zeroq` on npm
belongs to an unrelated project by a different author. Load the built module from the CDN or build it
from source:

```typescript
import { ZeroQ } from 'https://cdn.jsdelivr.net/gh/itsoumya-d/zeroq@main/dist/index.mjs';

// ---- Peer A: the worker ----
const worker = new ZeroQ({ discoveryUrl: 'wss://your-signaling-server/ws' });

await worker.consume('agent_events', (msg, ack, nack) => {
  try {
    console.log('Processing AI task:', msg.payload);
    processTask(msg.payload);
    ack();
  } catch (err) {
    nack();   // up to 3 attempts, then left as a dead letter
  }
});

// ---- Peer B: the producer, in a separate tab or process ----
const producer = new ZeroQ({ discoveryUrl: 'wss://your-signaling-server/ws' });

await producer.createQueue('agent_events');
await producer.enqueue('agent_events', {
  task_id: '101',
  payload: 'Analyze local repository'
});
```

Three things to note, because the previous version of this snippet got all of them wrong:

- The constructor takes `{ discoveryUrl, topicId }`. There are no `name`, `sync`, `storage`,
  `maxRetries` or `deadLetterExchange` options.
- `subscribe`/`consume` take `(topicOrQueue, handler)`, and `publish`/`enqueue` take
  `(topicOrQueue, payload)`. The topic is never optional.
- `ack`/`nack` are the second and third *arguments* to a `consume` handler. There is no `msg.ack()`.
- Producer and consumer must be **different peers**. A peer does not receive its own publishes.

Finally: the discovery server bundled in `discovery/` does not currently relay `peer_joined`, `offer`,
`answer` or `ice_candidate` between clients, so no peer connection is established against it. You need
a signaling server that implements the protocol documented in the README.

---

### Changelog

**2026-08-01.** Corrected claims that were not supported by the code in this repository: bloom-filter
topic exchange and `O(log N)` topic-metadata propagation (neither exists), a separate IndexedDB store
for dead letters (there is one store), a configurable 16 MB backpressure threshold with producer pausing
and `bufferedamountlow` (the threshold is a hardcoded 64 KiB and the message is dropped), exactly-once
delivery via a hash ring buffer and Lamport timestamps (duplicate suppression uses sender-supplied
UUIDs; `seq` is always 0), the `<1ms` latency and `~$400/month` figures (neither was sourced or
reproducible), and an integration snippet importing a non-existent `@zeroq/core` package with an API
that does not match the library.
