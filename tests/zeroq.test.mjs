// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

const { ZeroQ } = await import(join(__dirname, '../dist/index.mjs'));

// ---------------------------------------------------------------------------
// Environment note: Node >= 22 DOES provide a global WebSocket, so the
// `typeof WebSocket === "undefined"` guards inside ZeroQ no longer short-circuit
// in Node. A ZeroQ instance therefore opens real WebSocket connections here.
// RTCPeerConnection and indexedDB are still absent, so peer connections and
// persistence are inert. Tests point at a closed port and always call
// disconnect(), which must cancel the reconnect backoff ladder.
// Network-dependent behaviour (P2P messaging, DLQ, RPC across peers) needs a
// live signaling server and working DataChannels and is not tested here.
// ---------------------------------------------------------------------------

const DEAD_URL = 'ws://127.0.0.1:9/none';
const mk = (extra = {}) => new ZeroQ({ discoveryUrl: DEAD_URL, ...extra });

describe('Module exports', () => {
  test('ZeroQ is exported and is a constructor', () => {
    assert.ok(typeof ZeroQ === 'function');
    const proto = ZeroQ.prototype;
    assert.ok(typeof proto.publish === 'function', 'publish');
    assert.ok(typeof proto.subscribe === 'function', 'subscribe');
    assert.ok(typeof proto.createTopic === 'function', 'createTopic');
    assert.ok(typeof proto.createQueue === 'function', 'createQueue');
    assert.ok(typeof proto.enqueue === 'function', 'enqueue');
    assert.ok(typeof proto.consume === 'function', 'consume');
    assert.ok(typeof proto.request === 'function', 'request');
    assert.ok(typeof proto.reply === 'function', 'reply');
    assert.ok(typeof proto.disconnect === 'function', 'disconnect');
  });
});

describe('ZeroQ construction', () => {
  test('constructor with no options does not throw', () => {
    assert.doesNotThrow(() => { const q = new ZeroQ(); q.disconnect(); });
  });

  test('constructor with discoveryUrl option does not throw', () => {
    const q = mk();
    assert.doesNotThrow(() => q.disconnect());
  });

  test('disconnect() on fresh instance does not throw', () => {
    const q = mk();
    assert.doesNotThrow(() => q.disconnect());
  });

  test('double disconnect() does not throw', () => {
    const q = mk();
    q.disconnect();
    assert.doesNotThrow(() => q.disconnect());
  });

  test('options.discoveryUrl is used for the peer-mesh signaling socket', async () => {
    // Regression: init() previously hardcoded 'ws://localhost:8080' and
    // 'default-topic', so options.discoveryUrl was ignored for the socket that
    // actually carries WebRTC signaling.
    const q = new ZeroQ({ discoveryUrl: DEAD_URL, topicId: 'my-room' });
    await new Promise(r => setTimeout(r, 50));   // init() is async
    assert.equal(q.discoveryUrl, DEAD_URL);
    assert.equal(q.topicId, 'my-room');
    assert.equal(q.peerMesh.discoveryUrl, DEAD_URL, 'PeerMesh must dial the configured URL');
    assert.equal(q.peerMesh.topicId, 'my-room', 'PeerMesh must join the configured topicId');
    q.disconnect();
  });

  test('disconnect() cancels the reconnect backoff ladder', async () => {
    // Regression: pending reconnect timers used to keep the host process alive
    // for ~181s (1+2+4+8+16+30*5 s) after disconnect() had been called.
    const q = mk();
    await new Promise(r => setTimeout(r, 50));   // let init() open the sockets
    q.disconnect();
    await new Promise(r => setTimeout(r, 50));
    assert.equal(q.peerMesh.pendingTimers.size, 0, 'no pending reconnect timers may survive disconnect()');
    assert.equal(q.peerMesh.pingIntervals.size, 0, 'no ping intervals may survive disconnect()');
    assert.equal(q.peerMesh.reconnectTimeouts.size, 0);
  });
});

describe('subscribe returns Subscription object', () => {
  test('subscribe returns object with unsubscribe()', async () => {
    const q = mk();
    const sub = await q.subscribe('test-topic', () => { });
    assert.ok(sub, 'subscribe must return a value');
    assert.ok(typeof sub.unsubscribe === 'function', 'Subscription must have unsubscribe()');
    sub.unsubscribe();
    q.disconnect();
  });

  test('unsubscribe is idempotent', async () => {
    const q = mk();
    const sub = await q.subscribe('idem-topic', () => { });
    sub.unsubscribe();
    assert.doesNotThrow(() => sub.unsubscribe());
    q.disconnect();
  });

  test('unsubscribe removes the topic key, leaving no empty entry', async () => {
    const q = mk();
    const sub = await q.subscribe('gone-topic', () => { });
    assert.equal(q.broker.topicHandlers.has('gone-topic'), true);
    sub.unsubscribe();
    assert.equal(q.broker.topicHandlers.has('gone-topic'), false);
    q.disconnect();
  });
});

describe('consume returns Consumer object', () => {
  test('consume returns object with unsubscribe()', async () => {
    const q = mk();
    const consumer = await q.consume('work-queue', () => { });
    assert.ok(consumer, 'consume must return a value');
    assert.ok(typeof consumer.unsubscribe === 'function', 'Consumer must have unsubscribe()');
    consumer.unsubscribe();
    q.disconnect();
  });
});

describe('publish (local broker path — no peers)', () => {
  test('publish to topic does not throw even with no peers', async () => {
    const q = mk();
    await assert.doesNotReject(() => q.publish('no-peers-topic', { data: 'test' }));
    q.disconnect();
  });

  test('publish with complex payload does not throw', async () => {
    const q = mk();
    const payload = { nested: { a: 1 }, arr: [1, 2, 3], ts: Date.now() };
    await assert.doesNotReject(() => q.publish('complex-topic', payload));
    q.disconnect();
  });

  test('publish to empty-string topic does not crash', async () => {
    const q = mk();
    try { await q.publish('', {}); } catch (e) {
      assert.ok(typeof e.message === 'string');
    }
    q.disconnect();
  });
});

describe('createTopic and createQueue', () => {
  test('createTopic does not throw (no signaling server in Node)', async () => {
    const q = mk();
    await assert.doesNotReject(() => q.createTopic('my-topic'));
    q.disconnect();
  });

  test('createQueue does not throw', async () => {
    const q = mk();
    await assert.doesNotReject(() => q.createQueue('my-queue'));
    q.disconnect();
  });
});

describe('enqueue', () => {
  test('enqueue does not throw with no peers', async () => {
    const q = mk();
    await assert.doesNotReject(() => q.enqueue('task-queue', { job: 'render' }));
    q.disconnect();
  });

  test('enqueue with priority option does not throw', async () => {
    const q = mk();
    await assert.doesNotReject(() => q.enqueue('prio-q', { v: 1 }, { priority: 1 }));
    q.disconnect();
  });

  test('enqueue priority is carried on the message envelope', async () => {
    // Regression: the options argument used to be accepted and dropped, so
    // Message.priority was always undefined.
    const q = mk();
    const seen = [];
    q.peerMesh.broadcast = (data) => { seen.push(JSON.parse(data)); };
    await q.enqueue('prio-q2', { v: 1 }, { priority: 7 });
    assert.equal(seen.length, 1);
    assert.equal(seen[0].priority, 7, 'priority must reach the wire envelope');
    q.disconnect();
  });
});

describe('request timeout', () => {
  test('request times out when no reply comes (Node environment)', async () => {
    const q = mk();
    await assert.rejects(
      () => q.request('rpc-topic', { id: 1 }, 100),
      /Timeout/i
    );
    q.disconnect();
  });

  test('a timed-out request() leaves no leaked subscription', async () => {
    // Regression: each timed-out request() permanently leaked a handler plus a
    // Map entry keyed by a fresh randomUUID reply topic.
    const q = mk();
    const before = q.broker.topicHandlers.size;
    for (let i = 0; i < 50; i++) {
      await q.request('rpc-leak', { i }, 1).catch(() => { });
    }
    assert.equal(q.broker.topicHandlers.size, before,
      'timed-out requests must not accumulate reply-topic handlers');
    q.disconnect();
  });
});

describe('inbound frame validation', () => {
  test('an unparseable frame from a peer is dropped, not thrown', async () => {
    // Regression: JSON.parse inside the async handleMessage produced an
    // unhandled rejection, which terminates a Node process by default.
    const q = mk();
    const warn = console.warn; console.warn = () => { };
    try {
      q.peerMesh.emit('message_received', 'definitely not json');
      await new Promise(r => setTimeout(r, 30));
    } finally { console.warn = warn; }
    assert.ok(true, 'survived a malformed peer frame');
    q.disconnect();
  });

  test('a JSON frame with no id/topic is dropped', async () => {
    const q = mk();
    let delivered = 0;
    await q.subscribe('shape-topic', () => delivered++);
    const warn = console.warn; console.warn = () => { };
    try {
      q.peerMesh.emit('message_received', JSON.stringify({ hello: 'world' }));
      await new Promise(r => setTimeout(r, 30));
    } finally { console.warn = warn; }
    assert.equal(delivered, 0);
    q.disconnect();
  });

  test('a well-formed frame is delivered to the subscriber', async () => {
    const q = mk();
    const got = [];
    await q.subscribe('good-topic', (m) => got.push(m.payload));
    q.peerMesh.emit('message_received', JSON.stringify({
      id: crypto.randomUUID(), topic: 'good-topic', payload: { ok: 1 },
      timestamp: Date.now(), seq: 0, retryCount: 0
    }));
    await new Promise(r => setTimeout(r, 30));
    assert.deepEqual(got, [{ ok: 1 }]);
    q.disconnect();
  });

  test('duplicate id + same attempt is suppressed; a retry attempt is not', async () => {
    const q = mk();
    const got = [];
    await q.subscribe('dedup-topic', (m) => got.push(m.retryCount));
    const id = crypto.randomUUID();
    const frame = (retryCount) => JSON.stringify({
      id, topic: 'dedup-topic', payload: {}, timestamp: 0, seq: 0, retryCount
    });
    q.peerMesh.emit('message_received', frame(0));
    q.peerMesh.emit('message_received', frame(0)); // exact duplicate -> dropped
    q.peerMesh.emit('message_received', frame(1)); // nack retry     -> delivered
    await new Promise(r => setTimeout(r, 30));
    assert.deepEqual(got, [0, 1], 'retries must not be mistaken for duplicates');
    q.disconnect();
  });
});

describe('reply() reads replyTo from the message body', () => {
  test('reply() responds to a request()-shaped message', async () => {
    // Regression: request() writes replyTo into the message body (which lands
    // under `payload`), while reply() only read the envelope field, so RPC
    // could never round-trip.
    const q = mk();
    const published = [];
    q.peerMesh.broadcast = (data) => { published.push(JSON.parse(data)); };
    await q.reply('rpc.sum', (msg) => ({ sum: msg.payload.a + msg.payload.b }));
    const replyTo = crypto.randomUUID();
    q.peerMesh.emit('message_received', JSON.stringify({
      id: crypto.randomUUID(), topic: 'rpc.sum',
      payload: { a: 2, b: 3, replyTo }, timestamp: Date.now(), seq: 0, retryCount: 0
    }));
    await new Promise(r => setTimeout(r, 60));
    const response = published.find(p => p.topic === replyTo);
    assert.ok(response, 'reply() must publish to the replyTo topic');
    assert.deepEqual(response.payload, { sum: 5 });
    q.disconnect();
  });
});

describe('Adversarial cases', () => {
  test('publish with null payload does not crash', async () => {
    const q = mk();
    try { await q.publish('null-topic', null); } catch (e) {
      assert.ok(typeof e.message === 'string');
    }
    q.disconnect();
  });

  test('a throwing subscriber does not stop other subscribers', async () => {
    const q = mk();
    let second = 0;
    const warn = console.warn; console.warn = () => { };
    try {
      await q.subscribe('throwy', () => { throw new Error('boom'); });
      await q.subscribe('throwy', () => { second++; });
      q.peerMesh.emit('message_received', JSON.stringify({
        id: crypto.randomUUID(), topic: 'throwy', payload: {},
        timestamp: 0, seq: 0, retryCount: 0
      }));
      await new Promise(r => setTimeout(r, 30));
    } finally { console.warn = warn; }
    assert.equal(second, 1, 'a throwing handler must not prevent later handlers');
    q.disconnect();
  });

  test('multiple ZeroQ instances do not interfere', () => {
    const q1 = mk();
    const q2 = mk();
    q1.disconnect();
    q2.disconnect();
    assert.ok(true, 'multiple instances created and disconnected without error');
  });
});
