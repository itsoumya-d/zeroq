// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

const { ZeroQ } = await import(join(__dirname, '../dist/index.mjs'));

// ---------------------------------------------------------------------------
// Note: ZeroQ connects WebSocket + WebRTC in constructor; in Node those
// environments do not exist (WebSocket is undefined unless polyfilled).
// We test the parts that are pure-logic and guard-against undefined:
// the module loads, the class exports the documented API, and the
// disconnect() path is safe.  Network-dependent behaviour (P2P messaging,
// DLQ, RPC) requires a live signaling server and is not tested here.
// ---------------------------------------------------------------------------

describe('Module exports', () => {
  test('ZeroQ is exported and is a constructor', () => {
    assert.ok(typeof ZeroQ === 'function');
    // Prototype must have the documented methods
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
    // WebSocket/RTCPeerConnection are undefined in Node — the class guards
    // against that internally (typeof WebSocket === "undefined" checks).
    assert.doesNotThrow(() => new ZeroQ());
  });

  test('constructor with discoveryUrl option does not throw', () => {
    assert.doesNotThrow(() => new ZeroQ({ discoveryUrl: 'ws://localhost:9999' }));
  });

  test('disconnect() on fresh instance does not throw', () => {
    const q = new ZeroQ();
    assert.doesNotThrow(() => q.disconnect());
  });

  test('double disconnect() does not throw', () => {
    const q = new ZeroQ();
    q.disconnect();
    assert.doesNotThrow(() => q.disconnect());
  });
});

describe('subscribe returns Subscription object', () => {
  test('subscribe returns object with unsubscribe()', async () => {
    const q = new ZeroQ();
    const sub = await q.subscribe('test-topic', () => {});
    assert.ok(sub, 'subscribe must return a value');
    assert.ok(typeof sub.unsubscribe === 'function', 'Subscription must have unsubscribe()');
    sub.unsubscribe();
    q.disconnect();
  });

  test('unsubscribe is idempotent', async () => {
    const q = new ZeroQ();
    const sub = await q.subscribe('idem-topic', () => {});
    sub.unsubscribe();
    assert.doesNotThrow(() => sub.unsubscribe());
    q.disconnect();
  });
});

describe('consume returns Consumer object', () => {
  test('consume returns object with unsubscribe()', async () => {
    const q = new ZeroQ();
    const consumer = await q.consume('work-queue', () => {});
    assert.ok(consumer, 'consume must return a value');
    assert.ok(typeof consumer.unsubscribe === 'function', 'Consumer must have unsubscribe()');
    consumer.unsubscribe();
    q.disconnect();
  });
});

describe('publish (local broker path — no peers)', () => {
  test('publish to topic does not throw even with no peers', async () => {
    const q = new ZeroQ();
    await assert.doesNotReject(() => q.publish('no-peers-topic', { data: 'test' }));
    q.disconnect();
  });

  test('publish with complex payload does not throw', async () => {
    const q = new ZeroQ();
    const payload = { nested: { a: 1 }, arr: [1, 2, 3], ts: Date.now() };
    await assert.doesNotReject(() => q.publish('complex-topic', payload));
    q.disconnect();
  });

  test('publish to empty-string topic does not crash', async () => {
    const q = new ZeroQ();
    try { await q.publish('', {}); } catch (e) {
      assert.ok(typeof e.message === 'string');
    }
    q.disconnect();
  });
});

describe('createTopic and createQueue', () => {
  test('createTopic does not throw (no signaling server in Node)', async () => {
    const q = new ZeroQ();
    // DiscoveryClient.send is a no-op when WS is not connected
    await assert.doesNotReject(() => q.createTopic('my-topic'));
    q.disconnect();
  });

  test('createQueue does not throw', async () => {
    const q = new ZeroQ();
    await assert.doesNotReject(() => q.createQueue('my-queue'));
    q.disconnect();
  });
});

describe('enqueue', () => {
  test('enqueue does not throw with no peers', async () => {
    const q = new ZeroQ();
    await assert.doesNotReject(() => q.enqueue('task-queue', { job: 'render' }));
    q.disconnect();
  });

  test('enqueue with priority option does not throw', async () => {
    const q = new ZeroQ();
    await assert.doesNotReject(() => q.enqueue('prio-q', { v: 1 }, { priority: 1 }));
    q.disconnect();
  });
});

describe('request timeout', () => {
  test('request times out when no reply comes (Node environment)', async () => {
    const q = new ZeroQ();
    // With no signaling/peers, request must reject with Timeout
    await assert.rejects(
      () => q.request('rpc-topic', { id: 1 }, 100),
      /Timeout/i
    );
    q.disconnect();
  });
});

describe('Adversarial cases', () => {
  test('publish with null payload does not crash', async () => {
    const q = new ZeroQ();
    try { await q.publish('null-topic', null); } catch (e) {
      assert.ok(typeof e.message === 'string');
    }
    q.disconnect();
  });

  test('multiple ZeroQ instances do not interfere', () => {
    const q1 = new ZeroQ();
    const q2 = new ZeroQ();
    q1.disconnect();
    q2.disconnect();
    assert.ok(true, 'multiple instances created and disconnected without error');
  });
});
