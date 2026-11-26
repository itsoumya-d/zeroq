import test from 'node:test';
import assert from 'node:assert/strict';
import { ZeroQ } from '../dist/index.mjs';

test('Message TTL and Expiration handling', async (t) => {
  await t.test('delivers message when received before TTL expires', async () => {
    const q = new ZeroQ({ allowEval: true });
    const received = [];

    await q.subscribe('orders.active', (msg) => {
      received.push(msg);
    });

    q.peerMesh.emit('message_received', JSON.stringify({
      id: 'ord-ttl-001',
      topic: 'orders.active',
      payload: { orderId: 'ord-101' },
      timestamp: Date.now(),
      ttl: 5000,
      expiresAt: Date.now() + 5000,
      seq: 0,
      retryCount: 0
    }));

    await new Promise((resolve) => setTimeout(resolve, 60));

    assert.equal(received.length, 1);
    assert.equal(received[0].payload.orderId, 'ord-101');
    assert.equal(received[0].ttl, 5000);
    assert.ok(typeof received[0].expiresAt === 'number');

    q.disconnect();
  });

  await t.test('routes expired message to DLQ with message_expired reason without dispatching to consumer', async () => {
    const q = new ZeroQ({ allowEval: true, dlqSuffix: '.dlq' });
    const receivedDlq = [];
    const receivedNormal = [];

    q.peerMesh.broadcast = (frame) => {
      setImmediate(() => {
        q.peerMesh.emit('message_received', frame);
      });
    };

    await q.subscribe('inventory.dlq', (msg) => {
      receivedDlq.push(msg);
    });

    await q.consume('inventory', async (msg, ack, nack) => {
      receivedNormal.push(msg);
      await ack();
    });

    // Inbound frame with expiresAt already in the past
    const expiredTimestamp = Date.now() - 2000;
    q.peerMesh.emit('message_received', JSON.stringify({
      id: 'inv-expired-001',
      topic: 'inventory',
      payload: { itemId: 'sku-888' },
      timestamp: expiredTimestamp,
      ttl: 1000,
      expiresAt: expiredTimestamp + 1000,
      seq: 0,
      retryCount: 0
    }));

    await new Promise((resolve) => setTimeout(resolve, 80));

    assert.equal(receivedNormal.length, 0, 'normal consumer must not receive expired message');
    assert.equal(receivedDlq.length, 1, 'expected 1 DLQ delivery for expired message');
    const dlqItem = receivedDlq[0];
    assert.equal(dlqItem.topic, 'inventory.dlq');
    assert.equal(dlqItem.originalTopic, 'inventory');
    assert.equal(dlqItem.dlqReason, 'message_expired');
    assert.equal(dlqItem.payload.itemId, 'sku-888');

    q.disconnect();
  });
});
