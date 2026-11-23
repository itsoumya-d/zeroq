import test from 'node:test';
import assert from 'node:assert/strict';
import { ZeroQ } from '../dist/index.mjs';

test('Dead Letter Queue (DLQ) routing on exhausted retries', async (t) => {
  await t.test('routes to topic.dlq when consumer nacks exceed maxRetries', async () => {
    const q = new ZeroQ({ allowEval: true, maxRetries: 2 });
    const receivedDlq = [];

    // Loopback broadcast frames to simulate peer mesh message receipt
    q.peerMesh.broadcast = (frame) => {
      setImmediate(() => {
        q.peerMesh.emit('message_received', frame);
      });
    };

    // Subscribe to DLQ
    await q.subscribe('payments.dlq', (msg) => {
      receivedDlq.push(msg);
    });

    // Consumer that nacks with error reason
    let attempts = 0;
    await q.consume('payments', async (msg, ack, nack) => {
      attempts++;
      await nack('payment_gateway_down');
    });

    // Inbound initial message
    q.peerMesh.emit('message_received', JSON.stringify({
      id: 'tx-001',
      topic: 'payments',
      payload: { orderId: 'ord-999', amount: 4500 },
      timestamp: Date.now(),
      seq: 0,
      retryCount: 0
    }));

    // Allow retry cycle and DLQ routing to execute
    await new Promise((resolve) => setTimeout(resolve, 80));

    assert.equal(attempts, 2, 'expected exactly 2 consumer attempts before DLQ');
    assert.equal(receivedDlq.length, 1, 'expected 1 DLQ delivery');
    const dlqItem = receivedDlq[0];
    assert.equal(dlqItem.topic, 'payments.dlq');
    assert.equal(dlqItem.originalTopic, 'payments');
    assert.equal(dlqItem.dlqReason, 'payment_gateway_down');
    assert.equal(dlqItem.payload.orderId, 'ord-999');
    assert.ok(typeof dlqItem.failedAt === 'number');

    q.disconnect();
  });

  await t.test('supports custom dlqSuffix', async () => {
    const q = new ZeroQ({ allowEval: true, maxRetries: 1, dlqSuffix: '.dead' });
    const receivedDlq = [];

    q.peerMesh.broadcast = (frame) => {
      setImmediate(() => {
        q.peerMesh.emit('message_received', frame);
      });
    };

    await q.subscribe('transfers.dead', (msg) => {
      receivedDlq.push(msg);
    });

    await q.consume('transfers', async (msg, ack, nack) => {
      await nack('insufficient_funds');
    });

    q.peerMesh.emit('message_received', JSON.stringify({
      id: 'tx-002',
      topic: 'transfers',
      payload: { transferId: 'tx-123' },
      timestamp: Date.now(),
      seq: 0,
      retryCount: 0
    }));

    await new Promise((resolve) => setTimeout(resolve, 80));

    assert.equal(receivedDlq.length, 1);
    assert.equal(receivedDlq[0].topic, 'transfers.dead');
    assert.equal(receivedDlq[0].originalTopic, 'transfers');
    assert.equal(receivedDlq[0].dlqReason, 'insufficient_funds');

    q.disconnect();
  });
});
