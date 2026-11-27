import test from 'node:test';
import assert from 'node:assert/strict';
import { verifyEnvelope, repairEnvelope } from '../dist/index.mjs';

test('Verify-and-Repair Engine in ZeroQ', async (t) => {
  await t.test('scores valid message envelope at 100', () => {
    const valid = {
      id: 'msg_valid_01',
      topic: 'telemetry.audio.stream',
      payload: { frame: 'raw_pcm' },
      timestamp: Date.now(),
      ttl: 60000,
      seq: 1,
      retryCount: 0,
    };
    const res = verifyEnvelope(valid);
    assert.equal(res.passed, true);
    assert.equal(res.score, 100);
    assert.equal(res.violations.length, 0);
  });

  await t.test('mechanically repairs missing id, timestamp and ttl', () => {
    const broken = {
      topic: 'events.notification',
      payload: { alert: 'low_bandwidth' },
    };
    const res = repairEnvelope(broken);
    assert.equal(res.passed, true);
    assert.equal(res.final_score, 100);
    assert.ok(res.message.id.startsWith('zeroq_'));
    assert.ok(typeof res.message.timestamp === 'number');
    assert.equal(res.message.ttl, 60000);
  });

  await t.test('reports non-mechanical payload absence without guessing', () => {
    const unfixable = {
      id: 'msg_nofail',
      topic: 'events.heartbeat',
      payload: null,
      timestamp: Date.now(),
      ttl: 30000,
      retryCount: 0,
      seq: 0,
    };
    const res = repairEnvelope(unfixable);
    assert.equal(res.passed, false);
    assert.equal(res.message.payload, null);
    assert.ok(res.unresolved.some((u) => u.check_id === 'payload_integrity'));
  });
});
