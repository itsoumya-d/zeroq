import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';

const bundle = new URL('../dist/index.mjs', import.meta.url).href;

// Run with Node's strict rejection policy in a separate process. Catching an
// unhandledRejection in this test would hide the application-crashing defect.
async function exerciseHandlers(bundleUrl, kind, outcome) {
  const { default: assert } = await import('node:assert/strict');
  const { setImmediate } = await import('node:timers/promises');
  const { ZeroQ } = await import(bundleUrl);
  globalThis.WebSocket = undefined;
  globalThis.indexedDB = undefined;

  const q = new ZeroQ({ allowEval: true });
  const warnings = [];
  const failure = new Error('handler failed after delivery');
  const saves = [];
  const deletions = [];
  const broadcasts = [];
  const healthyDeliveries = [];
  let handlerCalls = 0;
  console.warn = (...args) => warnings.push(args);
  q.persistence.save = async (msg) => saves.push({ ...msg });
  q.persistence.delete = async (id) => deletions.push(id);
  q.peerMesh.broadcast = (frame) => broadcasts.push(frame);

  const handler = () => {
    handlerCalls++;
    switch (outcome) {
      case 'sync-throw': throw failure;
      case 'rejected-promise': return Promise.reject(failure);
      case 'delayed-rejection': return (async () => {
        await setImmediate();
        throw failure;
      })();
      case 'rejecting-thenable': return { then: (_resolve, reject) => reject(failure) };
      case 'throwing-then-getter': return { get then() { throw failure; } };
      case 'fulfilled-promise': return Promise.resolve();
      case 'pending-promise': return new Promise(() => {});
      default: throw new Error(`Unknown outcome: ${outcome}`);
    }
  };
  const register = kind === 'subscriber' ? 'subscribe' : 'consume';
  const first = await q[register]('jobs', handler);
  const second = await q[register]('jobs', (msg) => healthyDeliveries.push(msg.id));

  try {
    for (let index = 0; index < 4; index++) {
      q.peerMesh.emit('message_received', JSON.stringify({
        id: `message-${index}`, topic: 'jobs', payload: index,
        timestamp: 0, seq: index, retryCount: 0,
      }));
      // Allow persistence, the delayed handler, and rejection observers to run.
      // A pending handler must not block either another handler or a new frame.
      await setImmediate();
      await setImmediate();
    }

    const expectedCalls = kind === 'subscriber' ? 4 : 2;
    assert.equal(handlerCalls, expectedCalls);
    assert.deepEqual(healthyDeliveries, kind === 'subscriber'
      ? ['message-0', 'message-1', 'message-2', 'message-3']
      : ['message-1', 'message-3']);
    const expectedWarnings = outcome.endsWith('promise') && outcome !== 'rejected-promise'
      ? 0 : expectedCalls;
    assert.equal(warnings.length, expectedWarnings);
    for (const [message, error] of warnings) {
      assert.equal(message, `ZeroQ: ${kind} threw`);
      assert.equal(error, failure, 'preserve the original rejection reason');
    }
    assert.equal(saves.length, 4, 'persist each delivery once');
    assert.ok(saves.every((msg) => msg.retryCount === 0));
    assert.deepEqual(deletions, [], 'handler failure must not implicitly ack');
    assert.deepEqual(broadcasts, [], 'handler failure must not implicitly nack');

    first.unsubscribe();
    second.unsubscribe();
    const handlers = kind === 'subscriber' ? q.broker.topicHandlers : q.broker.queueHandlers;
    assert.equal(handlers.has('jobs'), false, 'subscriptions still clean up');
  } finally {
    q.disconnect();
  }
}

for (const kind of ['subscriber', 'consumer']) {
  describe(`${kind} callback outcomes`, () => {
    for (const outcome of [
      'sync-throw', 'rejected-promise', 'delayed-rejection', 'rejecting-thenable',
      'throwing-then-getter', 'fulfilled-promise', 'pending-promise',
    ]) {
      test(`${outcome} stays isolated without blocking later delivery`, () => {
        const result = spawnSync(process.execPath, [
          '--unhandled-rejections=strict', '--input-type=module', '--eval',
          `await (${exerciseHandlers})(${JSON.stringify(bundle)}, ${JSON.stringify(kind)}, ${JSON.stringify(outcome)});`,
        ], { encoding: 'utf8', timeout: 5000 });
        assert.ifError(result.error);
        assert.equal(result.signal, null, result.stderr);
        assert.equal(result.status, 0, result.stderr || result.stdout);
      });
    }
  });
}
