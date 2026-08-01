// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com

import { Message, Subscription, Consumer } from './types';
import { PeerMesh } from './peer-mesh';
import { PersistenceLayer } from './persistence';
import { Serializer } from './serializer';

const MAX_RETRIES = 3;
const SEEN_LIMIT = 10000;

export class MessageBroker {
  private peerMesh: PeerMesh;
  private persistence: PersistenceLayer;
  private topicHandlers: Map<string, Function[]> = new Map();
  private queueHandlers: Map<string, Function[]> = new Map();
  private seenMessages: Set<string> = new Set();
  private seenMessagesQueue: string[] = [];

  constructor(peerMesh: PeerMesh, persistence: PersistenceLayer) {
    this.peerMesh = peerMesh;
    this.persistence = persistence;

    this.peerMesh.on('message_received', (data: string) => {
      // handleMessage is async; without this catch a rejection inside it becomes
      // an unhandled rejection, which terminates a Node process by default.
      Promise.resolve(this.handleMessage(data)).catch((err) => {
        console.warn('ZeroQ: failed to handle inbound message', err);
      });
    });
  }

  /**
   * Duplicate suppression. Keyed on id + delivery attempt so that a nack()
   * retry (same id, higher retryCount) is not mistaken for a duplicate of the
   * original delivery — which previously made redelivery unreachable.
   */
  private trackMessage(id: string, attempt: number = 0): boolean {
    const key = `${id}:${attempt}`;
    if (this.seenMessages.has(key)) {
      return false; // Already seen
    }
    this.seenMessages.add(key);
    this.seenMessagesQueue.push(key);
    if (this.seenMessagesQueue.length > SEEN_LIMIT) {
      const oldest = this.seenMessagesQueue.shift();
      if (oldest) this.seenMessages.delete(oldest);
    }
    return true;
  }

  private async handleMessage(data: string) {
    let msg: Message;
    try {
      msg = Serializer.deserialize(data as string);
    } catch (err) {
      // A peer can send anything. Never let a bad frame escape as a rejection.
      console.warn('ZeroQ: dropping unparseable frame from peer');
      return;
    }

    // Validate the envelope before it reaches persistence or user handlers.
    if (!msg || typeof msg !== 'object'
      || typeof (msg as any).id !== 'string' || (msg as any).id === ''
      || typeof (msg as any).topic !== 'string') {
      console.warn('ZeroQ: dropping malformed message from peer (missing id/topic)');
      return;
    }
    if (typeof msg.retryCount !== 'number' || !Number.isFinite(msg.retryCount) || msg.retryCount < 0) {
      msg.retryCount = 0;
    }

    if (!this.trackMessage(msg.id, msg.retryCount)) {
      return;
    }

    try {
      await this.persistence.save(msg);
    } catch (err) {
      console.warn('ZeroQ: could not persist inbound message', err);
    }

    if (this.topicHandlers.has(msg.topic)) {
      // Copy before iterating: a handler may unsubscribe during dispatch.
      for (const h of [...this.topicHandlers.get(msg.topic)!]) {
        try {
          h(msg);
        } catch (err) {
          console.warn('ZeroQ: subscriber threw', err);
        }
      }
    }

    if (this.queueHandlers.has(msg.topic)) {
      const handlers = this.queueHandlers.get(msg.topic)!;
      const handler = handlers.shift();
      if (handler) {
        handlers.push(handler); // round-robin: rotate before invoking
        const ack = async () => {
          try {
            await this.persistence.delete(msg.id);
          } catch (err) {
            console.warn('ZeroQ: ack failed to remove message', err);
          }
        };
        const nack = async () => {
          try {
            msg.retryCount++;
            await this.persistence.save(msg);
            if (msg.retryCount < MAX_RETRIES) {
              this.peerMesh.broadcast(Serializer.serialize(msg));
            }
          } catch (err) {
            console.warn('ZeroQ: nack failed', err);
          }
        };
        try {
          handler(msg, ack, nack);
        } catch (err) {
          console.warn('ZeroQ: consumer threw', err);
        }
      }
    }
  }

  async publish(topic: string, message: any, options?: { priority?: number }) {
    const msg: Message = {
      id: crypto.randomUUID(),
      topic,
      payload: message,
      timestamp: Date.now(),
      seq: 0,
      retryCount: 0
    };
    if (options?.priority !== undefined) {
      msg.priority = options.priority;
    }

    // Serialize first: a non-serialisable payload must not leave a row behind
    // in persistence that can never be broadcast.
    const wire = Serializer.serialize(msg);

    this.trackMessage(msg.id, 0);
    try {
      await this.persistence.save(msg);
    } catch (err) {
      console.warn('ZeroQ: could not persist outbound message', err);
    }
    this.peerMesh.broadcast(wire);
  }

  subscribe(topic: string, handler: (msg: Message) => void): Subscription {
    if (!this.topicHandlers.has(topic)) {
      this.topicHandlers.set(topic, []);
    }
    this.topicHandlers.get(topic)!.push(handler);

    return {
      topic,
      unsubscribe: () => {
        const handlers = (this.topicHandlers.get(topic) || []).filter(h => h !== handler);
        // Drop the key entirely when empty, otherwise every request() reply
        // topic leaves a permanent empty-array entry in the Map.
        if (handlers.length === 0) this.topicHandlers.delete(topic);
        else this.topicHandlers.set(topic, handlers);
      }
    };
  }

  consume(queue: string, handler: (msg: Message, ack: () => void, nack: () => void) => void): Consumer {
    if (!this.queueHandlers.has(queue)) {
      this.queueHandlers.set(queue, []);
    }
    this.queueHandlers.get(queue)!.push(handler);

    return {
      queue,
      unsubscribe: () => {
        const handlers = (this.queueHandlers.get(queue) || []).filter(h => h !== handler);
        if (handlers.length === 0) this.queueHandlers.delete(queue);
        else this.queueHandlers.set(queue, handlers);
      }
    };
  }
}
