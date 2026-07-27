// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com | +91 7031648617

import { Message, Subscription, Consumer } from './types';
import { PeerMesh } from './peer-mesh';
import { PersistenceLayer } from './persistence';
import { Serializer } from './serializer';

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
    
    this.peerMesh.on('message_received', this.handleMessage.bind(this));
  }

  private trackMessage(id: string): boolean {
    if (this.seenMessages.has(id)) {
      return false; // Already seen
    }
    this.seenMessages.add(id);
    this.seenMessagesQueue.push(id);
    if (this.seenMessagesQueue.length > 10000) {
      const oldest = this.seenMessagesQueue.shift();
      if (oldest) this.seenMessages.delete(oldest);
    }
    return true;
  }

  private async handleMessage(data: string) {
    const msg = Serializer.deserialize(data);
    
    if (!this.trackMessage(msg.id)) {
      return;
    }
    
    await this.persistence.save(msg);
    
    if (this.topicHandlers.has(msg.topic)) {
      this.topicHandlers.get(msg.topic)!.forEach(h => h(msg));
    }
    
    if (this.queueHandlers.has(msg.topic)) {
      const handlers = this.queueHandlers.get(msg.topic)!;
      const handler = handlers.shift();
      if (handler) {
        handler(
          msg, 
          async () => {
            await this.persistence.delete(msg.id);
          },
          async () => {
            msg.retryCount++;
            await this.persistence.save(msg);
            if (msg.retryCount < 3) {
              this.peerMesh.broadcast(Serializer.serialize(msg));
            }
          }
        );
        handlers.push(handler);
      }
    }
  }

  async publish(topic: string, message: any) {
    const msg: Message = {
      id: crypto.randomUUID(),
      topic,
      payload: message,
      timestamp: Date.now(),
      seq: 0,
      retryCount: 0
    };
    
    this.trackMessage(msg.id);
    await this.persistence.save(msg);
    this.peerMesh.broadcast(Serializer.serialize(msg));
  }

  subscribe(topic: string, handler: (msg: Message) => void): Subscription {
    if (!this.topicHandlers.has(topic)) {
      this.topicHandlers.set(topic, []);
    }
    this.topicHandlers.get(topic)!.push(handler);
    
    return {
      topic,
      unsubscribe: () => {
        const handlers = this.topicHandlers.get(topic) || [];
        this.topicHandlers.set(topic, handlers.filter(h => h !== handler));
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
        const handlers = this.queueHandlers.get(queue) || [];
        this.queueHandlers.set(queue, handlers.filter(h => h !== handler));
      }
    };
  }
}
