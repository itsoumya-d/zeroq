import { LicenseValidator } from "./license-validator";
// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1619@gmail.com

import { DiscoveryClient } from './discovery-client';
import { PeerMesh } from './peer-mesh';
import { PersistenceLayer } from './persistence';
import { MessageBroker } from './message-broker';
import { Message, Subscription, Consumer, ZeroQOptions } from './types';

export class ZeroQ {
  private discoveryClient: DiscoveryClient;
  private peerMesh: PeerMesh;
  private persistence: PersistenceLayer;
  private broker: MessageBroker;
  private discoveryUrl: string;
  private topicId: string;

  constructor(options?: ZeroQOptions) {
    LicenseValidator.validate(options);
    this.discoveryUrl = options?.discoveryUrl || 'ws://localhost:8080';
    this.topicId = options?.topicId || 'default-topic';
    this.discoveryClient = new DiscoveryClient(this.discoveryUrl);
    this.persistence = new PersistenceLayer();
    this.peerMesh = new PeerMesh();
    this.broker = new MessageBroker(this.peerMesh, this.persistence);

    // init() is async and intentionally not awaited by the constructor; catch
    // here so a persistence failure can never become an unhandled rejection
    // (which terminates a Node process by default).
    this.init().catch((err) => {
      console.warn('ZeroQ: initialisation failed', err);
    });
  }

  private async init() {
    try {
      await this.persistence.init();
    } catch (err) {
      // IndexedDB can be unavailable (private browsing, blocked storage) or
      // absent entirely (Node). Continue without durability rather than
      // leaving the instance half-constructed.
      console.warn('ZeroQ: persistence unavailable — running without durability', err);
    }
    this.discoveryClient.connect();
    // Use the configured discovery URL and topic id. Previously both were
    // hardcoded to 'ws://localhost:8080' / 'default-topic', so options.discoveryUrl
    // was silently ignored for the socket that actually carries WebRTC signaling.
    this.peerMesh.connect(this.discoveryUrl, this.topicId);
  }

  async createTopic(topic: string): Promise<void> {
    this.discoveryClient.send({ type: 'create_topic', topic });
  }

  async publish(topic: string, message: any): Promise<void> {
    await this.broker.publish(topic, message);
  }

  async subscribe(topic: string, handler: (msg: Message) => void): Promise<Subscription> {
    this.discoveryClient.send({ type: 'subscribe', topic });
    return this.broker.subscribe(topic, handler);
  }

  async createQueue(queue: string): Promise<void> {
    this.discoveryClient.send({ type: 'create_queue', queue });
  }

  async enqueue(queue: string, message: any, options?: { priority?: number; delay?: number }): Promise<void> {
    // `priority` is carried on the message envelope so consumers can read it.
    // NOTE: it does not reorder delivery — there is no priority scheduler.
    // `delay` is accepted for API compatibility and is not implemented.
    await this.broker.publish(queue, message, { priority: options?.priority });
  }

  async consume(queue: string, handler: (msg: Message, ack: () => void, nack: () => void) => void): Promise<Consumer> {
    this.discoveryClient.send({ type: 'consume', queue });
    return this.broker.consume(queue, handler);
  }

  async request(topic: string, message: any, timeoutMs: number = 5000): Promise<any> {
    const replyTo = crypto.randomUUID();
    let sub: Subscription | undefined;
    let timer: any;
    try {
      return await new Promise<any>((resolve, reject) => {
        timer = setTimeout(() => reject(new Error('Timeout')), timeoutMs);
        // Register the reply handler synchronously, before publishing, so a
        // fast reply cannot arrive before the subscription exists.
        sub = this.broker.subscribe(replyTo, (msg) => resolve(msg.payload));
        this.discoveryClient.send({ type: 'subscribe', topic: replyTo });
        this.publish(topic, { ...message, replyTo }).catch(reject);
      });
    } finally {
      // Runs on resolve, reject AND timeout. Without this, every timed-out
      // request() permanently leaked a handler plus a Map entry keyed by a
      // fresh UUID, growing without bound.
      clearTimeout(timer);
      sub?.unsubscribe();
    }
  }

  async reply(topic: string, handler: (msg: Message) => any): Promise<void> {
    await this.subscribe(topic, async (msg) => {
      // request() writes replyTo into the message body, which publish() places
      // under `payload`. Read both locations so RPC actually round-trips.
      const replyTo = msg.replyTo
        ?? (msg.payload && typeof msg.payload === 'object' ? (msg.payload as any).replyTo : undefined);
      if (!replyTo) return;
      try {
        const response = await handler(msg);
        await this.publish(replyTo, response);
      } catch (err) {
        console.warn('ZeroQ: reply handler failed', err);
      }
    });
  }

  disconnect(): void {
    this.discoveryClient.close();
    this.peerMesh.disconnect();
  }
}
