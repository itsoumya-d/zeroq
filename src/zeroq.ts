import { DiscoveryClient } from './discovery-client';
import { PeerMesh } from './peer-mesh';
import { PersistenceLayer } from './persistence';
import { MessageBroker } from './message-broker';
import { Message, Subscription, Consumer } from './types';

export class ZeroQ {
  private discoveryClient: DiscoveryClient;
  private peerMesh: PeerMesh;
  private persistence: PersistenceLayer;
  private broker: MessageBroker;

  constructor(discoveryUrl: string) {
    this.discoveryClient = new DiscoveryClient(discoveryUrl);
    this.persistence = new PersistenceLayer();
    this.peerMesh = new PeerMesh(this.discoveryClient);
    this.broker = new MessageBroker(this.peerMesh, this.persistence);
    
    this.init();
  }

  private async init() {
    await this.persistence.init();
    this.discoveryClient.connect();
  }

  async createTopic(topic: string): Promise<void> {
    // Notify discovery server
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
    await this.broker.publish(queue, message); // Simplified mapping queue to topic internally
  }

  async consume(queue: string, handler: (msg: Message, ack: () => void, nack: () => void) => void): Promise<Consumer> {
    this.discoveryClient.send({ type: 'consume', queue });
    return this.broker.consume(queue, handler);
  }

  async request(topic: string, message: any, timeoutMs: number = 5000): Promise<any> {
    const replyTo = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Timeout')), timeoutMs);
      
      const sub = this.subscribe(replyTo, (msg) => {
        clearTimeout(timeout);
        sub.then(s => s.unsubscribe());
        resolve(msg.payload);
      });
      
      this.publish(topic, { ...message, replyTo });
    });
  }

  async reply(topic: string, handler: (msg: Message) => any): Promise<void> {
    this.subscribe(topic, async (msg) => {
      if (msg.replyTo) {
        const response = await handler(msg);
        this.publish(msg.replyTo, response);
      }
    });
  }

  disconnect(): void {
    this.discoveryClient.close();
  }
}
