import { Message, Subscription, Consumer } from './types';
import { PeerMesh } from './peer-mesh';
import { PersistenceLayer } from './persistence';
import { Serializer } from './serializer';

export class MessageBroker {
  private peerMesh: PeerMesh;
  private persistence: PersistenceLayer;
  private topicHandlers: Map<string, Function[]> = new Map();
  private queueHandlers: Map<string, Function[]> = new Map();

  constructor(peerMesh: PeerMesh, persistence: PersistenceLayer) {
    this.peerMesh = peerMesh;
    this.persistence = persistence;
    
    this.peerMesh.on('message_received', this.handleMessage.bind(this));
  }

  private async handleMessage(data: string) {
    const msg = Serializer.deserialize(data);
    
    if (this.topicHandlers.has(msg.topic)) {
      this.topicHandlers.get(msg.topic)!.forEach(h => h(msg));
    }
    
    if (this.queueHandlers.has(msg.topic)) {
      const handlers = this.queueHandlers.get(msg.topic)!;
      // Round-robin
      const handler = handlers.shift();
      if (handler) {
        handler(msg, async () => {
           // ack
           await this.persistence.delete(msg.id);
        });
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
    
    await this.persistence.save(msg);
    this.peerMesh.sendToAll(Serializer.serialize(msg));
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

  consume(queue: string, handler: (msg: Message, ack: () => void) => void): Consumer {
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
