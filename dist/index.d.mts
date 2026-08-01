interface Message {
    id: string;
    topic: string;
    payload: any;
    timestamp: number;
    seq: number;
    priority?: number;
    replyTo?: string;
    retryCount: number;
}
interface Subscription {
    topic: string;
    unsubscribe: () => void;
}
interface Consumer {
    queue: string;
    unsubscribe: () => void;
}

declare class ZeroQ {
    private discoveryClient;
    private peerMesh;
    private persistence;
    private broker;
    constructor(options?: any);
    private init;
    createTopic(topic: string): Promise<void>;
    publish(topic: string, message: any): Promise<void>;
    subscribe(topic: string, handler: (msg: Message) => void): Promise<Subscription>;
    createQueue(queue: string): Promise<void>;
    enqueue(queue: string, message: any, options?: {
        priority?: number;
        delay?: number;
    }): Promise<void>;
    consume(queue: string, handler: (msg: Message, ack: () => void, nack: () => void) => void): Promise<Consumer>;
    request(topic: string, message: any, timeoutMs?: number): Promise<any>;
    reply(topic: string, handler: (msg: Message) => any): Promise<void>;
    disconnect(): void;
}

export { type Consumer, type Message, type Subscription, ZeroQ };
