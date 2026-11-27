interface Message {
    id: string;
    topic: string;
    payload: any;
    timestamp: number;
    seq: number;
    priority?: number;
    ttl?: number;
    expiresAt?: number;
    replyTo?: string;
    retryCount: number;
    dlqReason?: string;
    originalTopic?: string;
    failedAt?: number;
}
interface Subscription {
    topic: string;
    unsubscribe: () => void;
}
interface Consumer {
    queue: string;
    unsubscribe: () => void;
}
interface ZeroQOptions {
    /** WebSocket URL of the Go discovery/signaling server. Default 'ws://localhost:8080'. */
    discoveryUrl?: string;
    /** Signaling room joined by this instance. Peers must share it to see each other. Default 'default-topic'. */
    topicId?: string;
    /** Commercial licence key (see LICENSE / COMMERCIAL_LICENSE.md). */
    licenseKey?: string;
    /** Suppress the commercial-use notice during evaluation. */
    allowEval?: boolean;
    /** Maximum retry attempts before routing message to Dead Letter Queue. Default 3. */
    maxRetries?: number;
    /** Topic suffix for routed Dead Letter Queue messages. Default '.dlq'. */
    dlqSuffix?: string;
}

declare class ZeroQ {
    private discoveryClient;
    private peerMesh;
    private persistence;
    private broker;
    private discoveryUrl;
    private topicId;
    constructor(options?: ZeroQOptions);
    private init;
    createTopic(topic: string): Promise<void>;
    publish(topic: string, message: any, options?: {
        priority?: number;
        ttl?: number;
    }): Promise<void>;
    subscribe(topic: string, handler: (msg: Message) => void): Promise<Subscription>;
    createQueue(queue: string): Promise<void>;
    enqueue(queue: string, message: any, options?: {
        priority?: number;
        delay?: number;
        ttl?: number;
    }): Promise<void>;
    consume(queue: string, handler: (msg: Message, ack: () => void, nack: () => void) => void): Promise<Consumer>;
    request(topic: string, message: any, timeoutMs?: number): Promise<any>;
    reply(topic: string, handler: (msg: Message) => any): Promise<void>;
    disconnect(): void;
}

interface RubricCheck {
    id: string;
    name: string;
    description: string;
    weight: number;
    mechanical: boolean;
}
interface Rubric {
    name: string;
    version: number;
    passing_threshold: number;
    checks: RubricCheck[];
}
interface VerificationViolation {
    check_id: string;
    name: string;
    weight: number;
    mechanical: boolean;
    reason: string;
}
interface VerificationResult {
    score: number;
    passing_threshold: number;
    passed: boolean;
    violations: VerificationViolation[];
}
interface RepairRoundLog {
    round: number;
    timestamp: number;
    score: number;
    status: 'passed' | 'repaired' | 'unresolved_non_mechanical';
    repairs?: string[];
    non_mechanical_violations?: VerificationViolation[];
}
interface RepairResult {
    rounds: number;
    initial_score: number;
    final_score: number;
    passed: boolean;
    message: Partial<Message>;
    rounds_log: RepairRoundLog[];
    unresolved: VerificationViolation[];
}
declare const DEFAULT_RUBRIC: Rubric;
declare function verifyEnvelope(msg: Partial<Message>, rubric?: Rubric): VerificationResult;
declare function repairEnvelope(msg: Partial<Message>, rubric?: Rubric, maxRounds?: number): RepairResult;

export { type Consumer, DEFAULT_RUBRIC, type Message, type RepairResult, type RepairRoundLog, type Rubric, type RubricCheck, type Subscription, type VerificationResult, type VerificationViolation, ZeroQ, type ZeroQOptions, repairEnvelope, verifyEnvelope };
