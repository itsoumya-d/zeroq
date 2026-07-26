export interface Message {
  id: string;
  topic: string;
  payload: any;
  timestamp: number;
  seq: number;
  priority?: number;
  replyTo?: string;
  retryCount: number;
}

export interface Subscription {
  topic: string;
  unsubscribe: () => void;
}

export interface Consumer {
  queue: string;
  unsubscribe: () => void;
}
