// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1619@gmail.com

export interface Message {
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

export interface Subscription {
  topic: string;
  unsubscribe: () => void;
}

export interface Consumer {
  queue: string;
  unsubscribe: () => void;
}

export interface ZeroQOptions {
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
