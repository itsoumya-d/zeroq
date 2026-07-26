// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com | +91 7031648617

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
