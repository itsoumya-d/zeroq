// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com | +91 7031648617

import { EventEmitter } from './events';

export class DiscoveryClient extends EventEmitter {
  private ws: WebSocket | null = null;
  private url: string;

  constructor(url: string) {
    super();
    this.url = url;
  }

  connect() {
    if (typeof WebSocket === 'undefined') return;
    this.ws = new WebSocket(this.url);
    this.ws.onopen = () => this.emit('connected');
    this.ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      this.emit('message', msg);
    };
    this.ws.onclose = () => this.emit('disconnected');
  }

  send(msg: any) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  close() {
    this.ws?.close();
  }
}
