// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com

import { EventEmitter } from './events';

export class DiscoveryClient extends EventEmitter {
  private ws: WebSocket | null = null;
  private url: string;
  private closed: boolean = false;

  constructor(url: string) {
    super();
    this.url = url;
  }

  connect() {
    // ZeroQ.init() is async, so close() can run before connect() does. Without
    // this guard the socket was opened after shutdown had been requested and was
    // then never closed, leaving the host process unable to exit.
    if (this.closed) return;
    if (typeof WebSocket === 'undefined') return;
    this.ws = new WebSocket(this.url);
    this.ws.onopen = () => {
      if (this.closed) { try { this.ws?.close(); } catch { } return; }
      this.emit('connected');
    };
    this.ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        this.emit('message', msg);
      } catch (err) {
        // A signaling server can send anything; never let a bad frame throw out
        // of the socket callback.
        console.warn('ZeroQ: discovery message parse error', err);
      }
    };
    this.ws.onclose = () => this.emit('disconnected');
  }

  send(msg: any) {
    if (this.closed) return;
    if (typeof WebSocket === 'undefined') return;
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  close() {
    this.closed = true;
    try { this.ws?.close(); } catch { }
    this.ws = null;
  }
}
