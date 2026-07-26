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
