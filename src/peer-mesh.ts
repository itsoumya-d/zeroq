// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com

import { EventEmitter } from './events';

const MAX_WS_RECONNECT_ATTEMPTS = 10;
const MAX_PEER_RECONNECT_ATTEMPTS = 10;
const MAX_BACKOFF_MS = 30000;
const BUFFERED_AMOUNT_LIMIT = 65536;

export class PeerMesh extends EventEmitter {
  private peers: Map<string, { pc: RTCPeerConnection, dc: RTCDataChannel }> = new Map();
  private ws: WebSocket | null = null;
  private knownPeers: Set<string> = new Set();
  private reconnectTimeouts: Map<string, number> = new Map();
  private peerAttempts: Map<string, number> = new Map();
  private lastSeen: Map<string, number> = new Map();
  private pingIntervals: Map<string, any> = new Map();
  private pendingTimers: Set<any> = new Set();
  private discoveryUrl: string = '';
  private topicId: string = '';
  private wsReconnectAttempts: number = 0;
  private closed: boolean = false;

  constructor() {
    super();
  }

  /** setTimeout that is tracked, so disconnect() can cancel it. */
  private later(fn: () => void, ms: number): void {
    const t = setTimeout(() => {
      this.pendingTimers.delete(t);
      if (this.closed) return;
      fn();
    }, ms);
    this.pendingTimers.add(t);
  }

  private static jitter(delay: number): number {
    // README documents "exponential backoff with jitter"; apply +/-20%.
    return Math.round(delay * (0.8 + Math.random() * 0.4));
  }

  connect(discoveryUrl: string, topicId: string) {
    if (this.closed) return;
    this.discoveryUrl = discoveryUrl;
    this.topicId = topicId;
    if (typeof WebSocket === 'undefined') return;
    this.ws = new WebSocket(discoveryUrl);

    this.ws.onopen = () => {
      this.wsReconnectAttempts = 0;
      this.ws?.send(JSON.stringify({ type: 'join', topicId }));
    };

    this.ws.onmessage = async (e) => {
      try {
        const msg = JSON.parse(e.data);
        switch (msg.type) {
          case 'peer_joined':
            this.knownPeers.add(msg.peerId);
            this.connectToPeer(msg.peerId);
            break;
          case 'offer':
            await this.handleOffer(msg.peerId, msg.sdp);
            break;
          case 'answer':
            await this.handleAnswer(msg.peerId, msg.sdp);
            break;
          case 'ice_candidate':
            await this.handleIceCandidate(msg.peerId, msg.candidate);
            break;
        }
      } catch (err) { console.warn('ZeroQ: WebSocket message parse error', err); }
    };

    this.ws.onclose = () => {
      // A close caused by disconnect() must not schedule a reconnect. Without
      // this guard the backoff ladder kept the host process alive for ~181s
      // after the caller had already asked for shutdown.
      if (this.closed) return;
      if (this.wsReconnectAttempts >= MAX_WS_RECONNECT_ATTEMPTS) {
        console.warn('ZeroQ: Max WebSocket reconnect attempts reached');
        this.emit('connection_failed', 'Max reconnect attempts');
        return;
      }
      const backoff = PeerMesh.jitter(Math.min(1000 * Math.pow(2, this.wsReconnectAttempts), MAX_BACKOFF_MS));
      this.wsReconnectAttempts++;
      this.later(() => this.connect(discoveryUrl, topicId), backoff);
    };
  }

  async connectToPeer(peerId: string) {
    if (this.closed) return;
    if (this.peers.has(peerId)) return;
    this.knownPeers.add(peerId);

    if (typeof RTCPeerConnection === 'undefined') return;
    const pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });
    const dc = pc.createDataChannel(this.topicId);

    this.setupPeer(peerId, pc, dc);

    pc.onicecandidate = (e) => {
      if (e.candidate && this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: 'ice_candidate', peerId, candidate: e.candidate }));
      }
    };

    try {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: 'offer', peerId, sdp: pc.localDescription }));
      }
    } catch (err) {
      console.error('Error creating offer', err);
      this.handleConnectionFailure(peerId, 'offer-failed');
    }
  }

  private async handleOffer(peerId: string, sdp: RTCSessionDescriptionInit) {
    if (this.closed) return;
    this.knownPeers.add(peerId);
    if (this.peers.has(peerId)) return;

    if (typeof RTCPeerConnection === 'undefined') return;
    const pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });

    pc.ondatachannel = (e) => {
      this.setupPeer(peerId, pc, e.channel);
    };

    pc.onicecandidate = (e) => {
      if (e.candidate && this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: 'ice_candidate', peerId, candidate: e.candidate }));
      }
    };

    try {
      await pc.setRemoteDescription(new RTCSessionDescription(sdp));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);

      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: 'answer', peerId, sdp: pc.localDescription }));
      }
    } catch (err) {
      console.error('Error handling offer', err);
      this.handleConnectionFailure(peerId, 'answer-failed');
    }
  }

  private async handleAnswer(peerId: string, sdp: RTCSessionDescriptionInit) {
    const peer = this.peers.get(peerId);
    if (peer && typeof RTCSessionDescription !== 'undefined') {
      try {
        await peer.pc.setRemoteDescription(new RTCSessionDescription(sdp));
      } catch (err) {
        console.error('Error setting remote description', err);
      }
    }
  }

  private async handleIceCandidate(peerId: string, candidate: RTCIceCandidateInit) {
    const peer = this.peers.get(peerId);
    if (peer && typeof RTCIceCandidate !== 'undefined') {
      try {
        await peer.pc.addIceCandidate(new RTCIceCandidate(candidate));
      } catch (err) {
        console.error('Error adding ICE candidate', err);
      }
    }
  }

  private setupPeer(peerId: string, pc: RTCPeerConnection, dc: RTCDataChannel) {
    this.peers.set(peerId, { pc, dc });

    dc.onopen = () => {
      this.reconnectTimeouts.delete(peerId);
      this.peerAttempts.delete(peerId);
      this.lastSeen.set(peerId, Date.now());
      this.emit('peer_connected', peerId);

      const interval = setInterval(() => {
        if (this.closed) { clearInterval(interval); return; }
        if (dc.readyState === 'open') {
          dc.send(JSON.stringify({ type: '__ping__' }));
        }

        const last = this.lastSeen.get(peerId) || 0;
        if (Date.now() - last > 30000) {
          this.removePeer(peerId);
          this.handleConnectionFailure(peerId, 'ping-timeout');
        }
      }, 10000);
      this.pingIntervals.set(peerId, interval);

      const allPeers = Array.from(this.knownPeers);
      const gossipPeers = allPeers.length <= 10 ? allPeers : allPeers.sort(() => Math.random() - 0.5).slice(0, 10);
      dc.send(JSON.stringify({ type: '__gossip__', peers: gossipPeers }));
    };

    dc.onmessage = (e) => {
      this.lastSeen.set(peerId, Date.now());
      if (typeof e.data === 'string') {
        try {
          const msg = JSON.parse(e.data);
          if (msg.type === '__ping__') {
            dc.send(JSON.stringify({ type: '__pong__' }));
            return;
          }
          if (msg.type === '__pong__') {
            return;
          }
          if (msg.type === '__gossip__') {
            const peers = Array.isArray(msg.peers) ? (msg.peers as string[]) : [];
            for (const p of peers) {
              if (typeof p === 'string' && !this.knownPeers.has(p)) {
                this.knownPeers.add(p);
                this.connectToPeer(p);
              }
            }
            return;
          }
        } catch (err) { }
      }
      this.emit('message_received', e.data);
    };

    dc.onclose = () => {
      this.removePeer(peerId);
      this.handleConnectionFailure(peerId, 'datachannel-closed');
    };

    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === 'disconnected' || pc.iceConnectionState === 'failed') {
        // Surface the ICE state so callers can tell an unreachable network
        // (typically 'failed' behind symmetric/CGNAT with no TURN relay) apart
        // from a peer that simply left. peer_disconnected still fires.
        this.emit('peer_connection_failed', peerId, pc.iceConnectionState);
        this.removePeer(peerId);
        this.handleConnectionFailure(peerId, pc.iceConnectionState);
      }
    };
  }

  private removePeer(peerId: string) {
    const peer = this.peers.get(peerId);
    if (!peer) return; // Already removed — prevent double-fire
    this.peers.delete(peerId); // Delete FIRST to prevent re-entry
    try { peer.dc.close(); } catch { }
    try { peer.pc.close(); } catch { }
    const interval = this.pingIntervals.get(peerId);
    if (interval) {
      clearInterval(interval);
      this.pingIntervals.delete(peerId);
    }
    this.lastSeen.delete(peerId);
    this.emit('peer_disconnected', peerId);
  }

  private handleConnectionFailure(peerId: string, reason: string = 'unknown') {
    if (this.closed) return;

    const attempts = (this.peerAttempts.get(peerId) || 0) + 1;
    this.peerAttempts.set(peerId, attempts);
    if (attempts > MAX_PEER_RECONNECT_ATTEMPTS) {
      // Previously retried forever, so a peer that had permanently left kept a
      // timer and a Map entry alive indefinitely.
      console.warn(`ZeroQ: giving up on peer ${peerId} after ${MAX_PEER_RECONNECT_ATTEMPTS} attempts (${reason})`);
      this.emit('peer_unreachable', peerId, reason);
      this.reconnectTimeouts.delete(peerId);
      this.peerAttempts.delete(peerId);
      this.knownPeers.delete(peerId);
      return;
    }

    let delay = this.reconnectTimeouts.get(peerId) || 500;
    delay = Math.min(delay * 2, MAX_BACKOFF_MS);
    this.reconnectTimeouts.set(peerId, delay);

    this.later(() => {
      if (!this.peers.has(peerId)) {
        this.connectToPeer(peerId);
      }
    }, PeerMesh.jitter(delay));
  }

  broadcast(data: string | ArrayBuffer) {
    let dropped = 0;
    for (const [peerId, { dc }] of this.peers.entries()) {
      if (dc.readyState === 'open') {
        if (dc.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
          dropped++;
          this.emit('message_dropped', peerId, 'backpressure', dc.bufferedAmount);
          continue;
        }
        try {
          dc.send(data as any);
        } catch (err) {
          dropped++;
          this.emit('message_dropped', peerId, 'send-error', err);
          console.warn('ZeroQ: broadcast error', err);
        }
      }
    }
    return { peers: this.peers.size, dropped };
  }

  sendToPeer(peerId: string, data: string | ArrayBuffer) {
    const peer = this.peers.get(peerId);
    if (peer && peer.dc.readyState === 'open') {
      if (peer.dc.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
        this.emit('message_dropped', peerId, 'backpressure', peer.dc.bufferedAmount);
        return;
      }
      try {
        peer.dc.send(data as any);
      } catch (err) {
        this.emit('message_dropped', peerId, 'send-error', err);
        console.warn('ZeroQ: sendToPeer error', err);
      }
    }
  }

  disconnect() {
    this.closed = true;
    for (const t of this.pendingTimers) clearTimeout(t);
    this.pendingTimers.clear();
    for (const interval of this.pingIntervals.values()) clearInterval(interval);
    this.pingIntervals.clear();
    this.ws?.close();
    this.ws = null;
    for (const peerId of [...this.peers.keys()]) {
      this.removePeer(peerId);
    }
    this.reconnectTimeouts.clear();
    this.peerAttempts.clear();
    this.knownPeers.clear();
  }
}
