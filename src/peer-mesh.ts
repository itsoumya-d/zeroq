// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com | +91 7031648617

import { EventEmitter } from './events';

export class PeerMesh extends EventEmitter {
  private peers: Map<string, { pc: RTCPeerConnection, dc: RTCDataChannel }> = new Map();
  private ws: WebSocket | null = null;
  private knownPeers: Set<string> = new Set();
  private reconnectTimeouts: Map<string, number> = new Map();
  private lastSeen: Map<string, number> = new Map();
  private pingIntervals: Map<string, any> = new Map();
  private discoveryUrl: string = '';
  private topicId: string = '';
  private wsReconnectAttempts: number = 0;

  constructor() {
    super();
  }

  connect(discoveryUrl: string, topicId: string) {
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
        switch(msg.type) {
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
      if (this.wsReconnectAttempts >= 10) {
        console.warn('ZeroQ: Max WebSocket reconnect attempts reached');
        this.emit('connection_failed', 'Max reconnect attempts');
        return;
      }
      const backoff = Math.min(1000 * Math.pow(2, this.wsReconnectAttempts), 30000);
      this.wsReconnectAttempts++;
      setTimeout(() => this.connect(discoveryUrl, topicId), backoff);
    };
  }

  async connectToPeer(peerId: string) {
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
      this.handleConnectionFailure(peerId);
    }
  }

  private async handleOffer(peerId: string, sdp: RTCSessionDescriptionInit) {
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
      this.handleConnectionFailure(peerId);
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
      this.lastSeen.set(peerId, Date.now());
      this.emit('peer_connected', peerId);
      
      const interval = setInterval(() => {
        if (dc.readyState === 'open') {
          dc.send(JSON.stringify({ type: '__ping__' }));
        }
        
        const last = this.lastSeen.get(peerId) || 0;
        if (Date.now() - last > 30000) {
          this.removePeer(peerId);
          this.handleConnectionFailure(peerId);
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
            const peers = msg.peers as string[];
            for (const p of peers) {
              if (!this.knownPeers.has(p)) {
                this.knownPeers.add(p);
                this.connectToPeer(p);
              }
            }
            return;
          }
        } catch (err) {}
      }
      this.emit('message_received', e.data);
    };

    dc.onclose = () => {
      this.removePeer(peerId);
      this.handleConnectionFailure(peerId);
    };

    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === 'disconnected' || pc.iceConnectionState === 'failed') {
        this.removePeer(peerId);
        this.handleConnectionFailure(peerId);
      }
    };
  }

  private removePeer(peerId: string) {
    const peer = this.peers.get(peerId);
    if (peer) {
      peer.dc.close();
      peer.pc.close();
      this.peers.delete(peerId);
    }
    const interval = this.pingIntervals.get(peerId);
    if (interval) {
      clearInterval(interval);
      this.pingIntervals.delete(peerId);
    }
    this.emit('peer_disconnected', peerId);
  }

  private handleConnectionFailure(peerId: string) {
    let delay = this.reconnectTimeouts.get(peerId) || 500;
    delay = Math.min(delay * 2, 30000);
    this.reconnectTimeouts.set(peerId, delay);
    
    setTimeout(() => {
      if (!this.peers.has(peerId)) {
        this.connectToPeer(peerId);
      }
    }, delay);
  }

  broadcast(data: string | ArrayBuffer) {
    for (const { dc } of this.peers.values()) {
      if (dc.readyState === 'open') {
        if (dc.bufferedAmount > 65536) continue;
        try {
          dc.send(data as any);
        } catch (err) {
          console.warn('ZeroQ: broadcast error', err);
        }
      }
    }
  }

  sendToPeer(peerId: string, data: string | ArrayBuffer) {
    const peer = this.peers.get(peerId);
    if (peer && peer.dc.readyState === 'open') {
      if (peer.dc.bufferedAmount > 65536) return;
      try {
        peer.dc.send(data as any);
      } catch (err) {
        console.warn('ZeroQ: sendToPeer error', err);
      }
    }
  }

  disconnect() {
    this.ws?.close();
    for (const peerId of this.peers.keys()) {
      this.removePeer(peerId);
    }
    this.reconnectTimeouts.clear();
  }
}
