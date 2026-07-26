// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
// Licensed under the Business Source License 1.1 (BSL 1.1).
// See LICENSE file for details. Production use requires a paid license.
// Contact: soumyadebnath1661@gmail.com | +91 7031648617

import { EventEmitter } from './events';
import { DiscoveryClient } from './discovery-client';

export class PeerMesh extends EventEmitter {
  private peers: Map<string, RTCDataChannel> = new Map();
  private discovery: DiscoveryClient;

  constructor(discovery: DiscoveryClient) {
    super();
    this.discovery = discovery;
    this.discovery.on('message', this.handleSignaling.bind(this));
  }

  private async handleSignaling(msg: any) {
    // Basic WebRTC signaling placeholder
    if (msg.type === 'peer_joined') {
      this.connectToPeer(msg.peerId);
    }
  }

  private async connectToPeer(peerId: string) {
    // Simplified peer connection logic
    // In a real implementation, this would handle offers/answers/ICE candidates
    this.emit('peer_connected', peerId);
  }

  sendToAll(data: string) {
    this.peers.forEach(channel => {
      if (channel.readyState === 'open') {
        channel.send(data);
      }
    });
  }

  sendToPeer(peerId: string, data: string) {
    const channel = this.peers.get(peerId);
    if (channel && channel.readyState === 'open') {
      channel.send(data);
    }
  }
}
