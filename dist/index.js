"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.ts
var index_exports = {};
__export(index_exports, {
  ZeroQ: () => ZeroQ
});
module.exports = __toCommonJS(index_exports);

// src/license-validator.ts
var LicenseValidator = class {
  static AUTHOR = "Soumya Debnath";
  static CONTACT = "soumyadebnath1661@gmail.com";
  static validate(options) {
    const key = options?.licenseKey || (typeof process !== "undefined" ? process.env.COMMERCIAL_LICENSE_KEY : void 0);
    const isDev = typeof window !== "undefined" ? window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1" : typeof process !== "undefined" && process.env.NODE_ENV !== "production";
    if (isDev || options?.allowEval) {
      return true;
    }
    if (!key || !key.startsWith("BSL11-")) {
      console.warn(`
================================================================================
\u{1F512} COMMERCIAL USE WARNING \u2014 BUSINESS SOURCE LICENSE 1.1 REQUIRED
Product: ZEROQ | Copyright (c) 2024-2026 Soumya Debnath

Production use of this software requires a valid paid commercial license key.
Unlicensed commercial deployment constitutes copyright infringement under DMCA \xA7 1201.

Purchase a commercial license key:
\u{1F4E7} Email: soumyadebnath1661@gmail.com | \u{1F4DE} Phone: +91 7031648617
================================================================================
      `);
      return false;
    }
    return true;
  }
};

// src/events.ts
var EventEmitter = class {
  listeners = {};
  on(event, fn) {
    if (!this.listeners[event]) this.listeners[event] = [];
    this.listeners[event].push(fn);
  }
  off(event, fn) {
    if (!this.listeners[event]) return;
    this.listeners[event] = this.listeners[event].filter((l) => l !== fn);
  }
  emit(event, ...args) {
    if (!this.listeners[event]) return;
    this.listeners[event].forEach((fn) => fn(...args));
  }
};

// src/discovery-client.ts
var DiscoveryClient = class extends EventEmitter {
  ws = null;
  url;
  constructor(url) {
    super();
    this.url = url;
  }
  connect() {
    if (typeof WebSocket === "undefined") return;
    this.ws = new WebSocket(this.url);
    this.ws.onopen = () => this.emit("connected");
    this.ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      this.emit("message", msg);
    };
    this.ws.onclose = () => this.emit("disconnected");
  }
  send(msg) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }
  close() {
    this.ws?.close();
  }
};

// src/peer-mesh.ts
var PeerMesh = class extends EventEmitter {
  peers = /* @__PURE__ */ new Map();
  ws = null;
  knownPeers = /* @__PURE__ */ new Set();
  reconnectTimeouts = /* @__PURE__ */ new Map();
  lastSeen = /* @__PURE__ */ new Map();
  pingIntervals = /* @__PURE__ */ new Map();
  discoveryUrl = "";
  topicId = "";
  wsReconnectAttempts = 0;
  constructor() {
    super();
  }
  connect(discoveryUrl, topicId) {
    this.discoveryUrl = discoveryUrl;
    this.topicId = topicId;
    if (typeof WebSocket === "undefined") return;
    this.ws = new WebSocket(discoveryUrl);
    this.ws.onopen = () => {
      this.wsReconnectAttempts = 0;
      this.ws?.send(JSON.stringify({ type: "join", topicId }));
    };
    this.ws.onmessage = async (e) => {
      try {
        const msg = JSON.parse(e.data);
        switch (msg.type) {
          case "peer_joined":
            this.knownPeers.add(msg.peerId);
            this.connectToPeer(msg.peerId);
            break;
          case "offer":
            await this.handleOffer(msg.peerId, msg.sdp);
            break;
          case "answer":
            await this.handleAnswer(msg.peerId, msg.sdp);
            break;
          case "ice_candidate":
            await this.handleIceCandidate(msg.peerId, msg.candidate);
            break;
        }
      } catch (err) {
        console.warn("ZeroQ: WebSocket message parse error", err);
      }
    };
    this.ws.onclose = () => {
      if (this.wsReconnectAttempts >= 10) {
        console.warn("ZeroQ: Max WebSocket reconnect attempts reached");
        this.emit("connection_failed", "Max reconnect attempts");
        return;
      }
      const backoff = Math.min(1e3 * Math.pow(2, this.wsReconnectAttempts), 3e4);
      this.wsReconnectAttempts++;
      setTimeout(() => this.connect(discoveryUrl, topicId), backoff);
    };
  }
  async connectToPeer(peerId) {
    if (this.peers.has(peerId)) return;
    this.knownPeers.add(peerId);
    if (typeof RTCPeerConnection === "undefined") return;
    const pc = new RTCPeerConnection({ iceServers: [{ urls: "stun:stun.l.google.com:19302" }] });
    const dc = pc.createDataChannel(this.topicId);
    this.setupPeer(peerId, pc, dc);
    pc.onicecandidate = (e) => {
      if (e.candidate && this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: "ice_candidate", peerId, candidate: e.candidate }));
      }
    };
    try {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: "offer", peerId, sdp: pc.localDescription }));
      }
    } catch (err) {
      console.error("Error creating offer", err);
      this.handleConnectionFailure(peerId);
    }
  }
  async handleOffer(peerId, sdp) {
    this.knownPeers.add(peerId);
    if (this.peers.has(peerId)) return;
    if (typeof RTCPeerConnection === "undefined") return;
    const pc = new RTCPeerConnection({ iceServers: [{ urls: "stun:stun.l.google.com:19302" }] });
    pc.ondatachannel = (e) => {
      this.setupPeer(peerId, pc, e.channel);
    };
    pc.onicecandidate = (e) => {
      if (e.candidate && this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: "ice_candidate", peerId, candidate: e.candidate }));
      }
    };
    try {
      await pc.setRemoteDescription(new RTCSessionDescription(sdp));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: "answer", peerId, sdp: pc.localDescription }));
      }
    } catch (err) {
      console.error("Error handling offer", err);
      this.handleConnectionFailure(peerId);
    }
  }
  async handleAnswer(peerId, sdp) {
    const peer = this.peers.get(peerId);
    if (peer && typeof RTCSessionDescription !== "undefined") {
      try {
        await peer.pc.setRemoteDescription(new RTCSessionDescription(sdp));
      } catch (err) {
        console.error("Error setting remote description", err);
      }
    }
  }
  async handleIceCandidate(peerId, candidate) {
    const peer = this.peers.get(peerId);
    if (peer && typeof RTCIceCandidate !== "undefined") {
      try {
        await peer.pc.addIceCandidate(new RTCIceCandidate(candidate));
      } catch (err) {
        console.error("Error adding ICE candidate", err);
      }
    }
  }
  setupPeer(peerId, pc, dc) {
    this.peers.set(peerId, { pc, dc });
    dc.onopen = () => {
      this.reconnectTimeouts.delete(peerId);
      this.lastSeen.set(peerId, Date.now());
      this.emit("peer_connected", peerId);
      const interval = setInterval(() => {
        if (dc.readyState === "open") {
          dc.send(JSON.stringify({ type: "__ping__" }));
        }
        const last = this.lastSeen.get(peerId) || 0;
        if (Date.now() - last > 3e4) {
          this.removePeer(peerId);
          this.handleConnectionFailure(peerId);
        }
      }, 1e4);
      this.pingIntervals.set(peerId, interval);
      const allPeers = Array.from(this.knownPeers);
      const gossipPeers = allPeers.length <= 10 ? allPeers : allPeers.sort(() => Math.random() - 0.5).slice(0, 10);
      dc.send(JSON.stringify({ type: "__gossip__", peers: gossipPeers }));
    };
    dc.onmessage = (e) => {
      this.lastSeen.set(peerId, Date.now());
      if (typeof e.data === "string") {
        try {
          const msg = JSON.parse(e.data);
          if (msg.type === "__ping__") {
            dc.send(JSON.stringify({ type: "__pong__" }));
            return;
          }
          if (msg.type === "__pong__") {
            return;
          }
          if (msg.type === "__gossip__") {
            const peers = msg.peers;
            for (const p of peers) {
              if (!this.knownPeers.has(p)) {
                this.knownPeers.add(p);
                this.connectToPeer(p);
              }
            }
            return;
          }
        } catch (err) {
        }
      }
      this.emit("message_received", e.data);
    };
    dc.onclose = () => {
      this.removePeer(peerId);
      this.handleConnectionFailure(peerId);
    };
    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === "disconnected" || pc.iceConnectionState === "failed") {
        this.removePeer(peerId);
        this.handleConnectionFailure(peerId);
      }
    };
  }
  removePeer(peerId) {
    const peer = this.peers.get(peerId);
    if (!peer) return;
    this.peers.delete(peerId);
    try {
      peer.dc.close();
    } catch {
    }
    try {
      peer.pc.close();
    } catch {
    }
    const interval = this.pingIntervals.get(peerId);
    if (interval) {
      clearInterval(interval);
      this.pingIntervals.delete(peerId);
    }
    this.emit("peer_disconnected", peerId);
  }
  handleConnectionFailure(peerId) {
    let delay = this.reconnectTimeouts.get(peerId) || 500;
    delay = Math.min(delay * 2, 3e4);
    this.reconnectTimeouts.set(peerId, delay);
    setTimeout(() => {
      if (!this.peers.has(peerId)) {
        this.connectToPeer(peerId);
      }
    }, delay);
  }
  broadcast(data) {
    for (const { dc } of this.peers.values()) {
      if (dc.readyState === "open") {
        if (dc.bufferedAmount > 65536) continue;
        try {
          dc.send(data);
        } catch (err) {
          console.warn("ZeroQ: broadcast error", err);
        }
      }
    }
  }
  sendToPeer(peerId, data) {
    const peer = this.peers.get(peerId);
    if (peer && peer.dc.readyState === "open") {
      if (peer.dc.bufferedAmount > 65536) return;
      try {
        peer.dc.send(data);
      } catch (err) {
        console.warn("ZeroQ: sendToPeer error", err);
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
};

// src/persistence.ts
var PersistenceLayer = class {
  dbName = "zeroq-db";
  storeName = "messages";
  db = null;
  async init() {
    if (typeof indexedDB === "undefined") return;
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 1);
      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        this.db = request.result;
        resolve();
      };
      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        if (!db.objectStoreNames.contains(this.storeName)) {
          db.createObjectStore(this.storeName, { keyPath: "id" });
        }
      };
    });
  }
  async save(message) {
    if (!this.db) return;
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(this.storeName, "readwrite");
      const store = transaction.objectStore(this.storeName);
      const request = store.put(message);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }
  async delete(id) {
    if (!this.db) return;
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(this.storeName, "readwrite");
      const store = transaction.objectStore(this.storeName);
      const request = store.delete(id);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }
  async getDeadLetterQueue() {
    if (!this.db) return [];
    return new Promise((resolve, reject) => {
      const transaction = this.db.transaction(this.storeName, "readonly");
      const store = transaction.objectStore(this.storeName);
      const request = store.getAll();
      request.onsuccess = () => {
        const msgs = request.result;
        resolve(msgs.filter((m) => m.retryCount >= 3));
      };
      request.onerror = () => reject(request.error);
    });
  }
};

// src/serializer.ts
var Serializer = class {
  static serialize(msg) {
    return JSON.stringify(msg);
  }
  static deserialize(data) {
    return JSON.parse(data);
  }
};

// src/message-broker.ts
var MessageBroker = class {
  peerMesh;
  persistence;
  topicHandlers = /* @__PURE__ */ new Map();
  queueHandlers = /* @__PURE__ */ new Map();
  seenMessages = /* @__PURE__ */ new Set();
  seenMessagesQueue = [];
  constructor(peerMesh, persistence) {
    this.peerMesh = peerMesh;
    this.persistence = persistence;
    this.peerMesh.on("message_received", this.handleMessage.bind(this));
  }
  trackMessage(id) {
    if (this.seenMessages.has(id)) {
      return false;
    }
    this.seenMessages.add(id);
    this.seenMessagesQueue.push(id);
    if (this.seenMessagesQueue.length > 1e4) {
      const oldest = this.seenMessagesQueue.shift();
      if (oldest) this.seenMessages.delete(oldest);
    }
    return true;
  }
  async handleMessage(data) {
    const msg = Serializer.deserialize(data);
    if (!this.trackMessage(msg.id)) {
      return;
    }
    await this.persistence.save(msg);
    if (this.topicHandlers.has(msg.topic)) {
      this.topicHandlers.get(msg.topic).forEach((h) => h(msg));
    }
    if (this.queueHandlers.has(msg.topic)) {
      const handlers = this.queueHandlers.get(msg.topic);
      const handler = handlers.shift();
      if (handler) {
        handler(
          msg,
          async () => {
            await this.persistence.delete(msg.id);
          },
          async () => {
            msg.retryCount++;
            await this.persistence.save(msg);
            if (msg.retryCount < 3) {
              this.peerMesh.broadcast(Serializer.serialize(msg));
            }
          }
        );
        handlers.push(handler);
      }
    }
  }
  async publish(topic, message) {
    const msg = {
      id: crypto.randomUUID(),
      topic,
      payload: message,
      timestamp: Date.now(),
      seq: 0,
      retryCount: 0
    };
    this.trackMessage(msg.id);
    await this.persistence.save(msg);
    this.peerMesh.broadcast(Serializer.serialize(msg));
  }
  subscribe(topic, handler) {
    if (!this.topicHandlers.has(topic)) {
      this.topicHandlers.set(topic, []);
    }
    this.topicHandlers.get(topic).push(handler);
    return {
      topic,
      unsubscribe: () => {
        const handlers = this.topicHandlers.get(topic) || [];
        this.topicHandlers.set(topic, handlers.filter((h) => h !== handler));
      }
    };
  }
  consume(queue, handler) {
    if (!this.queueHandlers.has(queue)) {
      this.queueHandlers.set(queue, []);
    }
    this.queueHandlers.get(queue).push(handler);
    return {
      queue,
      unsubscribe: () => {
        const handlers = this.queueHandlers.get(queue) || [];
        this.queueHandlers.set(queue, handlers.filter((h) => h !== handler));
      }
    };
  }
};

// src/zeroq.ts
var ZeroQ = class {
  discoveryClient;
  peerMesh;
  persistence;
  broker;
  constructor(options) {
    LicenseValidator.validate(options);
    const discoveryUrl = options?.discoveryUrl || "ws://localhost:8080";
    this.discoveryClient = new DiscoveryClient(discoveryUrl);
    this.persistence = new PersistenceLayer();
    this.peerMesh = new PeerMesh();
    this.broker = new MessageBroker(this.peerMesh, this.persistence);
    this.init();
  }
  async init() {
    await this.persistence.init();
    this.discoveryClient.connect();
    this.peerMesh.connect("ws://localhost:8080", "default-topic");
  }
  async createTopic(topic) {
    this.discoveryClient.send({ type: "create_topic", topic });
  }
  async publish(topic, message) {
    await this.broker.publish(topic, message);
  }
  async subscribe(topic, handler) {
    this.discoveryClient.send({ type: "subscribe", topic });
    return this.broker.subscribe(topic, handler);
  }
  async createQueue(queue) {
    this.discoveryClient.send({ type: "create_queue", queue });
  }
  async enqueue(queue, message, options) {
    await this.broker.publish(queue, message);
  }
  async consume(queue, handler) {
    this.discoveryClient.send({ type: "consume", queue });
    return this.broker.consume(queue, handler);
  }
  async request(topic, message, timeoutMs = 5e3) {
    const replyTo = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Timeout")), timeoutMs);
      const sub = this.subscribe(replyTo, (msg) => {
        clearTimeout(timeout);
        sub.then((s) => s.unsubscribe());
        resolve(msg.payload);
      });
      this.publish(topic, { ...message, replyTo });
    });
  }
  async reply(topic, handler) {
    this.subscribe(topic, async (msg) => {
      if (msg.replyTo) {
        const response = await handler(msg);
        this.publish(msg.replyTo, response);
      }
    });
  }
  disconnect() {
    this.discoveryClient.close();
    this.peerMesh.disconnect();
  }
};
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  ZeroQ
});
