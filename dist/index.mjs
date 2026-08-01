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
See LICENSE and COMMERCIAL_LICENSE.md for the applicable terms.

Purchase a commercial license key:
\u{1F4E7} soumyadebnath1661@gmail.com
   https://github.com/itsoumya-d/zeroq/blob/main/COMMERCIAL_LICENSE.md
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
  closed = false;
  constructor(url) {
    super();
    this.url = url;
  }
  connect() {
    if (this.closed) return;
    if (typeof WebSocket === "undefined") return;
    this.ws = new WebSocket(this.url);
    this.ws.onopen = () => {
      if (this.closed) {
        try {
          this.ws?.close();
        } catch {
        }
        return;
      }
      this.emit("connected");
    };
    this.ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        this.emit("message", msg);
      } catch (err) {
        console.warn("ZeroQ: discovery message parse error", err);
      }
    };
    this.ws.onclose = () => this.emit("disconnected");
  }
  send(msg) {
    if (this.closed) return;
    if (typeof WebSocket === "undefined") return;
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }
  close() {
    this.closed = true;
    try {
      this.ws?.close();
    } catch {
    }
    this.ws = null;
  }
};

// src/peer-mesh.ts
var MAX_WS_RECONNECT_ATTEMPTS = 10;
var MAX_PEER_RECONNECT_ATTEMPTS = 10;
var MAX_BACKOFF_MS = 3e4;
var BUFFERED_AMOUNT_LIMIT = 65536;
var PeerMesh = class _PeerMesh extends EventEmitter {
  peers = /* @__PURE__ */ new Map();
  ws = null;
  knownPeers = /* @__PURE__ */ new Set();
  reconnectTimeouts = /* @__PURE__ */ new Map();
  peerAttempts = /* @__PURE__ */ new Map();
  lastSeen = /* @__PURE__ */ new Map();
  pingIntervals = /* @__PURE__ */ new Map();
  pendingTimers = /* @__PURE__ */ new Set();
  discoveryUrl = "";
  topicId = "";
  wsReconnectAttempts = 0;
  closed = false;
  constructor() {
    super();
  }
  /** setTimeout that is tracked, so disconnect() can cancel it. */
  later(fn, ms) {
    const t = setTimeout(() => {
      this.pendingTimers.delete(t);
      if (this.closed) return;
      fn();
    }, ms);
    this.pendingTimers.add(t);
  }
  static jitter(delay) {
    return Math.round(delay * (0.8 + Math.random() * 0.4));
  }
  connect(discoveryUrl, topicId) {
    if (this.closed) return;
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
      if (this.closed) return;
      if (this.wsReconnectAttempts >= MAX_WS_RECONNECT_ATTEMPTS) {
        console.warn("ZeroQ: Max WebSocket reconnect attempts reached");
        this.emit("connection_failed", "Max reconnect attempts");
        return;
      }
      const backoff = _PeerMesh.jitter(Math.min(1e3 * Math.pow(2, this.wsReconnectAttempts), MAX_BACKOFF_MS));
      this.wsReconnectAttempts++;
      this.later(() => this.connect(discoveryUrl, topicId), backoff);
    };
  }
  async connectToPeer(peerId) {
    if (this.closed) return;
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
      this.handleConnectionFailure(peerId, "offer-failed");
    }
  }
  async handleOffer(peerId, sdp) {
    if (this.closed) return;
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
      this.handleConnectionFailure(peerId, "answer-failed");
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
      this.peerAttempts.delete(peerId);
      this.lastSeen.set(peerId, Date.now());
      this.emit("peer_connected", peerId);
      const interval = setInterval(() => {
        if (this.closed) {
          clearInterval(interval);
          return;
        }
        if (dc.readyState === "open") {
          dc.send(JSON.stringify({ type: "__ping__" }));
        }
        const last = this.lastSeen.get(peerId) || 0;
        if (Date.now() - last > 3e4) {
          this.removePeer(peerId);
          this.handleConnectionFailure(peerId, "ping-timeout");
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
            const peers = Array.isArray(msg.peers) ? msg.peers : [];
            for (const p of peers) {
              if (typeof p === "string" && !this.knownPeers.has(p)) {
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
      this.handleConnectionFailure(peerId, "datachannel-closed");
    };
    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === "disconnected" || pc.iceConnectionState === "failed") {
        this.emit("peer_connection_failed", peerId, pc.iceConnectionState);
        this.removePeer(peerId);
        this.handleConnectionFailure(peerId, pc.iceConnectionState);
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
    this.lastSeen.delete(peerId);
    this.emit("peer_disconnected", peerId);
  }
  handleConnectionFailure(peerId, reason = "unknown") {
    if (this.closed) return;
    const attempts = (this.peerAttempts.get(peerId) || 0) + 1;
    this.peerAttempts.set(peerId, attempts);
    if (attempts > MAX_PEER_RECONNECT_ATTEMPTS) {
      console.warn(`ZeroQ: giving up on peer ${peerId} after ${MAX_PEER_RECONNECT_ATTEMPTS} attempts (${reason})`);
      this.emit("peer_unreachable", peerId, reason);
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
    }, _PeerMesh.jitter(delay));
  }
  broadcast(data) {
    let dropped = 0;
    for (const [peerId, { dc }] of this.peers.entries()) {
      if (dc.readyState === "open") {
        if (dc.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
          dropped++;
          this.emit("message_dropped", peerId, "backpressure", dc.bufferedAmount);
          continue;
        }
        try {
          dc.send(data);
        } catch (err) {
          dropped++;
          this.emit("message_dropped", peerId, "send-error", err);
          console.warn("ZeroQ: broadcast error", err);
        }
      }
    }
    return { peers: this.peers.size, dropped };
  }
  sendToPeer(peerId, data) {
    const peer = this.peers.get(peerId);
    if (peer && peer.dc.readyState === "open") {
      if (peer.dc.bufferedAmount > BUFFERED_AMOUNT_LIMIT) {
        this.emit("message_dropped", peerId, "backpressure", peer.dc.bufferedAmount);
        return;
      }
      try {
        peer.dc.send(data);
      } catch (err) {
        this.emit("message_dropped", peerId, "send-error", err);
        console.warn("ZeroQ: sendToPeer error", err);
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
var MAX_RETRIES = 3;
var SEEN_LIMIT = 1e4;
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
    this.peerMesh.on("message_received", (data) => {
      Promise.resolve(this.handleMessage(data)).catch((err) => {
        console.warn("ZeroQ: failed to handle inbound message", err);
      });
    });
  }
  /**
   * Duplicate suppression. Keyed on id + delivery attempt so that a nack()
   * retry (same id, higher retryCount) is not mistaken for a duplicate of the
   * original delivery — which previously made redelivery unreachable.
   */
  trackMessage(id, attempt = 0) {
    const key = `${id}:${attempt}`;
    if (this.seenMessages.has(key)) {
      return false;
    }
    this.seenMessages.add(key);
    this.seenMessagesQueue.push(key);
    if (this.seenMessagesQueue.length > SEEN_LIMIT) {
      const oldest = this.seenMessagesQueue.shift();
      if (oldest) this.seenMessages.delete(oldest);
    }
    return true;
  }
  async handleMessage(data) {
    let msg;
    try {
      msg = Serializer.deserialize(data);
    } catch (err) {
      console.warn("ZeroQ: dropping unparseable frame from peer");
      return;
    }
    if (!msg || typeof msg !== "object" || typeof msg.id !== "string" || msg.id === "" || typeof msg.topic !== "string") {
      console.warn("ZeroQ: dropping malformed message from peer (missing id/topic)");
      return;
    }
    if (typeof msg.retryCount !== "number" || !Number.isFinite(msg.retryCount) || msg.retryCount < 0) {
      msg.retryCount = 0;
    }
    if (!this.trackMessage(msg.id, msg.retryCount)) {
      return;
    }
    try {
      await this.persistence.save(msg);
    } catch (err) {
      console.warn("ZeroQ: could not persist inbound message", err);
    }
    if (this.topicHandlers.has(msg.topic)) {
      for (const h of [...this.topicHandlers.get(msg.topic)]) {
        try {
          h(msg);
        } catch (err) {
          console.warn("ZeroQ: subscriber threw", err);
        }
      }
    }
    if (this.queueHandlers.has(msg.topic)) {
      const handlers = this.queueHandlers.get(msg.topic);
      const handler = handlers.shift();
      if (handler) {
        handlers.push(handler);
        const ack = async () => {
          try {
            await this.persistence.delete(msg.id);
          } catch (err) {
            console.warn("ZeroQ: ack failed to remove message", err);
          }
        };
        const nack = async () => {
          try {
            msg.retryCount++;
            await this.persistence.save(msg);
            if (msg.retryCount < MAX_RETRIES) {
              this.peerMesh.broadcast(Serializer.serialize(msg));
            }
          } catch (err) {
            console.warn("ZeroQ: nack failed", err);
          }
        };
        try {
          handler(msg, ack, nack);
        } catch (err) {
          console.warn("ZeroQ: consumer threw", err);
        }
      }
    }
  }
  async publish(topic, message, options) {
    const msg = {
      id: crypto.randomUUID(),
      topic,
      payload: message,
      timestamp: Date.now(),
      seq: 0,
      retryCount: 0
    };
    if (options?.priority !== void 0) {
      msg.priority = options.priority;
    }
    const wire = Serializer.serialize(msg);
    this.trackMessage(msg.id, 0);
    try {
      await this.persistence.save(msg);
    } catch (err) {
      console.warn("ZeroQ: could not persist outbound message", err);
    }
    this.peerMesh.broadcast(wire);
  }
  subscribe(topic, handler) {
    if (!this.topicHandlers.has(topic)) {
      this.topicHandlers.set(topic, []);
    }
    this.topicHandlers.get(topic).push(handler);
    return {
      topic,
      unsubscribe: () => {
        const handlers = (this.topicHandlers.get(topic) || []).filter((h) => h !== handler);
        if (handlers.length === 0) this.topicHandlers.delete(topic);
        else this.topicHandlers.set(topic, handlers);
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
        const handlers = (this.queueHandlers.get(queue) || []).filter((h) => h !== handler);
        if (handlers.length === 0) this.queueHandlers.delete(queue);
        else this.queueHandlers.set(queue, handlers);
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
  discoveryUrl;
  topicId;
  constructor(options) {
    LicenseValidator.validate(options);
    this.discoveryUrl = options?.discoveryUrl || "ws://localhost:8080";
    this.topicId = options?.topicId || "default-topic";
    this.discoveryClient = new DiscoveryClient(this.discoveryUrl);
    this.persistence = new PersistenceLayer();
    this.peerMesh = new PeerMesh();
    this.broker = new MessageBroker(this.peerMesh, this.persistence);
    this.init().catch((err) => {
      console.warn("ZeroQ: initialisation failed", err);
    });
  }
  async init() {
    try {
      await this.persistence.init();
    } catch (err) {
      console.warn("ZeroQ: persistence unavailable \u2014 running without durability", err);
    }
    this.discoveryClient.connect();
    this.peerMesh.connect(this.discoveryUrl, this.topicId);
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
    await this.broker.publish(queue, message, { priority: options?.priority });
  }
  async consume(queue, handler) {
    this.discoveryClient.send({ type: "consume", queue });
    return this.broker.consume(queue, handler);
  }
  async request(topic, message, timeoutMs = 5e3) {
    const replyTo = crypto.randomUUID();
    let sub;
    let timer;
    try {
      return await new Promise((resolve, reject) => {
        timer = setTimeout(() => reject(new Error("Timeout")), timeoutMs);
        sub = this.broker.subscribe(replyTo, (msg) => resolve(msg.payload));
        this.discoveryClient.send({ type: "subscribe", topic: replyTo });
        this.publish(topic, { ...message, replyTo }).catch(reject);
      });
    } finally {
      clearTimeout(timer);
      sub?.unsubscribe();
    }
  }
  async reply(topic, handler) {
    await this.subscribe(topic, async (msg) => {
      const replyTo = msg.replyTo ?? (msg.payload && typeof msg.payload === "object" ? msg.payload.replyTo : void 0);
      if (!replyTo) return;
      try {
        const response = await handler(msg);
        await this.publish(replyTo, response);
      } catch (err) {
        console.warn("ZeroQ: reply handler failed", err);
      }
    });
  }
  disconnect() {
    this.discoveryClient.close();
    this.peerMesh.disconnect();
  }
};
export {
  ZeroQ
};
