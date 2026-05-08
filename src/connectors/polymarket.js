/**
 * Polymarket connector — REST for market discovery, WebSocket for real-time prices.
 *
 * Flow:
 *   1. REST: connect() validates API, getMarkets() discovers 15-min markets via Gamma slug
 *   2. WS:   subscribeWs(assetIds) opens persistent stream for live price updates
 *   3. On each WS tick → emits 'tick' event for the Monitor to react instantly
 *
 * WebSocket endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
 */

const axios = require('axios');
const { ethers } = require('ethers');
const WebSocket = require('ws');
const EventEmitter = require('events');
const https = require('https');
const { ClobClient, Chain, SignatureTypeV2 } = require('@polymarket/clob-client-v2');
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const config = require('../config');
const { createL2Headers, buildSignedOrder } = require('../utils/polymarketSigner');

const GAMMA_BASE = 'https://gamma-api.polymarket.com';
const CLOB_WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const INTERVAL_SECS = 900; // 15 minutes

const clobAgent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 10_000,
  maxSockets: 4,
  maxFreeSockets: 2,
});


class PolymarketConnector extends EventEmitter {
  constructor() {
    super();
    this.platform = 'polymarket';
    this.feeRate = config.fees.polymarket; // empirical 0.04; per-user override via setFeeRate()
    this.minOrderNotional = 1.00; // Polymarket CLOB rejects BUY < $1 ("min size: $1")
    this.clobUrl = config.polymarket.baseUrl;
    this.apiKey = '';      // per-user, set via loadUserKeys()
    this.apiSecret = '';   // per-user, set via loadUserKeys()
    this.passphrase = '';  // per-user, set via loadUserKeys()
    this.connected = false;
    this.markets = new Map(); // conditionId → normalized market

    // Wallet for order signing (per-user, set via loadUserKeys())
    this.wallet = null;
    this.funderAddress = '';
    this._clobClient = null; // V2 ClobClient instance (used as init gate in placeOrder)

    // Per-token neg-risk flag cache (tokenId → boolean).
    // True = neg-risk market (NEG_RISK_EXCHANGE_V2), false = standard (CTF_EXCHANGE_V2).
    // Fetched from GET /neg-risk?token_id=... on first _postOrder; immutable per token.
    this._negRiskCache = new Map();

    // WebSocket state
    this._ws = null;
    this._wsConnected = false;
    this._subscribedAssets = new Map(); // conditionId → { yesTokenId, noTokenId }
    this._reconnectDelay = 1000;
    this._maxReconnectDelay = 30000;
    this._reconnectTimer = null;
    this._pingTimer = null;
    this._lastPongAt = 0;  // tracks WS health — updated on every pong response

    // Per-token mini orderbook maintained from WS book + price_change events.
    // tokenId → { asks: Map<priceStr, size>, bids: Map<priceStr, size> }
    //
    // Without this, the WS handler used to set yesPrice from msg.price on every
    // event — including last_trade_price events — which made the decisionEngine
    // see prices that had never been the actual top-of-book. The result was a
    // flood of phantom signals that the dispatcher had to abort at REST time.
    this._books = new Map();

    // Per-token last-tick wall-clock timestamp (ms). Used by the dispatcher's
    // WS-livecheck fast path to refuse stale cache reads.
    // tokenId → ms
    this._lastTickAt = new Map();

    // Last order-placement error metadata. Populated by _postOrder on any
    // failure path (HTTP error, CLOB `error` field, or network exception).
    // The hedge reads this right after a failed placeOrder() to decide
    // whether to walk the book (liquidity), halve size (balance), or retry.
    //   { type: 'liquidity' | 'balance' | 'other',
    //     message: string,
    //     polyBalance: number|null,  // parsed from "balance: N" (6-decimal USDC)
    //     polyRequired: number|null, // parsed from "order amount: N"
    //     httpStatus: number|null }
    this._lastOrderError = null;
  }

  /* ---------------------------------------------------------- */
  /*  Helpers                                                    */
  /* ---------------------------------------------------------- */

  _clobHeaders() {
    const h = { 'Content-Type': 'application/json' };
    if (this.apiKey) {
      h['POLY_API_KEY'] = this.apiKey;
      h['POLY_PASSPHRASE'] = this.passphrase;
    }
    return h;
  }

  async _gammaGet(path, params = {}) {
    const resp = await axios.get(`${GAMMA_BASE}${path}`, { params, timeout: 10_000 });
    return resp.data;
  }

  async _clobGet(path, params = {}) {
    const opts = {
      headers: this._clobHeaders(),
      params,
      timeout: 10_000,
    };
    // Proxy is handled by HTTPS_PROXY env var — no explicit httpsAgent needed.
    // Using both causes double-proxy and timeouts.
    const resp = await axios.get(`${this.clobUrl}${path}`, opts);
    return resp.data;
  }

  async _clobAuthGet(path, params = {}) {
    const qs = new URLSearchParams(params).toString();
    const fullUrl = qs ? `${this.clobUrl}${path}?${qs}` : `${this.clobUrl}${path}`;
    // HMAC signs only the path (no query params) for GET requests
    const headers = createL2Headers(
      this.apiKey, this.apiSecret, this.passphrase,
      this.wallet?.address || '', 'GET', path,
    );
    const resp = await axios.get(fullUrl, {
      headers, timeout: 10_000, proxy: false,
    });
    return resp.data;
  }

  async getTokenBalance(tokenId) {
    if (!this.apiKey || !this.wallet) return null;
    try {
      const usesProxy = this.funderAddress &&
        this.funderAddress.toLowerCase() !== (this.wallet?.address || '').toLowerCase();
      const data = await this._clobAuthGet('/balance-allowance', {
        asset_type: 'CONDITIONAL',
        token_id: tokenId,
        signature_type: usesProxy ? 2 : 0,
      });
      if (data && data.balance != null) {
        const raw = parseFloat(data.balance);
        const scaled = raw / 1e6;
        if (this._balLogCount === undefined) this._balLogCount = 0;
        if (this._balLogCount < 5) {
          this._balLogCount++;
          console.log(`[Polymarket] getTokenBalance raw=${data.balance} parsed=${raw} scaled=${scaled}`);
        }
        if (scaled > 0) return scaled;
        if (raw > 0 && raw < 1000) return raw;
        return null;
      }
      return null;
    } catch (err) {
      if (this._balErrLogCount === undefined) this._balErrLogCount = 0;
      if (this._balErrLogCount < 5) {
        this._balErrLogCount++;
        console.warn(`[Polymarket] getTokenBalance error: ${err.response?.status || ''} ${err.message}`);
      }
      return null;
    }
  }

  _get15mTimestamps() {
    const nowSecs = Math.floor(Date.now() / 1000);
    const current = Math.floor(nowSecs / INTERVAL_SECS) * INTERVAL_SECS;
    const next = current + INTERVAL_SECS;
    return [current, next];
  }

  /**
   * Load user's API keys into this connector.
   */
  /** Returns true when the connector has enough state to place orders. */
  isReady() {
    return !!(this.wallet && this.apiKey);
  }

  loadUserKeys(keys) {
    if (!keys?.polyApiKey) return;

    this.apiKey = keys.polyApiKey;
    this.apiSecret = keys.polyApiSecret || '';
    this.passphrase = keys.polyPassphrase || '';
    this.funderAddress = keys.polyFunderAddress || '';

    if (keys.polyPrivateKey) {
      try {
        this.wallet = new ethers.Wallet(keys.polyPrivateKey);
        if (!this.funderAddress) this.funderAddress = this.wallet.address;
      } catch (err) {
        console.error('[Polymarket] invalid private key:', err.message);
      }
    }

    console.log(`[Polymarket] user keys loaded (apiKey: ${this.apiKey.slice(0, 8)}...)`);
  }

  /**
   * Fetch neg-risk status for a given token.
   * neg-risk markets must use NEG_RISK_EXCHANGE_V2 as the EIP-712 verifyingContract.
   * Standard markets use CTF_EXCHANGE_V2.
   *
   * Endpoint: GET /neg-risk?token_id=...
   * Response: { neg_risk: boolean }
   *
   * @param {string} tokenId
   * @returns {Promise<boolean>} true if neg-risk market
   */
  async _fetchNegRisk(tokenId) {
    if (this._negRiskCache.has(tokenId)) {
      return this._negRiskCache.get(tokenId);
    }
    try {
      const resp = await axios.get(`${this.clobUrl}/neg-risk`, {
        params: { token_id: tokenId },
        headers: this._clobHeaders(),
        httpsAgent: clobAgent,
        timeout: 10_000,
      });
      const negRisk = resp.data?.neg_risk === true;
      this._negRiskCache.set(tokenId, negRisk);
      console.log(`[Polymarket] neg-risk for ${tokenId.slice(0, 16)}...: ${negRisk} raw=${JSON.stringify(resp.data)}`);
      return negRisk;
    } catch (err) {
      const data = err.response?.data ? JSON.stringify(err.response.data) : err.message;
      console.warn(`[Polymarket] _fetchNegRisk failed for ${tokenId.slice(0, 16)}...: ${data} — defaulting to false`);
      return false;
    }
  }

  /**
   * Derive API keys from wallet using official Polymarket CLOB V2 client.
   * Keys are derived from the wallet private key via L1 EIP-712 auth.
   * Requires proxy to be set first (if on datacenter IP).
   */
  async deriveApiKeys() {
    if (!this.wallet) {
      console.error('[Polymarket] cannot derive API keys — no wallet');
      return false;
    }

    // https.globalAgent is set once at boot in index.js — no save/restore needed here

    const pk = this.wallet.privateKey.startsWith('0x') ? this.wallet.privateKey : `0x${this.wallet.privateKey}`;
    const account = privateKeyToAccount(pk);
    const viemWallet = createWalletClient({ account, chain: polygon, transport: http() });

    const signerAddr = account.address.toLowerCase();
    const proxyAddr = (this.funderAddress || '').toLowerCase();
    const useProxy = proxyAddr && proxyAddr !== signerAddr;
    const sigType = useProxy ? SignatureTypeV2.POLY_GNOSIS_SAFE : SignatureTypeV2.EOA;

    try {
      // L1 client (no creds) — only used to call deriveApiKey()
      const l1Client = new ClobClient({
        host: this.clobUrl,
        chain: Chain.POLYGON,
        signer: viemWallet,
      });
      const creds = await l1Client.deriveApiKey();
      console.log(`[Polymarket] deriveApiKey response: ${JSON.stringify(creds).slice(0, 200)}`);

      if (creds && creds.key) {
        this.apiKey = creds.key;
        this.apiSecret = creds.secret;
        this.passphrase = creds.passphrase;

        this._clobClient = new ClobClient({
          host: this.clobUrl,
          chain: Chain.POLYGON,
          signer: viemWallet,
          creds: { key: creds.key, secret: creds.secret, passphrase: creds.passphrase },
          signatureType: sigType,
          funderAddress: this.funderAddress || undefined,
        });

        console.log(`[Polymarket] API keys derived successfully (apiKey: ${this.apiKey.slice(0, 8)}...)`);
        return true;
      }

      console.warn('[Polymarket] deriveApiKey returned empty — will use stored keys');
    } catch (err) {
      const data = err.response?.data ? JSON.stringify(err.response.data) : err.message;
      console.error(`[Polymarket] API key derivation failed: ${data}`);
    }

    // If derive failed but we have stored keys, create ClobClient with those
    if (!this._clobClient && this.apiKey && this.apiSecret) {
      try {
        this._clobClient = new ClobClient({
          host: this.clobUrl,
          chain: Chain.POLYGON,
          signer: viemWallet,
          creds: { key: this.apiKey, secret: this.apiSecret, passphrase: this.passphrase },
          signatureType: sigType,
          funderAddress: this.funderAddress || undefined,
        });
        console.log(`[Polymarket] ClobClient V2 created with STORED keys (apiKey: ${this.apiKey.slice(0, 8)}...)`);
        return true;
      } catch (fallbackErr) {
        console.error(`[Polymarket] ClobClient V2 fallback failed: ${fallbackErr.message}`);
      }
    }
    return false;
  }

  /* ---------------------------------------------------------- */
  /*  REST connection                                            */
  /* ---------------------------------------------------------- */

  async connect() {
    try {
      await this._gammaGet('/markets', { limit: 1 });
      console.log('[Polymarket] connected — Gamma API reachable');
      this.connected = true;
    } catch (err) {
      console.error('[Polymarket] connection failed:', err.response?.data?.message || err.message);
      this.connected = false;
    }
  }

  /* ---------------------------------------------------------- */
  /*  WebSocket — real-time price streaming via CLOB WS          */
  /* ---------------------------------------------------------- */

  /**
   * Subscribe to real-time price updates for given markets.
   * Each market needs its token IDs (yesTokenId, noTokenId) for subscription.
   *
   * Idempotent: tracks the set of token IDs already sent to the server
   * (`_lastSentTokens`) and skips the WS subscribe message when called with
   * the same set. The Polymarket CLOB WS replies with the literal string
   * "INVALID OPERATION" to duplicate subscribes and stops sending updates
   * for those tokens until the connection is reset — which then surfaced
   * as ~150s of WS staleness during round rotations (monitor calls
   * _discoverMarkets up to 3× per rotation while waiting for Kalshi).
   */
  subscribeWs(markets = []) {
    // Track what to subscribe to
    for (const m of markets) {
      if (m.conditionId) {
        this._subscribedAssets.set(m.conditionId, {
          asset: m.asset,
          yesTokenId: m.yesTokenId,
          noTokenId: m.noTokenId,
          conditionId: m.conditionId,
        });
      }
    }

    // Build the canonical token set we'd send right now.
    const tokenKey = [...this._subscribedAssets.values()]
      .flatMap(a => [a.yesTokenId, a.noTokenId])
      .filter(Boolean)
      .sort()
      .join(',');

    if (this._ws && this._wsConnected) {
      if (this._lastSentTokens === tokenKey) {
        // Same token set already subscribed — skip; resending triggers
        // "INVALID OPERATION" from the server and breaks the stream.
        return;
      }
      this._lastSentTokens = tokenKey;
      this._sendSubscriptions([...this._subscribedAssets.values()]);
      return;
    }

    if (this._ws) return; // connecting in progress

    // Reset on new connection — server has no memory of prior subscribes.
    this._lastSentTokens = '';
    this._connectWs();
  }

  _connectWs() {
    try {
      this._ws = new WebSocket(CLOB_WS_URL);
    } catch (err) {
      console.error('[Polymarket WS] failed to create connection:', err.message);
      this._scheduleReconnect();
      return;
    }

    this._ws.on('open', () => {
      console.log('[Polymarket WS] connected');
      this._wsConnected = true;
      this._lastPongAt = Date.now();
      this._reconnectDelay = 1000;

      // Subscribe to all tracked assets
      if (this._subscribedAssets.size > 0) {
        this._sendSubscriptions([...this._subscribedAssets.values()]);
      }

      this._startPing();
    });

    this._ws.on('pong', () => {
      this._lastPongAt = Date.now();
    });

    this._ws.on('message', (raw) => {
      const text = raw.toString();
      // The CLOB WS replies with the literal string "INVALID OPERATION" to
      // duplicate / unrecognised subscribes. Not JSON — silently ignore.
      // After the dedup fix in subscribeWs() this should rarely fire.
      if (text === 'INVALID OPERATION') return;
      try {
        const msgs = JSON.parse(text);
        // Messages can be a single object or an array
        const arr = Array.isArray(msgs) ? msgs : [msgs];
        for (const msg of arr) {
          this._handleWsMessage(msg);
        }
      } catch (e) { console.error('[Poly] WS handler error:', e.message); }
    });

    this._ws.on('close', (code) => {
      console.log(`[Polymarket WS] closed (code: ${code})`);
      this._wsCleanup();
      this._scheduleReconnect();
    });

    this._ws.on('error', (err) => {
      console.error('[Polymarket WS] error:', err.message);
    });
  }

  _handleWsMessage(msg) {
    if (!msg) return;
    // Polymarket CLOB WS event types we care about:
    //   - "book"             → full snapshot { asset_id, bids:[{price,size}], asks:[{price,size}] }
    //                          asset_id is at the TOP LEVEL of the message.
    //   - "price_change"     → batch of deltas { market, price_changes:[{asset_id,price,side,size,...}] }
    //                          asset_id is on EACH item, NOT at top level. The top-level `market`
    //                          is the conditionId (hex), which is useless for token-level books.
    //                          NOTE: field is `price_changes` (plural), not `changes`.
    //   - "last_trade_price" → last executed trade price only — IGNORE (not a quote; was source
    //                          of phantom signals before the fix).
    //   - "tick_size_change" → ignored.
    const eventType = msg.event_type || msg.type || '';

    if (eventType === 'last_trade_price' || eventType === 'tick_size_change') return;

    // Track which token books were touched in this message so we can propagate once at the end.
    const touchedTokens = new Set();

    if (eventType === 'book' || Array.isArray(msg.asks) || Array.isArray(msg.bids)) {
      // Full snapshot for a single token. asset_id is at top level.
      const tokenId = msg.asset_id || msg.token_id || '';
      if (tokenId && this._applyBookSnapshot(tokenId, msg)) {
        touchedTokens.add(tokenId);
      }
    } else if (eventType === 'price_change' || Array.isArray(msg.price_changes) || Array.isArray(msg.changes)) {
      // Batch of deltas. Each item in price_changes carries its own asset_id.
      // Accept `price_changes` (current server format) with `changes` as legacy fallback.
      const items = Array.isArray(msg.price_changes) ? msg.price_changes
                  : Array.isArray(msg.changes) ? msg.changes
                  : [];
      for (const ch of items) {
        const tokenId = this._applyPriceChange(ch);
        if (tokenId) touchedTokens.add(tokenId);
      }
    } else {
      // Unknown / shape-only fallback: if it has a top-level asset_id and looks like a book,
      // try to handle it as a snapshot. Otherwise drop silently.
      const tokenId = msg.asset_id || '';
      if (tokenId && (Array.isArray(msg.asks) || Array.isArray(msg.bids))) {
        if (this._applyBookSnapshot(tokenId, msg)) touchedTokens.add(tokenId);
      }
      return;
    }

    // Propagate updated books to cached markets and emit ticks.
    const now = Date.now();
    for (const tokenId of touchedTokens) {
      this._lastTickAt.set(tokenId, now);
      this._propagateBook(tokenId);
    }

  }

  /**
   * Read top-of-book for a token directly from the in-memory WS mini-book.
   * Returns null if no book exists. Returns ageMs = wall-clock ms since the
   * last tick was applied — caller must enforce its own staleness threshold.
   *
   * Shape mirrors a parsed REST orderbook just enough for the dispatcher to
   * consume it identically:
   *   { bestAsk, bestAskSize, bestBid, bestBidSize, asks: [{price,size}], bids: [{price,size}], ageMs }
   *
   * Only the top 5 levels per side are returned — that's all the dispatcher
   * uses (size walking + DiagBook). The full Map stays in place for the
   * propagation logic.
   */
  getLiveSnapshot(tokenId) {
    if (!tokenId) return null;
    const book = this._books.get(tokenId);
    if (!book) return null;
    if (book.asks.size === 0 && book.bids.size === 0) return null;

    const sortedAsks = [...book.asks.entries()]
      .map(([p, s]) => ({ price: parseFloat(p), size: s }))
      .filter((l) => l.price > 0 && l.size > 0)
      .sort((a, b) => a.price - b.price)
      .slice(0, 5);

    const sortedBids = [...book.bids.entries()]
      .map(([p, s]) => ({ price: parseFloat(p), size: s }))
      .filter((l) => l.price > 0 && l.size > 0)
      .sort((a, b) => b.price - a.price)
      .slice(0, 5);

    const bestAsk = sortedAsks[0]?.price ?? null;
    const bestAskSize = sortedAsks[0]?.size ?? 0;
    const bestBid = sortedBids[0]?.price ?? null;
    const bestBidSize = sortedBids[0]?.size ?? 0;

    const lastAt = this._lastTickAt.get(tokenId) || 0;
    const ageMs = lastAt > 0 ? Date.now() - lastAt : -1;

    // wsAlive: true if the WS received a pong within the last 20s.
    // Proves the connection is alive even when the book is quiet.
    const wsAlive = this._wsConnected && (Date.now() - this._lastPongAt) < 20_000;

    return { bestAsk, bestAskSize, bestBid, bestBidSize, asks: sortedAsks, bids: sortedBids, ageMs, wsAlive };
  }

  /**
   * Apply a full book snapshot for one token. Replaces the local mini-book
   * with the server's view. Returns true if the book was touched.
   */
  _applyBookSnapshot(tokenId, msg) {
    if (!tokenId) return false;
    let book = this._books.get(tokenId);
    if (!book) {
      book = { asks: new Map(), bids: new Map() };
      this._books.set(tokenId, book);
    }
    book.asks.clear();
    book.bids.clear();
    const asksArr = Array.isArray(msg.asks) ? msg.asks : [];
    const bidsArr = Array.isArray(msg.bids) ? msg.bids : [];
    for (const lvl of asksArr) {
      const price = parseFloat(Array.isArray(lvl) ? lvl[0] : lvl.price);
      const size = parseFloat(Array.isArray(lvl) ? lvl[1] : lvl.size);
      if (price > 0 && size > 0) book.asks.set(price.toString(), size);
    }
    for (const lvl of bidsArr) {
      const price = parseFloat(Array.isArray(lvl) ? lvl[0] : lvl.price);
      const size = parseFloat(Array.isArray(lvl) ? lvl[1] : lvl.size);
      if (price > 0 && size > 0) book.bids.set(price.toString(), size);
    }
    return true;
  }

  /**
   * Apply one price_change delta item. The item carries its own asset_id
   * (which side/price/size to modify). Returns the tokenId touched, or ''.
   * size=0 removes the level.
   */
  _applyPriceChange(ch) {
    if (!ch) return '';
    const tokenId = ch.asset_id || ch.token_id || '';
    if (!tokenId) return '';
    const price = parseFloat(ch.price);
    const size = parseFloat(ch.size);
    if (!(price > 0) || isNaN(size)) return '';
    const sideStr = String(ch.side || '').toLowerCase();
    let book = this._books.get(tokenId);
    if (!book) {
      book = { asks: new Map(), bids: new Map() };
      this._books.set(tokenId, book);
    }
    const target = (sideStr === 'sell' || sideStr === 'ask') ? book.asks
                 : (sideStr === 'buy'  || sideStr === 'bid') ? book.bids
                 : null;
    if (!target) return '';
    if (size > 0) target.set(price.toString(), size);
    else target.delete(price.toString());
    return tokenId;
  }

  /**
   * Recompute best bid/ask for a token and push to any cached market that
   * references it (yes or no side). Emits 'tick' per market updated.
   */
  _propagateBook(tokenId) {
    const book = this._books.get(tokenId);
    if (!book) return;
    const bestAsk = this._bestPrice(book.asks, 'min');
    const bestBid = this._bestPrice(book.bids, 'max');

    for (const [conditionId, info] of this._subscribedAssets) {
      const cached = this.markets.get(conditionId);
      if (!cached) continue;

      let updated = false;
      if (tokenId === info.yesTokenId) {
        if (bestAsk != null) cached.yesAsk = bestAsk;
        if (bestBid != null) cached.yesBid = bestBid;
        if (bestAsk != null) cached.yesPrice = bestAsk;
        updated = true;
      } else if (tokenId === info.noTokenId) {
        if (bestAsk != null) cached.noAsk = bestAsk;
        if (bestBid != null) cached.noBid = bestBid;
        if (bestAsk != null) cached.noPrice = bestAsk;
        updated = true;
      }

      if (updated) {
        cached.lastUpdated = new Date().toISOString();
        this.emit('tick', cached);
      }
    }
  }

  _bestPrice(map, mode) {
    if (!map || map.size === 0) return null;
    let best = null;
    for (const k of map.keys()) {
      const p = parseFloat(k);
      if (!(p > 0)) continue;
      if (best == null) best = p;
      else if (mode === 'min' && p < best) best = p;
      else if (mode === 'max' && p > best) best = p;
    }
    return best;
  }

  _sendSubscriptions(markets) {
    const tokenIds = [];
    for (const m of markets) {
      if (m.yesTokenId) tokenIds.push(m.yesTokenId);
      if (m.noTokenId) tokenIds.push(m.noTokenId);
    }

    if (tokenIds.length === 0) return;

    console.log(`[Polymarket WS] subscribing to ${tokenIds.length} token streams`);

    // Polymarket CLOB WS subscription: assets_ids is an ARRAY, sent in one message
    this._wsSend({
      auth: {},
      type: 'market',
      assets_ids: tokenIds,
    });
    // Mark this exact set as sent so subscribeWs() can dedup follow-up calls.
    this._lastSentTokens = [...tokenIds].sort().join(',');
  }

  /**
   * Unsubscribe from markets no longer needed.
   */
  unsubscribeMarkets(conditionIds) {
    for (const id of conditionIds) {
      this._subscribedAssets.delete(id);
    }
    // Polymarket WS may not support explicit unsubscribe — new connection on rotation handles this
  }

  _wsSend(obj) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(obj));
    }
  }

  _startPing() {
    if (this._pingTimer) clearInterval(this._pingTimer);
    this._pingTimer = setInterval(() => {
      if (this._ws && this._ws.readyState === WebSocket.OPEN) {
        this._ws.ping();
      }
    }, 15_000);
  }

  _wsCleanup() {
    this._wsConnected = false;
    // Reset the dedup key — the server has no memory of prior subscribes
    // across connections, so the next subscribe must fire a real WS message.
    this._lastSentTokens = '';
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._ws) {
      this._ws.removeAllListeners();
      this._ws = null;
    }
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;

    console.log(`[Polymarket WS] reconnecting in ${this._reconnectDelay}ms...`);
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this._reconnectDelay = Math.min(this._reconnectDelay * 2, this._maxReconnectDelay);
      this._connectWs();
    }, this._reconnectDelay);
  }

  /* ---------------------------------------------------------- */
  /*  Market fetching — REST (for discovery only)                */
  /* ---------------------------------------------------------- */

  async getMarkets(asset) {
    if (!this.connected) return [];

    const slugPrefix = asset.polymarket?.slugPrefix;
    if (!slugPrefix) {
      console.log(`[Polymarket] ${asset.symbol} — no slugPrefix configured, skipping`);
      return [];
    }

    const [currentTs] = this._get15mTimestamps();
    const slugs = [`${slugPrefix}-${currentTs}`];

    const allMarkets = [];

    for (const slug of slugs) {
      try {
        const events = await this._gammaGet('/events', { slug });
        const items = Array.isArray(events) ? events : [events];

        for (const event of items) {
          if (!event || !event.markets) continue;

          const eventMarkets = Array.isArray(event.markets) ? event.markets : [];
          for (const raw of eventMarkets) {
            const normalized = this._normalize(raw, asset);
            if (normalized.conditionId && !allMarkets.some((m) => m.conditionId === normalized.conditionId)) {
              allMarkets.push(normalized);
            }
          }
        }
      } catch (err) {
        if (err.response?.status !== 404) {
          console.debug(`[Polymarket] ${asset.symbol} slug=${slug} — ${err.response?.status || err.message}`);
        }
      }
    }

    if (allMarkets.length > 0) {
      console.log(`[Polymarket] ${asset.symbol} — found ${allMarkets.length} market(s): "${allMarkets[0].question}"`);
    } else {
      console.log(`[Polymarket] ${asset.symbol} — no 15-min market found (slugs: ${slugs.join(', ')})`);
    }

    // Strike is NOT fetched here anymore — RTDS client captures it in real-time.
    // The Monitor injects the strike from RTDS into each market after discovery.

    // Cache for WS tick matching (conditionId → market object).
    // REST is only used for discovery; WS handles all price updates after this.
    for (const m of allMarkets) {
      this.markets.set(m.conditionId, m);
    }

    return allMarkets;
  }

  async getOrderbook(tokenId) {
    if (!this.connected) return null;
    try {
      const data = await this._clobGet('/book', { token_id: tokenId });
      return data || null;
    } catch (err) {
      console.error(`[Polymarket] getOrderbook(${tokenId}) error:`, err.response?.data?.message || err.message);
      return null;
    }
  }

  async getPrices(tokenIds) {
    if (!this.connected || tokenIds.length === 0) return {};
    try {
      const data = await this._clobGet('/prices', { token_ids: tokenIds.join(',') });
      return data || {};
    } catch (err) {
      console.error('[Polymarket] getPrices error:', err.response?.data?.message || err.message);
      return {};
    }
  }

  /* ---------------------------------------------------------- */
  /*  Normalization                                              */
  /* ---------------------------------------------------------- */

  _normalize(raw, asset) {
    let outcomes, prices, tokenIds;
    try {
      outcomes = raw.outcomes ? JSON.parse(raw.outcomes) : [];
      prices = raw.outcomePrices ? JSON.parse(raw.outcomePrices) : [];
      tokenIds = raw.clobTokenIds ? JSON.parse(raw.clobTokenIds) : [];
    } catch (e) {
      console.error(`[Polymarket] failed to parse Gamma fields for ${raw.question || 'unknown'}:`, e.message);
      return null;
    }

    const yesIdx = outcomes.indexOf('Up') !== -1 ? outcomes.indexOf('Up') :
                   outcomes.indexOf('Yes') !== -1 ? outcomes.indexOf('Yes') : 0;
    const noIdx = outcomes.indexOf('Down') !== -1 ? outcomes.indexOf('Down') :
                  outcomes.indexOf('No') !== -1 ? outcomes.indexOf('No') : 1;

    const question = raw.question || raw.title || '';

    return {
      platform: 'polymarket',
      asset: asset.symbol,
      conditionId: raw.conditionId || raw.condition_id || raw.id || '',
      question,
      slug: raw.slug || '',
      outcomes,
      yesTokenId: tokenIds[yesIdx] || '',
      noTokenId: tokenIds[noIdx] || '',
      yesPrice: parseFloat(prices[yesIdx] || 0),
      noPrice: parseFloat(prices[noIdx] || 0),
      volume: parseFloat(raw.volume || raw.volumeNum || 0),
      liquidity: parseFloat(raw.liquidity || raw.liquidityNum || 0),
      endDate: raw.endDateIso || raw.end_date_iso || '',
      eventStartTime: raw.eventStartTime || '',
      active: raw.active !== false,
      closed: raw.closed === true,
      strike: null, // Filled later via Binance price at eventStartTime
      lastUpdated: new Date().toISOString(),
    };
  }

  /* ---------------------------------------------------------- */
  /*  Order placement                                            */
  /* ---------------------------------------------------------- */

  /**
   * Place FOK order at exact signal price — no split, no slippage.
   * If FOK fails, trade fails cleanly (no resting orders, no partial fills).
   */
  async placeOrder(order) {
    if (!this.connected) {
      console.error('[Polymarket] cannot place order — not connected');
      return null;
    }

    if (!this._clobClient) {
      console.error('[Polymarket] cannot place order — ClobClient not initialized (keys not derived)');
      return null;
    }

    if (!order.tokenId || order.tokenId.trim() === '') {
      console.error('[Polymarket] cannot place order — tokenId is empty');
      return null;
    }

    const orderType = order.orderType || 'FOK';
    console.log(`[Polymarket] placing ${orderType} order: ${order.side}@${order.price} x${order.size} tokenId=${order.tokenId.slice(0, 16)}...`);
    console.log(`[Polymarket] apiKey: ${this.apiKey ? this.apiKey.slice(0, 8) + '...' : 'MISSING'}, funder: ${this.funderAddress || 'none'}`);

    // https.globalAgent is set once at boot — no save/restore needed

    const resp = await this._postOrder(order.tokenId, order.price, order.size, order.side, orderType);

    if (resp !== null) {
      console.log(`[Polymarket] order SUCCESS (${orderType}): ${order.side}@${order.price} x${order.size} — ${JSON.stringify(resp).slice(0, 200)}`);
      return resp;
    }

    console.warn(`[Polymarket] ${orderType} FAILED — ${order.side}@${order.price} x${order.size}, not retrying`);
    return null;
  }

  /**
   * Post a single FOK order. Returns response or null on failure.
   *
   * Implementation note: this method does NOT use ClobClient.createAndPostOrder
   * because that path uses an internal axios instance that ignores process.env
   * HTTPS_PROXY, axios.defaults.*, and https.globalAgent — making it impossible
   * to route through a residential proxy. Instead, we sign the order locally
   * via buildSignedOrder() (EIP-712) + createL2Headers() (HMAC), then POST
   * directly with axios passing httpsAgent EXPLICITLY in the request config.
   * This is the only reliable way to force the request through the Decodo
   * Mexico exit, bypassing Polymarket's USA geoblock.
   */
  async _postOrder(tokenId, price, size, side, orderType = 'FOK') {
    if (!this.wallet) {
      console.error('[Polymarket] cannot place order — no wallet');
      return null;
    }

    // Reset last-error so the hedge never reads stale data from a prior call.
    this._lastOrderError = null;

    try {
      // 1) Determine neg-risk flag — selects verifyingContract for EIP-712 domain.
      // BTC/ETH crypto markets may be neg-risk (NEG_RISK_EXCHANGE_V2) or standard
      // (CTF_EXCHANGE_V2). Wrong contract = wrong domain hash = "invalid signature".
      const negRisk = await this._fetchNegRisk(tokenId);

      // 2) Build and sign the order LOCALLY (no network call) via EIP-712 V2.
      // feeRateBps removed from V2 order struct — fee is calculated by the
      // exchange at match time and charged in pUSD, not deducted from tokens.
      const tSign0 = Date.now();
      // V2: proxy Gnosis Safe usa sigType=2 (maker=proxy, signer=EOA owner).
      // Confirmado aceito pelo CLOB em 2026-04-30. EOA direto usa sigType=0.
      const signedPayload = await buildSignedOrder(this.wallet, {
        tokenId,
        price,
        size,
        side,
        funderAddress: this.funderAddress,  // proxy=Gnosis Safe, ou EOA se sem proxy
        apiKey: this.apiKey,
        negRisk,
      });
      // Override default GTC with the requested order type
      signedPayload.orderType = orderType;
      console.log(`[Polymarket] EIP-712 sign took ${Date.now() - tSign0}ms`);

      // 2) Build L2 HMAC headers (signature is HMAC over the body string)
      const path = '/order';
      const bodyStr = JSON.stringify(signedPayload);
      const headers = createL2Headers(
        this.apiKey,
        this.apiSecret,
        this.passphrase,
        this.wallet.address,
        'POST',
        path,
        bodyStr,
      );

      // 3) Build axios config — direct POST using keepAlive agent
      const axiosConfig = {
        method: 'POST',
        url: `${this.clobUrl}${path}`,
        data: signedPayload,
        headers,
        timeout: 15_000,
        maxRedirects: 0,
        proxy: false,
        httpsAgent: clobAgent,
      };

      // 4) Direct POST — bypasses ClobClient entirely
      const t0 = Date.now();
      const resp = await axios(axiosConfig);
      const postLatencyMs = Date.now() - t0;
      console.log(`[Polymarket] POST /order latency: ${postLatencyMs}ms (direct)`);
      const data = resp.data;

      // CLOB returns 200 with {error: "..."} on validation failures
      if (data && data.error) {
        const errStr = typeof data.error === 'string' ? data.error : JSON.stringify(data.error);
        this._lastOrderError = this._classifyOrderError(errStr, 200);
        if (errStr.toLowerCase().includes('fully filled') || errStr.toLowerCase().includes('not filled')) {
          console.warn(`[Polymarket] FOK rejected (not enough liquidity at ${price}): ${errStr.slice(0, 200)}`);
          return null;
        }
        console.error(`[Polymarket] order FAILED (200 with error): ${errStr.slice(0, 300)}`);
        return null;
      }

      return data;
    } catch (err) {
      const status = err.response?.status || err.status || 'no-status';
      const data = err.response?.data ? JSON.stringify(err.response.data).slice(0, 300) : err.message;
      const location = err.response?.headers?.location || err.response?.headers?.Location;
      const reqUrl = err.config?.url || 'unknown';
      const reqMethod = err.config?.method || 'unknown';
      const errBody = err.response?.data?.error
        || (typeof err.response?.data === 'string' ? err.response.data : null)
        || err.message
        || 'unknown';
      this._lastOrderError = this._classifyOrderError(String(errBody), typeof status === 'number' ? status : null);
      console.error(`[Polymarket] order FAILED (HTTP ${status}): ${data}`);
      console.error(`[Polymarket] order FAILED context: ${reqMethod.toUpperCase()} ${reqUrl}${location ? ` → Location: ${location}` : ''}`);
      return null;
    }
  }

  /**
   * Parse a CLOB error string into structured hedge-actionable metadata.
   * Recognises:
   *   - "not enough balance / allowance: ... balance: 8720, order amount: 1000000"
   *     → type='balance', polyBalance / polyRequired in USDC (6-decimal → float)
   *   - "fully filled" / "not filled" → type='liquidity'
   *   - anything else → type='other'
   */
  _classifyOrderError(errStr, httpStatus) {
    const msg = String(errStr || '').slice(0, 500);
    const lower = msg.toLowerCase();
    let type = 'other';
    let polyBalance = null;
    let polyRequired = null;

    if (lower.includes('not enough balance') || lower.includes('allowance')) {
      type = 'balance';
      const balMatch = msg.match(/balance:\s*(\d+)/i);
      const reqMatch = msg.match(/order\s*amount:\s*(\d+)/i);
      if (balMatch) polyBalance = parseInt(balMatch[1], 10) / 1e6;
      if (reqMatch) polyRequired = parseInt(reqMatch[1], 10) / 1e6;
    } else if (lower.includes('fully filled') || lower.includes('not filled')) {
      type = 'liquidity';
    } else if (
      lower.includes('market closed') || lower.includes('market_closed') ||
      lower.includes('market not found') || lower.includes('market_not_found') ||
      lower.includes('invalid market') || lower.includes('market does not exist') ||
      httpStatus === 404
    ) {
      type = 'market_closed';
    }

    return {
      type,
      message: msg,
      polyBalance,
      polyRequired,
      httpStatus: httpStatus || null,
    };
  }

  /**
   * Split order into up to 2 lots based on orderbook depth.
   * Lot 1: available qty at original price
   * Lot 2: remaining qty at next ask (within 2¢ slippage)
   */
  async _splitOrder(tokenId, price, totalSize, side) {
    const MAX_SLIP = 0.02;

    try {
      // Fetch orderbook
      const axiosOpts = { timeout: 5000 };
      // Proxy handled by HTTPS_PROXY env var
      const bookResp = await axios.get(`${this.clobUrl}/book?token_id=${tokenId}`, axiosOpts);
      const book = bookResp.data;

      // For BUY orders, we look at asks (sellers); for SELL, we look at bids
      const levels = side === 'BUY'
        ? (book.asks || []).map(o => ({ price: parseFloat(o.price), size: parseFloat(o.size) })).sort((a, b) => a.price - b.price)
        : (book.bids || []).map(o => ({ price: parseFloat(o.price), size: parseFloat(o.size) })).sort((a, b) => b.price - a.price);

      if (levels.length === 0) {
        console.error('[Polymarket] split FAILED — orderbook empty');
        return null;
      }

      // Calculate available qty at original price and next level
      let qtyAtPrice = 0;
      let nextPrice = null;

      for (const level of levels) {
        if (side === 'BUY') {
          if (level.price <= price + 0.005) {
            qtyAtPrice += level.size;
          } else if (level.price <= price + MAX_SLIP + 0.005) {
            nextPrice = level.price;
            break;
          } else {
            break; // beyond max slippage
          }
        } else {
          if (level.price >= price - 0.005) {
            qtyAtPrice += level.size;
          } else if (level.price >= price - MAX_SLIP - 0.005) {
            nextPrice = level.price;
            break;
          } else {
            break;
          }
        }
      }

      console.log(`[Polymarket] orderbook: ${qtyAtPrice} available @${price}, next level @${nextPrice || 'none'}`);

      const slipNeeded = nextPrice ? Math.abs(nextPrice - price) : Infinity;

      if (slipNeeded > MAX_SLIP && qtyAtPrice < 1) {
        console.error(`[Polymarket] split FAILED — no liquidity within ${MAX_SLIP * 100}¢ slippage`);
        return null;
      }

      let filledTotal = 0;
      let lastResp = null;

      // Lot 1: fill at original price
      if (qtyAtPrice >= 1) {
        const qty1 = Math.min(Math.floor(qtyAtPrice), totalSize);
        console.log(`[Polymarket] lot 1: ${qty1} contracts @${price}`);
        const r1 = await this._postOrder(tokenId, price, qty1, side);
        if (r1) {
          filledTotal += qty1;
          lastResp = r1;
          console.log(`[Polymarket] lot 1 SUCCESS: ${qty1} filled`);
        } else {
          console.warn(`[Polymarket] lot 1 FAILED`);
        }
      }

      // Lot 2: fill remaining at next price level (within slippage)
      const remaining = totalSize - filledTotal;
      if (remaining >= 1 && nextPrice && slipNeeded <= MAX_SLIP) {
        console.log(`[Polymarket] lot 2: ${remaining} contracts @${nextPrice} (slip: ${(slipNeeded * 100).toFixed(1)}¢)`);
        const r2 = await this._postOrder(tokenId, nextPrice, remaining, side);
        if (r2) {
          filledTotal += remaining;
          lastResp = r2;
          console.log(`[Polymarket] lot 2 SUCCESS: ${remaining} filled`);
        } else {
          console.warn(`[Polymarket] lot 2 FAILED`);
        }
      }

      if (filledTotal === 0) {
        console.error(`[Polymarket] split order FAILED — 0 contracts filled`);
        return null;
      }

      console.log(`[Polymarket] split order DONE: ${filledTotal}/${totalSize} contracts filled`);
      return lastResp;
    } catch (err) {
      console.error(`[Polymarket] split order error: ${err.message}`);
      return null;
    }
  }

  /* ---------------------------------------------------------- */
  /*  Generic platform interface (used by decisionEngine/dispatcher) */
  /* ---------------------------------------------------------- */

  /** Hedge/exit floor price for SELL sweep orders. Poly: $0.001 sub-cent. */
  get floorPrice() { return 0.001; }

  setFeeRate(rate) { if (typeof rate === 'number' && rate >= 0) this.feeRate = rate; }

  /** Unique market key (CTF conditionId). */
  getMarketKey(market) { return market.conditionId; }

  /** Outcome token id for one side. */
  getSideToken(market, side) {
    return side === 'yes' || side === 'up' ? market.yesTokenId : market.noTokenId;
  }

  getBestAsk(market, side) {
    const tokenId = this.getSideToken(market, side);
    if (!tokenId) return null;
    const snap = this.getLiveSnapshot(tokenId);
    return snap?.bestAsk ?? null;
  }
  getBestBid(market, side) {
    const tokenId = this.getSideToken(market, side);
    if (!tokenId) return null;
    const snap = this.getLiveSnapshot(tokenId);
    return snap?.bestBid ?? null;
  }

  /**
   * V2 CTF Exchange entry fee per contract (paid in tokens):
   *   tokens_lost = feeRate × min(P, 1-P) / P
   * Polymarket SELL has 0% fee (confirmed via official docs).
   */
  calcEntryFee(_market, price) {
    if (!(price > 0) || !(price < 1)) return 0;
    return this.feeRate * Math.min(price, 1 - price) / price;
  }
  calcExitFee(_market, _price) {
    return 0;
  }

  /**
   * Unified snapshot for one side. Reads the WS book of the token.
   * Returns { bestAsk, bestBid, asks, bids, ageMs, wsAlive } in the same
   * shape Kalshi exposes — caller doesn't need to know the platform.
   */
  getLiveSnapshotForSide(market, side) {
    const tokenId = this.getSideToken(market, side);
    if (!tokenId) return null;
    return this.getLiveSnapshot(tokenId);
  }

  /** Total ask depth (tokens) at price ≤ ceilingPrice. */
  getDepthAtCeiling(market, side, ceilingPrice) {
    const snap = this.getLiveSnapshotForSide(market, side);
    if (!snap?.asks?.length) return 0;
    let total = 0;
    for (const a of snap.asks) {
      if (a.price <= ceilingPrice + 1e-9) total += a.size;
    }
    return total;
  }

  /**
   * Simulate buying `qty` at limit ≤ `ceilingPrice` — walks asks lowest first.
   */
  simulateBuyFill(market, side, qty, ceilingPrice) {
    const empty = { qtyFillable: 0, totalCost: 0, avgFillPrice: 0, depleted: true, levelsUsed: 0 };
    const snap = this.getLiveSnapshotForSide(market, side);
    if (!snap?.asks?.length || !(qty > 0)) return empty;

    const sorted = [...snap.asks].sort((a, b) => a.price - b.price);
    let remaining = qty;
    let totalCost = 0;
    let qtyFillable = 0;
    let levelsUsed = 0;
    for (const a of sorted) {
      if (a.price > ceilingPrice + 1e-9) break;
      const consume = Math.min(remaining, a.size);
      totalCost += consume * a.price;
      qtyFillable += consume;
      remaining -= consume;
      levelsUsed++;
      if (remaining <= 0) break;
    }
    const avgFillPrice = qtyFillable > 0 ? totalCost / qtyFillable : 0;
    return { qtyFillable, totalCost, avgFillPrice, depleted: remaining > 0, levelsUsed };
  }

  /**
   * Simulate selling `qty` via FAK floor sweep — walks bids highest first.
   * Polymarket SELL has 0% fee. Apply WS-staleness haircut (configurable).
   */
  simulateSellFill(market, side, qty) {
    const empty = { qtyFillable: 0, netRevenue: 0, avgFillPrice: 0, depleted: true, levelsUsed: 0, haircutApplied: 0 };
    const snap = this.getLiveSnapshotForSide(market, side);
    if (!snap?.bids?.length || !(qty > 0)) return empty;

    let remaining = qty;
    let grossRevenue = 0;
    let qtyFillable = 0;
    let levelsUsed = 0;
    for (const b of snap.bids) {
      if (!(b.price > 0) || !(b.size > 0)) continue;
      const consume = Math.min(remaining, b.size);
      grossRevenue += consume * b.price;
      qtyFillable += consume;
      remaining -= consume;
      levelsUsed++;
      if (remaining <= 0) break;
    }
    if (qtyFillable === 0) return empty;

    const haircutScale = config.execution?.polyHaircutScale ?? 0.60;
    const avgRawBid = grossRevenue / qtyFillable;
    const haircut = Math.min(0.30, (1 - avgRawBid) * haircutScale);
    const netRevenue = grossRevenue * (1 - haircut);
    const avgFillPrice = qtyFillable > 0 ? netRevenue / qtyFillable : 0;
    return { qtyFillable, netRevenue, avgFillPrice, depleted: remaining > 0, levelsUsed, haircutApplied: haircut };
  }

  /** Generic buy — wraps placeOrder with FAK BUY against the side token. */
  async placeBuyOrder(market, side, price, qty) {
    const tokenId = this.getSideToken(market, side);
    return this.placeOrder({ tokenId, price, size: qty, side: 'BUY', orderType: 'FAK' });
  }
  /** Generic sell — wraps placeOrder with FAK SELL against the side token. */
  async placeSellOrder(market, side, price, qty) {
    const tokenId = this.getSideToken(market, side);
    return this.placeOrder({ tokenId, price, size: qty, side: 'SELL', orderType: 'FAK' });
  }

  /** Outcome token holdings for one side (settled CTF balance). */
  async getSideTokenBalance(market, side) {
    const tokenId = this.getSideToken(market, side);
    if (!tokenId) return null;
    return this.getTokenBalance(tokenId);
  }

  /**
   * Collateral balance (pUSD on Polygon) via Polygon RPC.
   * The on-chain query lives in CapitalGuard's reusable providers; this
   * method exposes the funder address so CapitalGuard can query it.
   * Returns null when no providers are wired — CapitalGuard handles fallback.
   */
  async getBalance() {
    // Returns the funder address; the actual on-chain query is performed by
    // CapitalGuard which has the Polygon RPC providers cached. Keeping the
    // RPC fan-out in one place avoids creating a second pool here.
    return null;
  }
  getBalanceAddress() { return this.funderAddress || this.wallet?.address || null; }

  /**
   * Tokens received by a BUY order. makingAmount is USDC paid; dividing by
   * intended price yields the contract count (rounded to whole units).
   */
  extractBuyFillQty(result, intendedPrice) {
    if (!result) return 0;
    const m = parseFloat(result.makingAmount || 0);
    if (m > 0 && intendedPrice > 0) return Math.round(m / intendedPrice);
    return 0;
  }

  /** SELL outcome — takingAmount is USDC received. soldQty derived from makingAmount/avg. */
  extractSellOutcome(result) {
    if (!result) return { soldQty: 0, takingAmount: 0, orderId: null };
    const taking = parseFloat(result.takingAmount || 0);
    const making = parseFloat(result.makingAmount || 0);
    const soldQty = making > 0 ? making : 0; // makingAmount on a SELL is shares given
    return { soldQty, takingAmount: taking, orderId: null };
  }

  /** Polymarket order response carries fill amounts directly — no extraction needed. */
  extractFillPrice(orderResult) {
    if (!orderResult) return null;
    const taking = parseFloat(orderResult.takingAmount || 0);
    const making = parseFloat(orderResult.makingAmount || 0);
    if (taking > 0 && making > 0) return making / taking;
    return null;
  }

  /** Best book bids list for hedge fallback — kept for backward compat with dispatcher. */
  async getHedgeBids(tokenId) {
    try {
      const snap = this.getLiveSnapshot(tokenId);
      if (snap?.bids?.length) {
        return snap.bids.map(l => ({ price: l.price, size: l.size, source: 'ws' }));
      }
    } catch (_) {}
    try {
      const book = await this.getOrderbook(tokenId);
      if (book?.bids?.length) {
        return book.bids
          .map(l => ({ price: parseFloat(l.price), size: parseFloat(l.size) }))
          .filter(l => l.price > 0 && l.size > 0)
          .sort((a, b) => b.price - a.price)
          .slice(0, 5)
          .map(l => ({ ...l, source: 'rest' }));
      }
    } catch (_) {}
    return [];
  }

  /* ---------------------------------------------------------- */
  /*  Cleanup                                                    */
  /* ---------------------------------------------------------- */

  destroy() {
    if (this._ws) {
      this._ws.close();
      this._wsCleanup();
    }
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
  }
}

module.exports = PolymarketConnector;
