/**
 * predict.fun connector — CLOB on BNB Chain (chainId=56), USDT collateral.
 *
 * Flow:
 *   1. REST: connect() tests API, getMarkets() discovers 15-min BTC markets
 *   2. WS:   subscribeWs() opens stream for real-time orderbook updates
 *   3. Each WS tick → emits 'tick' for Monitor (same interface as Polymarket)
 *   4. placeOrder() places MARKET order + polls until fill (no inline fill in POST)
 *
 * Key differences vs Polymarket:
 *   - chainId=56 (BNB), collateral=USDT (18 decimals)
 *   - EIP-712 domain: "predict.fun CTF Exchange" / v1
 *   - MARKET strategy ≈ FOK (expires 5 min, partial fills allowed)
 *   - Fill price comes from polling GET /v1/orders (not in POST response)
 *   - WS heartbeat: server sends {type:"M",topic:"heartbeat"}, must respond
 *   - Single book per market (Up token); Down prices derived as 1 - Up
 */

const axios = require('axios');
const { ethers } = require('ethers');
const WebSocket = require('ws');
const EventEmitter = require('events');
const config = require('../config');

const BASE_URL = config.predictfun?.baseUrl || 'https://api.predict.fun';
const WS_URL   = config.predictfun?.wsUrl   || 'wss://ws.predict.fun/ws';
const INTERVAL_SECS = 900; // 15 minutes

// BNB Chain USDT (18 decimals) — used for on-chain balance queries
const BSC_USDT_ADDRESS = '0x55d398326f99059fF775485246999027B3197955';
const BSC_USDT_ABI     = ['function balanceOf(address) view returns (uint256)'];
const BSC_RPCS = [
  'https://bsc-dataseed.binance.org',
  'https://bsc-dataseed1.binance.org',
  'https://bsc.publicnode.com',
  'https://bsc-dataseed2.binance.org',
];

// verifyingContract by market type (BNB mainnet)
const CONTRACTS = {
  standard:              '0x8BC070BEdAB741406F4B1Eb65A72bee27894B689',
  negRisk:               '0x365fb81bd4A24D6303cd2F19c349dE6894D8d58A',
  yieldBearing:          '0x6bEb5a40C032AFc305961162d8204CDA16DECFa5',
  yieldBearingNegRisk:   '0x8A289d458f5a134bA40015085A8F50Ffb681B41d',
};

// EIP-712 typed data for predict.fun orders
const ORDER_TYPES = {
  Order: [
    { name: 'salt',          type: 'uint256' },
    { name: 'maker',         type: 'address' },
    { name: 'signer',        type: 'address' },
    { name: 'taker',         type: 'address' },
    { name: 'tokenId',       type: 'uint256' },
    { name: 'makerAmount',   type: 'uint256' },
    { name: 'takerAmount',   type: 'uint256' },
    { name: 'expiration',    type: 'uint256' },
    { name: 'nonce',         type: 'uint256' },
    { name: 'feeRateBps',    type: 'uint256' },
    { name: 'side',          type: 'uint8'   },
    { name: 'signatureType', type: 'uint8'   },
  ],
};

const WEI = BigInt('1000000000000000000'); // 1e18

function toWei(n) {
  // n is a number/float (e.g. 0.5 or 5.0). Returns BigInt in 1e18.
  return BigInt(Math.round(n * 1e9)) * BigInt(1e9);
}

class PredictFunConnector extends EventEmitter {
  constructor() {
    super();
    this.platform = 'predictfun';
    this.feeRate = 0.01; // sane default; per-market feeRateBps overrides this when present
    this.minOrderNotional = 1.00; // assumed parity with Polymarket; adjust if predict.fun differs
    this.baseUrl  = BASE_URL;
    this.apiKey   = '';     // set via loadUserKeys()
    this.wallet   = null;   // ethers.Wallet (signing key — creates signatures)
    this.predictAccount = null; // Predict Account address (smart wallet that holds USDT)
    this.connected = false;

    // Market cache: conditionId (String(market.id)) → normalized market
    this.markets = new Map();

    // JWT for authenticated endpoints (poll fill status)
    this._jwt = null;
    this._jwtExpiry = 0; // unix timestamp ms

    // WS state
    this._ws = null;
    this._wsConnected = false;
    this._reconnectDelay = 1000;
    this._maxReconnectDelay = 30_000;
    this._reconnectTimer = null;
    this._lastPongAt = 0;
    this._reqId = 1;

    // Per-market mini-book from WS (conditionId → { bids: Map, asks: Map })
    this._books = new Map();

    // Per-market tick age (conditionId → ms)
    this._lastTickAt = new Map();

    // Subscribed market IDs (conditionId → { marketId, yesTokenId, noTokenId, asset })
    this._subscribedAssets = new Map();

    // Last order error metadata (same shape as polymarket for dispatcher compat)
    this._lastOrderError = null;

    // Reusable BNB Chain providers for balance queries (lazy init)
    this._bscProviders = null;
  }

  /* ---------------------------------------------------------- */
  /*  Interface compatibility                                    */
  /* ---------------------------------------------------------- */

  isReady() {
    return !!(this.wallet && this.apiKey);
  }

  /**
   * Get USDT balance on BNB Chain via on-chain query.
   * Returns balance as a number in USDT (e.g. 100.50), or null on failure.
   */
  async getBalance() {
    // Balance lives in the Predict Account (smart wallet), NOT the signing wallet.
    // If predictAccount is not configured, fall back to signing wallet address.
    const targetAddress = this.predictAccount || this.wallet?.address;
    if (!targetAddress) return null;

    // Lazy-init reusable BSC providers (same pattern as capitalGuard Polygon providers)
    if (!this._bscProviders) {
      this._bscProviders = BSC_RPCS.map(rpc =>
        new ethers.JsonRpcProvider(rpc, 56, { staticNetwork: true, batchMaxCount: 1 })
      );
    }

    for (const provider of this._bscProviders) {
      try {
        const usdt = new ethers.Contract(BSC_USDT_ADDRESS, BSC_USDT_ABI, provider);
        const raw = await Promise.race([
          usdt.balanceOf(targetAddress),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 5000)),
        ]);
        return Number(raw) / 1e18; // BSC USDT has 18 decimals
      } catch (_) {}
    }
    return null;
  }

  /**
   * Get the number of outcome tokens held for a given tokenId.
   *
   * Endpoint: GET /v1/positions (JWT required)
   * Response field: amount — wei string (1e18 decimals), matches ERC-1155 balance.
   * Match via outcome.onChainId === tokenId (the CTF token address/id).
   *
   * Returns token count as float (e.g. 150.0 shares), null on error, 0 if not found.
   * Used by dispatcher early exit to confirm settled token balance exists to sell.
   */
  async getTokenBalance(tokenId) {
    if (!tokenId) return null;
    await this._ensureJwt();
    try {
      const resp = await axios.get(`${this.baseUrl}/v1/positions`, {
        headers: this._headers(),
        timeout: 8_000,
      });
      const data = resp.data?.data || resp.data;
      const positions = Array.isArray(data) ? data : [];
      for (const pos of positions) {
        if (String(pos.outcome?.onChainId) === String(tokenId)) {
          // amount is a wei string (ERC-1155 balance, 18 decimals)
          return Number(BigInt(pos.amount || '0')) / 1e18;
        }
      }
      return 0; // no position found for this tokenId
    } catch (err) {
      console.debug(`[PredictFun] getTokenBalance(${tokenId}) error: ${err.message}`);
      return null;
    }
  }

  /* ---------------------------------------------------------- */
  /*  Auth helpers                                               */
  /* ---------------------------------------------------------- */

  _headers() {
    const h = { 'Content-Type': 'application/json', 'x-api-key': this.apiKey };
    if (this._jwt) h['Authorization'] = `Bearer ${this._jwt}`;
    return h;
  }

  /**
   * Authenticate: EIP-191 personal sign → JWT.
   * Called automatically when JWT is absent or expired.
   */
  async _authenticate() {
    if (!this.wallet) return false;
    try {
      const msgResp = await axios.get(`${this.baseUrl}/v1/auth/message`, {
        headers: { 'x-api-key': this.apiKey },
        timeout: 10_000,
      });
      const message = msgResp.data?.data?.message || msgResp.data?.message;
      if (!message) {
        console.warn('[PredictFun] auth/message returned no message field');
        return false;
      }
      const signature = await this.wallet.signMessage(message);
      // Auth uses the Privy EOA (wallet.address), not the smart contract (predictAccount).
      // The server uses ecrecover (not EIP-1271) on the auth endpoint.
      // JWT issued for wallet.address; order.signer also = wallet.address → they match.
      const authResp = await axios.post(`${this.baseUrl}/v1/auth`, {
        signer: this.wallet.address,
        signature,
        message,
      }, { headers: { 'x-api-key': this.apiKey }, timeout: 10_000 });
      const token = authResp.data?.data?.token || authResp.data?.token;
      if (!token) {
        console.warn('[PredictFun] auth returned no token');
        return false;
      }
      this._jwt = token;
      // JWT typically valid 24h — refresh 1h before expiry
      this._jwtExpiry = Date.now() + 23 * 3600 * 1000;
      console.log('[PredictFun] JWT obtained');
      return true;
    } catch (err) {
      const d = err.response?.data ? JSON.stringify(err.response.data).slice(0, 200) : err.message;
      console.warn(`[PredictFun] auth failed: ${d}`);
      return false;
    }
  }

  async _ensureJwt() {
    if (this._jwt && Date.now() < this._jwtExpiry) return;
    await this._authenticate();
  }

  /* ---------------------------------------------------------- */
  /*  Keys                                                       */
  /* ---------------------------------------------------------- */

  loadUserKeys(keys) {
    if (!keys?.predictApiKey) return;
    this.apiKey = keys.predictApiKey;

    // predict.fun uses POLY_PROXY pattern:
    //   signerWallet (Privy EOA, 0x09F...) = signs orders, derived from predictPrivateKey
    //   predictAccount (smart contract, 0x010b41...) = holds USDT, is the maker
    // The two addresses are DIFFERENT. Never override predictAccount with wallet.address.
    const pk = keys.predictPrivateKey || keys.polyPrivateKey;
    if (pk) {
      try {
        this.wallet = new ethers.Wallet(pk);
      } catch (err) {
        console.error('[PredictFun] invalid private key:', err.message);
      }
    }

    if (keys.predictAccountAddress) {
      this.predictAccount = keys.predictAccountAddress;
    }

    const acct = this.predictAccount ? this.predictAccount.slice(0, 10) + '...' : 'not set (orders will use signing wallet)';
    console.log(`[PredictFun] keys loaded — signerWallet: ${this.wallet?.address.slice(0, 10)}... predictAccount: ${acct} apiKey: ${this.apiKey.slice(0, 8)}...`);
  }

  /* ---------------------------------------------------------- */
  /*  REST connection                                            */
  /* ---------------------------------------------------------- */

  async connect() {
    // Try multiple candidate endpoints. Any HTTP response (even 4xx) proves
    // the network is reachable — only a network-level failure (timeout /
    // ECONNREFUSED / no response) means we should mark as disconnected.
    const candidates = ['/v1/markets', '/markets', '/v1/categories'];
    for (const path of candidates) {
      try {
        await axios.get(`${this.baseUrl}${path}`, {
          headers: { 'x-api-key': this.apiKey || '' },
          timeout: 10_000,
        });
        // HTTP 2xx → connected
        console.log(`[PredictFun] connected — REST API reachable (${path})`);
        this.connected = true;
        if (this.wallet && this.apiKey) this._authenticate().catch(() => {});
        return;
      } catch (err) {
        if (err.response) {
          // Server replied (4xx/5xx) → network works, just wrong endpoint
          console.log(`[PredictFun] connected — REST API reachable (${path} → ${err.response.status})`);
          this.connected = true;
          if (this.wallet && this.apiKey) this._authenticate().catch(() => {});
          return;
        }
        // No response → network error; try next candidate
      }
    }
    console.error('[PredictFun] connection failed — no response from API (all candidates)');
    this.connected = false;
  }

  /* ---------------------------------------------------------- */
  /*  USDT approval                                              */
  /* ---------------------------------------------------------- */

  /**
   * Ensure the Privy signing wallet has approved the CTF Exchange contracts to
   * spend USDT on its behalf. Called once at startup; skips if already approved.
   * Without this, orders fail with "Insufficient collateral allowance".
   */
  async ensureUsdtApproval() {
    if (!this.wallet) return;
    const USDT_ABI = [
      'function allowance(address owner, address spender) view returns (uint256)',
      'function approve(address spender, uint256 amount) returns (bool)',
    ];
    const MAX = BigInt('0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff');
    const MIN_ALLOWANCE = BigInt('10000000000000000000'); // 10 USDT in 1e18

    if (!this._bscProviders) {
      this._bscProviders = BSC_RPCS.map(rpc =>
        new ethers.JsonRpcProvider(rpc, 56, { staticNetwork: true, batchMaxCount: 1 })
      );
    }

    // Connect wallet to first working provider for sending txns
    let connectedWallet = null;
    for (const provider of this._bscProviders) {
      try {
        await Promise.race([
          provider.getBlockNumber(),
          new Promise((_, r) => setTimeout(() => r(new Error('timeout')), 5000)),
        ]);
        connectedWallet = this.wallet.connect(provider);
        break;
      } catch (_) {}
    }
    if (!connectedWallet) {
      console.warn('[PredictFun] ensureUsdtApproval: no BSC provider reachable');
      return;
    }

    const usdt = new ethers.Contract(BSC_USDT_ADDRESS, USDT_ABI, connectedWallet);
    const spenders = Object.values(CONTRACTS);

    for (const spender of spenders) {
      try {
        const allowance = await usdt.allowance(this.wallet.address, spender);
        if (allowance >= MIN_ALLOWANCE) continue;
        console.log(`[PredictFun] approving USDT → ${spender.slice(0, 10)}...`);
        const tx = await usdt.approve(spender, MAX);
        await tx.wait(1);
        console.log(`[PredictFun] USDT approved → ${spender.slice(0, 10)}... (tx: ${tx.hash})`);
      } catch (err) {
        console.warn(`[PredictFun] USDT approve failed for ${spender.slice(0, 10)}: ${err.message}`);
      }
    }
  }

  /* ---------------------------------------------------------- */
  /*  Market discovery                                            */
  /* ---------------------------------------------------------- */

  _get15mTimestamps() {
    const nowSecs = Math.floor(Date.now() / 1000);
    const current = Math.floor(nowSecs / INTERVAL_SECS) * INTERVAL_SECS;
    const next = current + INTERVAL_SECS;
    return [current, next];
  }

  /**
   * Build candidate category slugs for a given window timestamp.
   * predict.fun uses the same format as daily markets but with hour+minute:
   *   btc-usd-up-down-{YYYY}-{MM}-{DD}-{HH}-{MM}
   * We also try the legacy variant with the -15-minutes suffix (404 but cheap).
   * Timezone offsets tried: UTC, UTC-3 (BRT), UTC-4 (EDT).
   */
  _buildCategorySlugs(asset, windowTs) {
    const sym = asset.symbol.toLowerCase();
    const slugs = [];

    // Primary format (confirmed from UI URL): {sym}-updown-15m-{windowTimestamp}
    // e.g. btc-updown-15m-1778130900
    slugs.push(`${sym}-updown-15m-${windowTs}`);

    // Legacy date-based formats (all returned 404 in tests, kept as fallback)
    for (const offsetH of [0, -3, -4]) {
      for (const extraSecs of [0, 900]) {
        const d = new Date((windowTs + extraSecs + offsetH * 3600) * 1000);
        const yyyy = d.getUTCFullYear();
        const mo   = String(d.getUTCMonth() + 1).padStart(2, '0');
        const dd   = String(d.getUTCDate()).padStart(2, '0');
        const hh   = String(d.getUTCHours()).padStart(2, '0');
        const min  = String(d.getUTCMinutes()).padStart(2, '0');
        const base = `${sym}-usd-up-down-${yyyy}-${mo}-${dd}-${hh}-${min}`;
        slugs.push(base);
        slugs.push(`${base}-15-minutes`);
      }
    }
    return [...new Set(slugs)];
  }

  /**
   * Query the parent category and filter for markets in the current 15-min window.
   * Tries both confirmed and legacy slug formats.
   */
  async _discoverByParentCategory(asset, out) {
    const sym = asset.symbol.toLowerCase();
    // Try confirmed slug first (from UI URLs), then legacy format
    const slug = `${sym}-updown-15m`;          // e.g. btc-updown-15m
    const legacySlug = `${sym}-usd-up-down`;   // kept for fallback
    const nowSecs  = Math.floor(Date.now() / 1000);
    const [currentTs] = this._get15mTimestamps();
    const windowEndSecs = currentTs + INTERVAL_SECS * 2 + 120; // current + next + 2 min buffer

    for (const trySlug of [slug, legacySlug]) {
      try {
        const resp = await axios.get(`${this.baseUrl}/v1/categories/${trySlug}`, {
          headers: this._headers(),
          params: { limit: 50 },
          timeout: 12_000,
        });
        const data = resp.data?.data || resp.data;
        const items = Array.isArray(data) ? data : (data?.markets || [data].filter(Boolean));
        for (const raw of items) {
          if (raw.status === 'RESOLVED' || raw.tradingStatus === 'CLOSED') continue;
          const tradingOk = raw.tradingStatus === 'OPEN' || raw.status === 'OPEN' || raw.status === 'REGISTERED';
          if (!tradingOk) continue;
          const endMs = raw.endDate ? new Date(raw.endDate).getTime()
                      : raw.endAt   ? new Date(raw.endAt).getTime()
                      : null;
          if (endMs != null) {
            const endSec = endMs / 1000;
            if (endSec < nowSecs - 120 || endSec > windowEndSecs) continue;
          }
          const normalized = this._normalize(raw, asset);
          if (normalized && !out.some(m => m.conditionId === normalized.conditionId)) {
            out.push(normalized);
            console.log(`[PredictFun] ${asset.symbol} found via parentCat ${trySlug}: "${raw.title}" endDate=${raw.endDate || raw.endAt || 'null'}`);
          }
        }
        if (out.length > 0) break; // found via this slug — no need to try legacy
      } catch (err) {
        // 404 expected when slug format doesn't exist — slug strategy picks it up
      }
    }
  }

  async getMarkets(asset, options = {}) {
    if (!this.connected) return [];
    if (!asset.predictfun) return []; // asset not configured for predict.fun

    const allMarkets = [];

    // Strategy 0: parent category — GET /v1/categories/{sym}-usd-up-down
    // Lists all BTC/ETH Up/Down markets; filter for current 15-min window.
    // Cheap (1 request), doesn't require knowing the exact time-based slug.
    if (allMarkets.length === 0) {
      await this._discoverByParentCategory(asset, allMarkets);
    }

    // Strategy 1: kalshiMarketTicker param — API typically ignores it and returns
    // the default first-page, but attempt anyway (some deploys may support it).
    const kalshiTickers = options.kalshiTickers || [];
    if (allMarkets.length === 0 && kalshiTickers.length > 0) {
      await this._discoverByKalshiTickers(asset, kalshiTickers, allMarkets);
    }

    // Strategy 2: category slug matching (UTC and EDT variants).
    // Only the current 15-min window — fetching nextTs would pair with future
    // markets that have static 0.50/0.50 prices and zero liquidity.
    if (allMarkets.length === 0) {
      const [currentTs] = this._get15mTimestamps();
      for (const windowTs of [currentTs]) {
        for (const slug of this._buildCategorySlugs(asset, windowTs)) {
          if (allMarkets.length > 0) break;
          try {
            const resp = await axios.get(`${this.baseUrl}/v1/categories/${encodeURIComponent(slug)}`, {
              headers: this._headers(),
              timeout: 10_000,
            });
            const data = resp.data?.data || resp.data;
            const items = Array.isArray(data) ? data : (data?.markets || [data].filter(Boolean));
            for (const raw of items) {
              const normalized = this._normalize(raw, asset);
              if (normalized && !allMarkets.some(m => m.conditionId === normalized.conditionId)) {
                allMarkets.push(normalized);
              }
            }
            if (allMarkets.length > 0) {
              console.log(`[PredictFun] ${asset.symbol} found via slug: ${slug}`);
            }
          } catch (err) {
            // slug not found — try next candidate
          }
        }
      }
    }

    // Strategy 3: general search fallback
    if (allMarkets.length === 0) {
      try {
        await this._discoverViaSearch(asset, allMarkets);
      } catch (_) {}
    }

    // Enrich token IDs — category listing may omit outcomes.onChainId.
    // Without tokenIds the decisionEngine cannot place orders (rejects the signal).
    if (allMarkets.length > 0) {
      await this._enrichTokenIds(allMarkets);
    }

    for (const m of allMarkets) {
      this.markets.set(m.conditionId, m);
    }

    if (allMarkets.length > 0) {
      console.log(`[PredictFun] ${asset.symbol} — found ${allMarkets.length} market(s): "${allMarkets[0].question}"`);
    } else {
      console.log(`[PredictFun] ${asset.symbol} — no 15-min market found`);
    }

    return allMarkets;
  }

  /**
   * Search predict.fun for markets whose kalshiMarketTicker matches one of
   * the given Kalshi tickers. Each predict.fun market has a kalshiMarketTicker
   * field that directly links it to the corresponding Kalshi market.
   */
  async _discoverByKalshiTickers(asset, kalshiTickers, out) {
    const sym = asset.symbol.toLowerCase();
    for (const ticker of kalshiTickers) {
      try {
        const resp = await axios.get(`${this.baseUrl}/v1/markets`, {
          headers: this._headers(),
          params: { kalshiMarketTicker: ticker, limit: 5 },
          timeout: 10_000,
        });
        const data = resp.data?.data || resp.data;
        const items = Array.isArray(data) ? data : (data?.markets || data?.items || []);

        for (const raw of items) {
          // The API often ignores the kalshiMarketTicker param and returns the
          // default paginated list. Validate strictly:
          // 1. kalshiMarketTicker field MUST either match our ticker exactly,
          //    OR be absent from the response schema entirely.
          //    If the field is present but null/empty, the market is NOT linked
          //    to any Kalshi ticker → skip it.
          // 2. The title/question must reference the asset symbol.
          const kField = raw.kalshiMarketTicker;
          const fieldPresent = Object.prototype.hasOwnProperty.call(raw, 'kalshiMarketTicker');
          if (fieldPresent) {
            // Field exists in schema — must match our ticker exactly
            if (!kField || kField !== ticker) continue;
          }
          const title = (raw.title || raw.question || '').toLowerCase();
          if (!title.includes(sym)) continue;

          const normalized = this._normalize(raw, asset);
          if (normalized && !out.some(m => m.conditionId === normalized.conditionId)) {
            out.push(normalized);
            console.log(`[PredictFun] ${asset.symbol} found via kalshiTicker=${ticker}: "${raw.title}" kalshiField="${raw.kalshiMarketTicker}"`);
          }
        }
      } catch (err) {
        const errDetail = err.response
          ? `HTTP ${err.response.status} — ${JSON.stringify(err.response.data).slice(0, 200)}`
          : err.message;
        console.warn(`[PredictFun] kalshiTicker=${ticker} search failed: ${errDetail}`);
      }
    }
  }

  async _discoverViaSearch(asset, out) {
    const sym = asset.symbol.toLowerCase();

    // Primary: marketVariant=CRYPTO_UP_DOWN filters to only crypto Up/Down markets.
    // Confirmed from variantData.type="CRYPTO_UP_DOWN" logged in live session.
    // Fallback: status=OPEN deep scan if variant filter not supported.
    const passes = [
      { label: 'variant+open',  baseParams: { marketVariant: 'CRYPTO_UP_DOWN', status: 'OPEN', limit: 20 }, maxPages: 10 },
      { label: 'variant-only',  baseParams: { marketVariant: 'CRYPTO_UP_DOWN',                limit: 20 }, maxPages: 10 },
      { label: 'status=OPEN',   baseParams: { status: 'OPEN',                                 limit: 20 }, maxPages: 80 },
    ];

    for (const { label, baseParams, maxPages } of passes) {
      let cursor  = null;
      let page    = 0;
      const sampleTitles = [];

      while (page < maxPages) {
        const params = { ...baseParams };
        if (cursor) params.cursor = cursor;

        try {
          const resp = await axios.get(`${this.baseUrl}/v1/markets`, {
            headers: this._headers(),
            params,
            timeout: 10_000,
          });
          const body = resp.data;
          const data = body?.data || body;
          const items = Array.isArray(data) ? data : (data?.markets || data?.items || []);
          const nextCursor = body?.cursor || null;

          if (page < 3 && items.length > 0) sampleTitles.push(items[0]?.title || '?');

          for (const raw of items) {
            // Skip resolved/closed markets — we only want currently tradeable markets
            if (raw.status === 'RESOLVED' || raw.tradingStatus === 'CLOSED') continue;
            const title = (raw.title || raw.question || raw.categorySlug || '').toLowerCase();
            if (!title.includes(sym) && !title.includes('bitcoin')) continue;
            const normalized = this._normalize(raw, asset);
            if (normalized && normalized.active && !out.some(m => m.conditionId === normalized.conditionId)) {
              out.push(normalized);
            }
          }

          if (out.length > 0) {
            console.log(`[PredictFun] ${asset.symbol} found via search pass=${label} page ${page + 1}`);
            return;
          }
          if (!nextCursor) break;
          cursor = nextCursor;
          page++;
        } catch (err) {
          const errDetail = err.response
            ? `HTTP ${err.response.status} — ${JSON.stringify(err.response.data).slice(0, 200)}`
            : err.message;
          console.warn(`[PredictFun] search pass=${label} page ${page + 1} failed: ${errDetail}`);
          break;
        }
      }

      if (out.length > 0) return;
    }

    console.log(`[PredictFun] ${asset.symbol} — not found in any search pass`);
  }

  _normalize(raw, asset) {
    if (!raw || !raw.id) return null;
    const conditionId = String(raw.id);

    const outcomes = Array.isArray(raw.outcomes) ? raw.outcomes : [];
    // Up = indexSet 1 (YES), Down = indexSet 2 (NO)
    const upOutcome   = outcomes.find(o => o.indexSet === 1 || o.name?.toLowerCase() === 'up');
    const downOutcome = outcomes.find(o => o.indexSet === 2 || o.name?.toLowerCase() === 'down');
    const yesTokenId  = String(upOutcome?.onChainId || '');
    const noTokenId   = String(downOutcome?.onChainId || '');

    const isNegRisk      = raw.isNegRisk === true;
    const isYieldBearing = raw.isYieldBearing === true;
    const feeRateBps     = parseInt(raw.feeRateBps || 100, 10);

    // Resolve settlement time. Only endDate/endAt are valid — createdAt is the
    // creation timestamp (always in the past) and would make _filterMarkets
    // silently drop the market for being "expired".
    const endDate = raw.endDate || raw.endAt || null;

    // Extract predict.fun "Price to Beat" (strike) from market data.
    // Candidate fields — log all non-null values so we can learn the correct one.
    let strike = null;
    const vd = raw.variantData;
    if (vd != null) {
      const vdObj = typeof vd === 'string' ? (() => { try { return JSON.parse(vd); } catch (_) { return {}; } })() : (vd || {});
      strike = vdObj.startPrice ?? vdObj.strikePrice ?? vdObj.targetPrice ?? vdObj.openPrice ?? vdObj.startValue ?? vdObj.referencePrice ?? null;
    }
    if (strike == null && raw.stats != null) {
      const st = typeof raw.stats === 'object' ? raw.stats : {};
      strike = st.startPrice ?? st.openPrice ?? st.strikePrice ?? st.referencePrice ?? null;
    }
    if (strike == null && raw.resolution != null && typeof raw.resolution === 'object') {
      strike = raw.resolution.startPrice ?? raw.resolution.openPrice ?? null;
    }
    if (strike != null) {
      strike = parseFloat(strike);
      if (isNaN(strike)) strike = null;
    }

    return {
      platform:    'predictfun',
      asset:       asset.symbol,
      conditionId,
      question:    raw.title || raw.question || '',
      slug:        raw.categorySlug || raw.slug || conditionId,
      outcomes:    outcomes.map(o => o.name),
      yesTokenId,
      noTokenId,
      yesPrice:    0,
      noPrice:     0,
      yesAsk:      0,
      noAsk:       0,
      yesBid:      0,
      noBid:       0,
      volume:      parseFloat(raw.volume || 0),
      liquidity:   parseFloat(raw.liquidity || 0),
      endDate,
      active:      raw.tradingStatus !== 'CLOSED' && raw.status !== 'RESOLVED' && raw.status !== 'CLOSED',
      closed:      raw.status === 'RESOLVED' || raw.tradingStatus === 'CLOSED',
      strike,     // from market data; Monitor injects RTDS only if null
      feeRateBps,
      isNegRisk,
      isYieldBearing,
      marketId:    raw.id,
      lastUpdated: new Date().toISOString(),
    };
  }

  /**
   * Enrich markets with yesTokenId/noTokenId if missing after category listing.
   *
   * The category listing endpoint may omit outcomes.onChainId. Without tokenIds
   * the decisionEngine rejects all signals (token required to sign/place orders).
   *
   * Strategy: try GET /v1/markets/{marketId} (same host, same path as the confirmed
   * orderbook endpoint) — its full market detail typically includes outcomes.onChainId.
   */
  async _enrichTokenIds(markets) {
    for (const m of markets) {
      if (m.yesTokenId && m.noTokenId) continue; // already populated
      if (!m.marketId) continue;
      try {
        const resp = await axios.get(`${this.baseUrl}/v1/markets/${m.marketId}`, {
          headers: this._headers(),
          timeout: 8_000,
        });
        const raw = resp.data?.data || resp.data;
        if (!raw) continue;
        const outcomes = Array.isArray(raw.outcomes) ? raw.outcomes : [];
        const upOut   = outcomes.find(o => o.indexSet === 1 || o.name?.toLowerCase() === 'up');
        const downOut = outcomes.find(o => o.indexSet === 2 || o.name?.toLowerCase() === 'down');
        if (upOut?.onChainId)   m.yesTokenId = String(upOut.onChainId);
        if (downOut?.onChainId) m.noTokenId  = String(downOut.onChainId);
        console.log(`[PredictFun] ${m.asset} tokenIds enriched — yes="${m.yesTokenId}" no="${m.noTokenId}"`);
      } catch (err) {
        console.debug(`[PredictFun] _enrichTokenIds(${m.marketId}) failed: ${err.message}`);
      }
    }
  }

  async getOrderbook(tokenId) {
    const market = this._findMarketByTokenId(tokenId)
               || [...this.markets.values()].find(m => m.conditionId === tokenId);
    if (!market) return null;
    const data = await this._fetchRawOrderbook(market.marketId, market.slug || market.conditionId);
    if (!data) return null;
    return this._normalizeBook(data, tokenId, market);
  }

  /**
   * Try multiple endpoint formats for the predict.fun orderbook REST API.
   * Logs the first working URL so we know the correct format going forward.
   */
  async _fetchRawOrderbook(marketId, slugOrId) {
    const candidates = [
      `/v1/markets/${marketId}/orderbook`,
      `/v1/orderbook/${marketId}`,
      `/v1/orderbooks/${marketId}`,
      `/v1/orderbook?marketId=${marketId}`,
    ];
    for (const path of candidates) {
      try {
        const url = path.includes('?')
          ? `${this.baseUrl}${path.split('?')[0]}?${path.split('?')[1]}`
          : `${this.baseUrl}${path}`;
        const resp = await axios.get(url, {
          headers: this._headers(),
          timeout: 10_000,
        });
        const data = resp.data?.data || resp.data;
        if (data && (data.bids || data.asks)) {
          console.log(`[PredictFun] orderbook endpoint confirmed: ${path}`);
          return data;
        }
      } catch (err) {
        if (err.response?.status === 404) continue; // expected, try next
        console.debug(`[PredictFun] ${path} → ${err.response?.status || err.message}`);
      }
    }
    console.log(`[PredictFun] all orderbook endpoints returned 404 for marketId=${marketId}`);
    return null;
  }

  _normalizeBook(data, tokenId, market) {
    if (!data) return null;
    const isYes = tokenId === market.yesTokenId;
    // predict.fun book shows the "Up" side. "Down" prices are derived.
    const rawBids = (data.bids || []).map(l => ({ price: parseFloat(l.price || l[0]), size: parseFloat(l.size || l[1]) }));
    const rawAsks = (data.asks || []).map(l => ({ price: parseFloat(l.price || l[0]), size: parseFloat(l.size || l[1]) }));

    if (isYes) {
      return { bids: rawBids.sort((a,b) => b.price - a.price), asks: rawAsks.sort((a,b) => a.price - b.price) };
    }
    // Down token: prices are complementary (Down_ask = 1 - Up_bid, Down_bid = 1 - Up_ask)
    const downAsks = rawBids.map(l => ({ price: Math.round((1 - l.price) * 1000) / 1000, size: l.size })).sort((a,b) => a.price - b.price);
    const downBids = rawAsks.map(l => ({ price: Math.round((1 - l.price) * 1000) / 1000, size: l.size })).sort((a,b) => b.price - a.price);
    return { bids: downBids, asks: downAsks };
  }

  _findMarketByTokenId(tokenId) {
    for (const m of this.markets.values()) {
      if (m.yesTokenId === tokenId || m.noTokenId === tokenId) return m;
    }
    return null;
  }

  /* ---------------------------------------------------------- */
  /*  WebSocket — real-time orderbook streaming                  */
  /* ---------------------------------------------------------- */

  subscribeWs(markets = []) {
    for (const m of markets) {
      if (m.conditionId) {
        this._subscribedAssets.set(m.conditionId, {
          asset: m.asset,
          marketId: m.marketId,
          yesTokenId: m.yesTokenId,
          noTokenId:  m.noTokenId,
          conditionId: m.conditionId,
        });
      }
    }

    // Prime the in-memory book via REST immediately.
    // The WS may not send an initial snapshot if the book has no recent activity.
    // Without this, yesAsk=0/noAsk=0 forever and decisionEngine ignores the market.
    this._primeBooks(markets).catch(() => {});

    // Start periodic REST refresh in case WS doesn't send book events.
    if (!this._bookRefreshTimer) {
      this._bookRefreshTimer = setInterval(() => this._refreshBooksIfStale(), 30_000);
      if (this._bookRefreshTimer.unref) this._bookRefreshTimer.unref();
    }

    if (this._ws && this._wsConnected) {
      this._sendSubscriptions(markets);
      return;
    }
    if (this._ws) return; // connecting
    this._connectWs();
  }

  async _primeBooks(markets) {
    for (const m of markets) {
      if (!m.marketId) continue;
      const data = await this._fetchRawOrderbook(m.marketId, m.slug || m.conditionId);
      if (data) {
        this._applyBookUpdate(m.marketId, data);
        const cached = this.markets.get(m.conditionId);
        if (cached) {
          console.log(`[PredictFun] ${m.asset} book primed: yesAsk=${cached.yesAsk?.toFixed(4)} noAsk=${cached.noAsk?.toFixed(4)}`);
        }
      }
    }
  }

  async _refreshBooksIfStale() {
    const now = Date.now();
    for (const [conditionId, info] of this._subscribedAssets) {
      const lastTick = this._lastTickAt.get(conditionId) || 0;
      if (now - lastTick < 25_000) continue; // WS book is fresh
      const data = await this._fetchRawOrderbook(info.marketId, conditionId).catch(() => null);
      if (data) this._applyBookUpdate(info.marketId, data);
    }
  }

  _connectWs() {
    try {
      this._ws = new WebSocket(WS_URL);
    } catch (err) {
      console.error('[PredictFun WS] failed to create connection:', err.message);
      this._scheduleReconnect();
      return;
    }

    this._ws.on('open', () => {
      console.log('[PredictFun WS] connected');
      this._wsConnected = true;
      this._lastPongAt = Date.now();
      this._reconnectDelay = 1000;

      if (this._subscribedAssets.size > 0) {
        this._sendSubscriptions([...this._subscribedAssets.values()]);
      }
    });

    this._ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        this._handleWsMessage(msg);
      } catch (e) {
        console.error('[PredictFun WS] parse error:', e.message);
      }
    });

    this._ws.on('close', (code) => {
      console.log(`[PredictFun WS] closed (code: ${code})`);
      this._wsCleanup();
      this._scheduleReconnect();
    });

    this._ws.on('error', (err) => {
      console.error('[PredictFun WS] error:', err.message);
    });
  }

  _handleWsMessage(msg) {
    if (!msg) return;

    // Server-side heartbeat: respond immediately
    if (msg.type === 'M' && msg.topic === 'heartbeat') {
      this._lastPongAt = Date.now();
      this._wsSend({ method: 'heartbeat', data: msg.data });
      return;
    }

    // Subscription acknowledgement
    if (msg.type === 'R') return;

    // Orderbook update: {type:"M", topic:"predictOrderbook/{marketId}", data:{bids,asks,...}}
    if (msg.type === 'M' && typeof msg.topic === 'string' && msg.topic.startsWith('predictOrderbook/')) {
      const marketId = parseInt(msg.topic.split('/')[1], 10);
      if (!isNaN(marketId) && msg.data) {
        this._applyBookUpdate(marketId, msg.data);
      }
      return;
    }

    // Log any unhandled message types for diagnostics
    if (msg.type && msg.type !== 'M') {
      console.log(`[PredictFun WS] unhandled message type=${msg.type} topic=${msg.topic}`);
    }
  }

  _applyBookUpdate(marketId, data) {
    // Find conditionId for this marketId
    let conditionId = null;
    for (const [cid, info] of this._subscribedAssets) {
      if (info.marketId === marketId) { conditionId = cid; break; }
    }
    if (!conditionId) return;

    const bidsArr = Array.isArray(data.bids) ? data.bids : [];
    const asksArr = Array.isArray(data.asks) ? data.asks : [];

    let book = this._books.get(conditionId);
    if (!book) {
      book = { bids: new Map(), asks: new Map() };
      this._books.set(conditionId, book);
    }

    // Full snapshot: clear and rebuild
    book.bids.clear();
    book.asks.clear();
    for (const l of bidsArr) {
      const p = parseFloat(l.price || l[0]);
      const s = parseFloat(l.size  || l[1]);
      if (p > 0 && s > 0) book.bids.set(p.toString(), s);
    }
    for (const l of asksArr) {
      const p = parseFloat(l.price || l[0]);
      const s = parseFloat(l.size  || l[1]);
      if (p > 0 && s > 0) book.asks.set(p.toString(), s);
    }

    this._lastTickAt.set(conditionId, Date.now());
    this._propagateBook(conditionId);
  }

  _propagateBook(conditionId) {
    const book  = this._books.get(conditionId);
    if (!book) return;
    const info  = this._subscribedAssets.get(conditionId);
    const cached = this.markets.get(conditionId);
    if (!cached) return;

    // UP (YES) token: use book asks/bids directly
    const upBestAsk = this._bestPrice(book.asks, 'min');
    const upBestBid = this._bestPrice(book.bids, 'max');

    if (upBestAsk != null) { cached.yesAsk = upBestAsk; cached.yesPrice = upBestAsk; }
    if (upBestBid != null)   cached.yesBid = upBestBid;

    // DOWN (NO) token: derived from complement
    if (upBestBid != null) {
      const downAsk = Math.round((1 - upBestBid) * 1000) / 1000;
      cached.noAsk   = downAsk;
      cached.noPrice = downAsk;
    }
    if (upBestAsk != null) {
      cached.noBid = Math.round((1 - upBestAsk) * 1000) / 1000;
    }

    cached.lastUpdated = new Date().toISOString();
    this.emit('tick', cached);
  }

  _bestPrice(map, mode) {
    let best = null;
    for (const k of map.keys()) {
      const p = parseFloat(k);
      if (!(p > 0)) continue;
      if (best == null || (mode === 'min' ? p < best : p > best)) best = p;
    }
    return best;
  }

  getLiveSnapshot(tokenId) {
    if (!tokenId) return null;
    const market = this._findMarketByTokenId(tokenId);
    if (!market) return null;
    const conditionId = market.conditionId;
    const book = this._books.get(conditionId);
    if (!book) return null;

    const isYes = tokenId === market.yesTokenId;
    const now = Date.now();

    // UP token: use book directly
    if (isYes) {
      const sortedAsks = [...book.asks.entries()]
        .map(([p,s]) => ({ price: parseFloat(p), size: s }))
        .filter(l => l.price > 0 && l.size > 0)
        .sort((a,b) => a.price - b.price).slice(0, 5);
      const sortedBids = [...book.bids.entries()]
        .map(([p,s]) => ({ price: parseFloat(p), size: s }))
        .filter(l => l.price > 0 && l.size > 0)
        .sort((a,b) => b.price - a.price).slice(0, 5);
      const lastAt = this._lastTickAt.get(conditionId) || 0;
      const ageMs = lastAt > 0 ? now - lastAt : -1;
      return {
        bestAsk: sortedAsks[0]?.price ?? null,
        bestAskSize: sortedAsks[0]?.size ?? 0,
        bestBid: sortedBids[0]?.price ?? null,
        bestBidSize: sortedBids[0]?.size ?? 0,
        asks: sortedAsks, bids: sortedBids, ageMs,
        wsAlive: this._wsConnected && (now - this._lastPongAt) < 20_000,
      };
    }

    // DOWN token: derive from UP book complement
    const upAsks = [...book.asks.entries()]
      .map(([p,s]) => ({ price: parseFloat(p), size: s }))
      .filter(l => l.price > 0 && l.size > 0).sort((a,b) => a.price - b.price);
    const upBids = [...book.bids.entries()]
      .map(([p,s]) => ({ price: parseFloat(p), size: s }))
      .filter(l => l.price > 0 && l.size > 0).sort((a,b) => b.price - a.price);

    // Down ask = 1 - Up bid (sorted by price asc)
    const downAsks = upBids.map(l => ({ price: Math.round((1 - l.price) * 1000) / 1000, size: l.size }))
      .sort((a,b) => a.price - b.price).slice(0, 5);
    // Down bid = 1 - Up ask
    const downBids = upAsks.map(l => ({ price: Math.round((1 - l.price) * 1000) / 1000, size: l.size }))
      .sort((a,b) => b.price - a.price).slice(0, 5);

    const lastAt = this._lastTickAt.get(conditionId) || 0;
    return {
      bestAsk: downAsks[0]?.price ?? null,
      bestAskSize: downAsks[0]?.size ?? 0,
      bestBid: downBids[0]?.price ?? null,
      bestBidSize: downBids[0]?.size ?? 0,
      asks: downAsks, bids: downBids,
      ageMs: lastAt > 0 ? now - lastAt : -1,
      wsAlive: this._wsConnected && (now - this._lastPongAt) < 20_000,
    };
  }

  _sendSubscriptions(markets) {
    const marketIds = [];
    for (const m of markets) {
      if (m.marketId) marketIds.push(m.marketId);
    }
    if (marketIds.length === 0) return;
    console.log(`[PredictFun WS] subscribing to ${marketIds.length} market streams`);
    for (const id of marketIds) {
      this._wsSend({
        method: 'subscribe',
        requestId: this._reqId++,
        params: [`predictOrderbook/${id}`],
      });
    }
  }

  unsubscribeMarkets(conditionIds) {
    for (const id of conditionIds) {
      this._subscribedAssets.delete(id);
    }
    // WS reconnect on rotation handles cleanup
  }

  _wsSend(obj) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(obj));
    }
  }

  _wsCleanup() {
    this._wsConnected = false;
    if (this._ws) { this._ws.removeAllListeners(); this._ws = null; }
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;
    console.log(`[PredictFun WS] reconnecting in ${this._reconnectDelay}ms...`);
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this._reconnectDelay = Math.min(this._reconnectDelay * 2, this._maxReconnectDelay);
      this._connectWs();
    }, this._reconnectDelay);
  }

  /* ---------------------------------------------------------- */
  /*  Order placement                                            */
  /* ---------------------------------------------------------- */

  async placeOrder(order) {
    if (!this.connected) {
      console.error('[PredictFun] cannot place order — not connected');
      return null;
    }
    if (!this.wallet) {
      console.error('[PredictFun] cannot place order — no wallet');
      return null;
    }
    if (!this.apiKey) {
      console.error('[PredictFun] cannot place order — no API key');
      return null;
    }

    // Predict Account is required. The order's maker/signer must be the
    // Predict Account (smart wallet), not the signing wallet. Without it,
    // predict.fun returns InvalidSignerError.
    if (!this.predictAccount) {
      console.error('[PredictFun] cannot place order — predictAccountAddress not configured (get it from predict.fun account settings)');
      this._lastOrderError = { type: 'other', message: 'predictAccountAddress not set' };
      return null;
    }

    this._lastOrderError = null;
    // predict.fun CLOB enforces maker === signer even with signatureType=1.
    // The only accepted combination is EOA mode: maker=signer=wallet.address,
    // signatureType=0. The CLOB internally maps the Privy wallet (0x09F...) to
    // the predictAccount (0x010b41...) and debits from there.
    const makerAddress  = this.predictAccount;
    const signerAddress = this.wallet.address;

    const tokenId = order.tokenId;
    const side    = order.side === 'BUY' ? 0 : 1; // 0=BUY, 1=SELL
    // Round to 4 decimals to strip IEEE-754 noise. JS stores 0.62 as
    // 0.61999999999999999556… so price.toFixed(18) returns
    // "0.619999999999999996" — the predict.fun server rejects that as
    // "invalid numeric value". 4 decimals is well past the price tick
    // (typically 0.001) and keeps makerAmount/takerAmount consistent.
    const price   = Math.round(parseFloat(order.price) * 1e4) / 1e4;
    const size    = parseFloat(order.size);

    const market = this._findMarketByTokenId(tokenId);
    if (!market) {
      console.error(`[PredictFun] cannot place order — market not found for tokenId ${tokenId?.slice(0,16)}...`);
      this._lastOrderError = { type: 'other', message: 'market not found' };
      return null;
    }

    const feeRateBps = market.feeRateBps || 100;

    // verifyingContract based on market type
    const vc = market.isNegRisk && market.isYieldBearing ? CONTRACTS.yieldBearingNegRisk
             : market.isNegRisk                          ? CONTRACTS.negRisk
             : market.isYieldBearing                     ? CONTRACTS.yieldBearing
             :                                             CONTRACTS.standard;

    const domain = {
      name:              'predict.fun CTF Exchange',
      version:           '1',
      chainId:           56,
      verifyingContract: vc,
    };

    // BUY: makerAmount=USDT to pay, takerAmount=shares to receive
    // SELL: makerAmount=shares to sell, takerAmount=USDT to receive
    const sharesWei = toWei(size);
const priceWei  = toWei(price);

// Cálculo base sem buffer manual — slippage é tratado pelo slippageBps no body
let usdtWei = (priceWei * sharesWei) / BigInt(1_000_000_000_000_000_000n);

const makerAmount = side === 0 ? usdtWei : sharesWei;
const takerAmount = side === 0 ? sharesWei : usdtWei;

console.log(`[PredictFun] DEBUG AMOUNTS → size=${size} price=${price} feeRateBps=${feeRateBps} | usdtWei=${usdtWei} sharesWei=${sharesWei} makerAmount=${makerAmount}`);

const salt = BigInt(Math.floor(Math.random() * 1e15));
const expiration = BigInt(Math.floor(Date.now() / 1000) + 300);
const nonce = BigInt(0);

const orderValue = {
  salt: salt,
  maker: makerAddress,    // predictAccount
  signer: makerAddress,   // predictAccount — doc: "set signer and maker to predictAccount"
  taker: '0x0000000000000000000000000000000000000000',
  tokenId: BigInt(tokenId),
  makerAmount,
  takerAmount,
  expiration,
  nonce,
  feeRateBps: BigInt(feeRateBps),
  side: side,
  signatureType: 0,
};

    // predict.fun API requires pricePerShare as uint256 wei string (1e18).
    // e.g. 0.3 → "300000000000000000". toWei() handles IEEE-754 precision.
    const priceStr     = toWei(price).toString();
    const makerAmtStr  = makerAmount.toString();
    const takerAmtStr  = takerAmount.toString();
    const body = {
      data: {
        strategy:      'MARKET',
        pricePerShare: priceStr,
        isMinAmountOut: side === 0,
        slippageBps:   50, // 0.5% slippage tolerance to improve fill rate
        order: {
          salt:          salt.toString(),
          maker:         makerAddress,
          signer:        makerAddress,
          taker:         '0x0000000000000000000000000000000000000000',
          tokenId:       tokenId,
          makerAmount:   makerAmtStr,
          takerAmount:   takerAmtStr,
          expiration:    expiration.toString(),
          nonce:         '0',
          feeRateBps:    String(feeRateBps),
          side:          side,
          signatureType: 0,
          signature,
        },
      },
    };

    await this._ensureJwt();
    try {
      const t0 = Date.now();
      const resp = await axios.post(`${this.baseUrl}/v1/orders`, body, {
        headers: this._headers(),
        timeout: 15_000,
      });
      const postMs = Date.now() - t0;
      console.log(`[PredictFun] POST /v1/orders ${postMs}ms`);

      const rdata = resp.data?.data || resp.data;
      if (!rdata?.orderId) {
        const errStr = JSON.stringify(resp.data).slice(0, 200);
        console.error(`[PredictFun] order POST returned no orderId: ${errStr}`);
        this._lastOrderError = { type: 'other', message: errStr };
        return null;
      }

      const orderId = String(rdata.orderId);
      console.log(`[PredictFun] order placed orderId=${orderId} — polling for fill...`);

      // Poll fill status (max 3s)
      const fillData = await this._pollFill(orderId, 3000);
      if (fillData) {
        // Map to Polymarket-compatible response shape
        return {
          orderId,
          orderHash:    rdata.orderHash || '',
          takingAmount: fillData.amountReceived, // matches Polymarket .takingAmount
          makingAmount: fillData.amountSpent,
          status:       fillData.status,
          fillPrice:    fillData.fillPrice,
          filledShares: fillData.filledShares,
        };
      }

      // Timeout — treat as unfilled (dispatcher will hedge)
      console.warn(`[PredictFun] fill poll timeout for orderId=${orderId}`);
      this._lastOrderError = { type: 'liquidity', message: 'fill timeout — order may be pending' };
      return null;
    } catch (err) {
      const status = err.response?.status;
      const errBody = JSON.stringify(err.response?.data || err.message).slice(0, 300);
      console.error(`[PredictFun] POST /v1/orders FAILED (HTTP ${status}): ${errBody}`);
      this._lastOrderError = this._classifyOrderError(errBody, status);
      return null;
    }
  }

  async _pollFill(orderId, maxMs) {
    const POLL_INTERVAL = 400;
    const deadline = Date.now() + maxMs;
    await this._ensureJwt();

    while (Date.now() < deadline) {
      try {
        const resp = await axios.get(`${this.baseUrl}/v1/orders`, {
          headers: this._headers(),
          params:  { orderId, limit: 1 },
          timeout: 5_000,
        });
        const data = resp.data?.data || resp.data;
        const orders = Array.isArray(data) ? data : (data?.orders || []);
        const order = orders.find(o => String(o.id || o.orderId) === orderId);
        if (order) {
          const status = order.status || '';
          if (status === 'FILLED' || status === 'PARTIALLY_FILLED') {
            return this._parseFill(order);
          }
          if (status === 'CANCELLED' || status === 'EXPIRED') {
            console.warn(`[PredictFun] order ${orderId} status=${status}`);
            this._lastOrderError = { type: 'liquidity', message: `order ${status}` };
            return null;
          }
        }
      } catch (_) {}
      await new Promise(r => setTimeout(r, POLL_INTERVAL));
    }
    return null;
  }

  _parseFill(order) {
    // amountFilled is in wei (shares received for BUY, USDT received for SELL)
    const raw = order.order || {};
    const side = raw.side === 0 || raw.side === 'BUY' ? 'BUY' : 'SELL';
    const amountFilled = BigInt(order.amountFilled || order.amount || 0);

    if (side === 'BUY') {
      const sharesReceived = Number(amountFilled) / Number(WEI);
      const usdtSpent = Number(BigInt(raw.makerAmount || 0)) / Number(WEI);
      const fillPrice = sharesReceived > 0 ? usdtSpent / sharesReceived : 0;
      return {
        filledShares:  sharesReceived,
        amountReceived: String(sharesReceived), // shares (matches Polymarket takingAmount = tokens)
        amountSpent:    String(usdtSpent),
        fillPrice,
        status: order.status,
      };
    } else {
      const sharesGiven   = Number(BigInt(raw.makerAmount || 0)) / Number(WEI);
      const usdtReceived  = Number(amountFilled) / Number(WEI);
      const fillPrice     = sharesGiven > 0 ? usdtReceived / sharesGiven : 0;
      return {
        filledShares:  sharesGiven,
        amountReceived: String(usdtReceived), // USDT (matches Polymarket takingAmount for SELL)
        amountSpent:    String(sharesGiven),
        fillPrice,
        status: order.status,
      };
    }
  }

  _classifyOrderError(errStr, httpStatus) {
    const lower = String(errStr || '').toLowerCase();
    let type = 'other';
    if (lower.includes('balance') || lower.includes('insufficient')) type = 'balance';
    else if (lower.includes('filled') || lower.includes('liquidity')) type = 'liquidity';
    else if (lower.includes('closed') || httpStatus === 404)           type = 'market_closed';
    return { type, message: String(errStr).slice(0, 300), polyBalance: null, polyRequired: null, httpStatus: httpStatus || null };
  }

  /* ---------------------------------------------------------- */
  /*  Generic platform interface (used by decisionEngine/dispatcher) */
  /* ---------------------------------------------------------- */

  /** Hedge/exit floor price for SELL sweep orders. Predict.fun: sub-cent. */
  get floorPrice() { return 0.001; }

  setFeeRate(rate) { if (typeof rate === 'number' && rate >= 0) this.feeRate = rate; }

  getMarketKey(market) { return market.conditionId; }

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
   * Per-market feeRateBps from the API. Falls back to instance default.
   * CTF formula on entry: fee_tokens = feeRate × min(P, 1-P) / P.
   * On exit (SELL): fee_usdt = feeRate × min(P, 1-P) per share.
   */
  _marketFeeRate(market) {
    if (market?.feeRateBps != null) return market.feeRateBps / 10000;
    return this.feeRate;
  }
  calcEntryFee(market, price) {
    if (!(price > 0) || !(price < 1)) return 0;
    return this._marketFeeRate(market) * Math.min(price, 1 - price) / price;
  }
  calcExitFee(market, price) {
    if (!(price > 0) || !(price < 1)) return 0;
    return this._marketFeeRate(market) * Math.min(price, 1 - price);
  }

  getLiveSnapshotForSide(market, side) {
    const tokenId = this.getSideToken(market, side);
    if (!tokenId) return null;
    return this.getLiveSnapshot(tokenId);
  }

  getDepthAtCeiling(market, side, ceilingPrice) {
    const snap = this.getLiveSnapshotForSide(market, side);
    if (!snap?.asks?.length) return 0;
    let total = 0;
    for (const a of snap.asks) {
      if (a.price <= ceilingPrice + 1e-9) total += a.size;
    }
    return total;
  }

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
   * Predict.fun SELL has a per-share fee (CTF formula). Apply per-level.
   */
  simulateSellFill(market, side, qty) {
    const empty = { qtyFillable: 0, netRevenue: 0, avgFillPrice: 0, depleted: true, levelsUsed: 0, haircutApplied: 0 };
    const snap = this.getLiveSnapshotForSide(market, side);
    if (!snap?.bids?.length || !(qty > 0)) return empty;

    const feeRate = this._marketFeeRate(market);
    let remaining = qty;
    let grossRevenue = 0;
    let netRaw = 0;
    let qtyFillable = 0;
    let levelsUsed = 0;
    for (const b of snap.bids) {
      if (!(b.price > 0) || !(b.size > 0)) continue;
      const consume = Math.min(remaining, b.size);
      const sellFeePerShare = feeRate > 0 ? feeRate * Math.min(b.price, 1 - b.price) : 0;
      grossRevenue += consume * b.price;
      netRaw       += consume * (b.price - sellFeePerShare);
      qtyFillable  += consume;
      remaining    -= consume;
      levelsUsed++;
      if (remaining <= 0) break;
    }
    if (qtyFillable === 0) return empty;

    const haircutScale = config.execution?.predictfunHaircutScale ?? 0.60;
    const avgRawBid = grossRevenue / qtyFillable;
    const haircut = Math.min(0.30, (1 - avgRawBid) * haircutScale);
    const netRevenue = netRaw * (1 - haircut);
    const avgFillPrice = qtyFillable > 0 ? netRevenue / qtyFillable : 0;
    return { qtyFillable, netRevenue, avgFillPrice, depleted: remaining > 0, levelsUsed, haircutApplied: haircut };
  }

  async placeBuyOrder(market, side, price, qty) {
    const tokenId = this.getSideToken(market, side);
    return this.placeOrder({ tokenId, price, size: qty, side: 'BUY' });
  }
  async placeSellOrder(market, side, price, qty) {
    const tokenId = this.getSideToken(market, side);
    return this.placeOrder({ tokenId, price, size: qty, side: 'SELL' });
  }

  async getSideTokenBalance(market, side) {
    const tokenId = this.getSideToken(market, side);
    if (!tokenId) return null;
    return this.getTokenBalance(tokenId);
  }

  /**
   * Shares received by a BUY order. _parseFill normalises this into
   * filledShares already.
   */
  extractBuyFillQty(result, _intendedPrice) {
    if (!result) return 0;
    return parseFloat(result.filledShares || 0);
  }

  /** SELL outcome — takingAmount is USDT received from the polled fill. */
  extractSellOutcome(result) {
    if (!result) return { soldQty: 0, takingAmount: 0, orderId: null };
    const taking = parseFloat(result.takingAmount || 0);
    const soldQty = parseFloat(result.filledShares || 0);
    return { soldQty, takingAmount: taking, orderId: result.orderId || null };
  }

  /** Predict.fun returns avg fill price directly from _parseFill. */
  extractFillPrice(orderResult) {
    if (!orderResult) return null;
    const fp = parseFloat(orderResult.fillPrice || 0);
    return fp > 0 ? fp : null;
  }

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

module.exports = PredictFunConnector;
