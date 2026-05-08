/**
 * predict.fun connector — reescrito usando o SDK oficial @predictdotfun/sdk
 *
 * Fonte: https://dev.predict.fun/how-to-create-or-cancel-orders-679306m0.md
 *        https://dev.predict.fun/ts-how-to-authenticate-your-api-requests-663127m0.md
 *        https://github.com/PredictDotFun/sdk
 *
 * Mudanças principais vs versão anterior:
 *   - _authenticate(): usa builder.signPredictAccountMessage() + signer=predictAccount
 *     (a doc diz explicitamente que wallet.signMessage() NÃO funciona para Predict Accounts)
 *   - placeOrder(): usa builder.buildOrder() + builder.buildTypedData() + builder.signTypedDataOrder()
 *     maker/signer são setados automaticamente para predictAccount quando passado no make()
 *   - ensureUsdtApproval(): usa builder.setApprovals() do SDK
 *   - Todo o resto (WS, market discovery, book, hedge) permanece inalterado
 */

const axios        = require('axios');
const { ethers }   = require('ethers');
const WebSocket    = require('ws');
const EventEmitter = require('events');
// SDK oficial da predict.fun
const { OrderBuilder, ChainId, Side } = require('@predictdotfun/sdk');
const config = require('../config');

const BASE_URL      = config.predictfun?.baseUrl || 'https://api.predict.fun';
const WS_URL        = config.predictfun?.wsUrl   || 'wss://ws.predict.fun/ws';
const INTERVAL_SECS = 900;

// BSC USDT — só usado para getBalance() via on-chain
const BSC_USDT_ADDRESS = '0x55d398326f99059fF775485246999027B3197955';
const BSC_USDT_ABI     = ['function balanceOf(address) view returns (uint256)'];
const BSC_RPCS = [
  'https://bsc-dataseed.binance.org',
  'https://bsc-dataseed1.binance.org',
  'https://bsc.publicnode.com',
  'https://bsc-dataseed2.binance.org',
];

const WEI = BigInt('1000000000000000000');

function toWei(n) {
  return BigInt(Math.round(n * 1e9)) * BigInt(1e9);
}

class PredictFunConnector extends EventEmitter {
  constructor() {
    super();
    this.platform           = 'predictfun';
    this.feeRate            = 0.01;
    this.minOrderNotional   = 1.00;
    this.baseUrl            = BASE_URL;
    this.apiKey             = '';
    this.wallet             = null;   // ethers.Wallet (Privy EOA — assina)
    this.predictAccount     = null;   // Smart Wallet address (deposit address)
    this.connected          = false;

    // SDK oficial — inicializado em loadUserKeys() após ter wallet + predictAccount
    this._orderBuilder      = null;

    this.markets            = new Map();
    this._jwt               = null;
    this._jwtExpiry         = 0;

    // WS state
    this._ws                = null;
    this._wsConnected       = false;
    this._reconnectDelay    = 1000;
    this._maxReconnectDelay = 30_000;
    this._reconnectTimer    = null;
    this._lastPongAt        = 0;
    this._reqId             = 1;

    this._books             = new Map();
    this._lastTickAt        = new Map();
    this._subscribedAssets  = new Map();
    this._lastOrderError    = null;
    this._bscProviders      = null;
  }

  /* ---------------------------------------------------------- */
  /*  Interface compatibility                                    */
  /* ---------------------------------------------------------- */

  isReady() {
    return !!(this.wallet && this.apiKey && this._orderBuilder);
  }

  async getBalance() {
    const targetAddress = this.predictAccount || this.wallet?.address;
    if (!targetAddress) return null;
    if (!this._bscProviders) {
      this._bscProviders = BSC_RPCS.map(rpc =>
        new ethers.JsonRpcProvider(rpc, 56, { staticNetwork: true, batchMaxCount: 1 })
      );
    }
    for (const provider of this._bscProviders) {
      try {
        const usdt = new ethers.Contract(BSC_USDT_ADDRESS, BSC_USDT_ABI, provider);
        const raw  = await Promise.race([
          usdt.balanceOf(targetAddress),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 5000)),
        ]);
        return Number(raw) / 1e18;
      } catch (_) {}
    }
    return null;
  }

  async getTokenBalance(tokenId) {
    if (!tokenId) return null;
    await this._ensureJwt();
    try {
      const resp = await axios.get(`${this.baseUrl}/v1/positions`, {
        headers: this._headers(),
        timeout: 8_000,
      });
      const data      = resp.data?.data || resp.data;
      const positions = Array.isArray(data) ? data : [];
      for (const pos of positions) {
        if (String(pos.outcome?.onChainId) === String(tokenId)) {
          return Number(BigInt(pos.amount || '0')) / 1e18;
        }
      }
      return 0;
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
   * Autenticação para Predict Account (Smart Wallet).
   *
   * Doc oficial: https://dev.predict.fun/ts-how-to-authenticate-your-api-requests-663127m0.md
   *   - signer no body = predictAccount address (NÃO wallet.address)
   *   - A assinatura DEVE usar builder.signPredictAccountMessage()
   *     "The standard signMessage won't work"
   */
  async _authenticate() {
    if (!this.wallet || !this._orderBuilder) return false;
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

      // Doc: "Sign the message using the SDK function for Predict accounts.
      //       The standard signMessage won't work."
      const signature = await this._orderBuilder.signPredictAccountMessage(message);

      // Doc: signer = predictAccount address (deposit address), NÃO wallet.address
      const authResp = await axios.post(`${this.baseUrl}/v1/auth`, {
        signer:    this.predictAccount,
        signature,
        message,
      }, { headers: { 'x-api-key': this.apiKey }, timeout: 10_000 });

      const token = authResp.data?.data?.token || authResp.data?.token;
      if (!token) {
        console.warn('[PredictFun] auth returned no token');
        return false;
      }
      this._jwt       = token;
      this._jwtExpiry = Date.now() + 23 * 3600 * 1000;
      console.log(`[PredictFun] JWT obtained — predictAccount=${this.predictAccount.slice(0, 10)}...`);
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
  /*  Keys + SDK init                                            */
  /* ---------------------------------------------------------- */

  loadUserKeys(keys) {
    if (!keys?.predictApiKey) return;
    this.apiKey = keys.predictApiKey;

    const pk = keys.predictPrivateKey || keys.polyPrivateKey;
    if (pk) {
      try {
        this.wallet = new ethers.Wallet(pk);
      } catch (err) {
        console.error('[PredictFun] invalid private key:', err.message);
        return;
      }
    }

    if (keys.predictAccountAddress) {
      this.predictAccount = keys.predictAccountAddress;
    }

    // Inicializa o OrderBuilder do SDK oficial assim que temos wallet + predictAccount.
    // Doc: OrderBuilder.make(ChainId.BnbMainnet, signer, { predictAccount })
    //      maker e signer das ordens são setados automaticamente para predictAccount.
    if (this.wallet && this.predictAccount) {
      // make() é async — iniciamos em background; connect() aguarda via _ensureOrderBuilder()
      this._initOrderBuilder().catch(err =>
        console.error('[PredictFun] OrderBuilder init error:', err.message)
      );
    }

    const acct = this.predictAccount
      ? this.predictAccount.slice(0, 10) + '...'
      : 'not set';
    console.log(
      `[PredictFun] keys loaded — signerWallet: ${this.wallet?.address.slice(0, 10)}...` +
      ` predictAccount: ${acct} apiKey: ${this.apiKey.slice(0, 8)}...`
    );
  }

  async _initOrderBuilder() {
    if (!this.wallet || !this.predictAccount) return;
    try {
      // Doc: OrderBuilder.make(chainId, privyWallet, { predictAccount })
      // Com predictAccount, buildOrder() seta maker=signer=predictAccount automaticamente.
      this._orderBuilder = await OrderBuilder.make(ChainId.BnbMainnet, this.wallet, {
        predictAccount: this.predictAccount,
      });
      console.log('[PredictFun] OrderBuilder initialized (SDK oficial)');
    } catch (err) {
      console.error('[PredictFun] OrderBuilder.make failed:', err.message);
    }
  }

  async _ensureOrderBuilder() {
    if (this._orderBuilder) return;
    await this._initOrderBuilder();
  }

  /* ---------------------------------------------------------- */
  /*  REST connection                                            */
  /* ---------------------------------------------------------- */

  async connect() {
    const candidates = ['/v1/markets', '/markets', '/v1/categories'];
    for (const path of candidates) {
      try {
        await axios.get(`${this.baseUrl}${path}`, {
          headers: { 'x-api-key': this.apiKey || '' },
          timeout: 10_000,
        });
        console.log(`[PredictFun] connected — REST API reachable (${path})`);
        this.connected = true;
        // Garante que OrderBuilder está pronto antes de tentar auth
        await this._ensureOrderBuilder();
        if (this._orderBuilder) this._authenticate().catch(() => {});
        return;
      } catch (err) {
        if (err.response) {
          console.log(`[PredictFun] connected — REST API reachable (${path} → ${err.response.status})`);
          this.connected = true;
          await this._ensureOrderBuilder();
          if (this._orderBuilder) this._authenticate().catch(() => {});
          return;
        }
      }
    }
    console.error('[PredictFun] connection failed — no response from API (all candidates)');
    this.connected = false;
  }

  /* ---------------------------------------------------------- */
  /*  USDT approval — usa SDK                                    */
  /* ---------------------------------------------------------- */

  /**
   * Doc: orderBuilder.setApprovals() — seta approvals para CTF_EXCHANGE e NEG_RISK_CTF_EXCHANGE.
   * https://dev.predict.fun/how-to-create-or-cancel-orders-679306m0.md#how-to-set-approvals
   */
  async ensureUsdtApproval() {
    await this._ensureOrderBuilder();
    if (!this._orderBuilder) {
      console.warn('[PredictFun] ensureUsdtApproval: OrderBuilder não inicializado');
      return;
    }
    try {
      console.log('[PredictFun] setApprovals via SDK...');
      const result = await this._orderBuilder.setApprovals();
      if (result.success) {
        console.log('[PredictFun] USDT approvals OK (SDK)');
      } else {
        console.warn('[PredictFun] setApprovals retornou success=false');
      }
    } catch (err) {
      console.warn(`[PredictFun] setApprovals error: ${err.message}`);
    }
  }

  /* ---------------------------------------------------------- */
  /*  Market discovery — INALTERADO                             */
  /* ---------------------------------------------------------- */

  _get15mTimestamps() {
    const nowSecs = Math.floor(Date.now() / 1000);
    const current = Math.floor(nowSecs / INTERVAL_SECS) * INTERVAL_SECS;
    const next    = current + INTERVAL_SECS;
    return [current, next];
  }

  _buildCategorySlugs(asset, windowTs) {
    const sym   = asset.symbol.toLowerCase();
    const slugs = [];
    slugs.push(`${sym}-updown-15m-${windowTs}`);
    for (const offsetH of [0, -3, -4]) {
      for (const extraSecs of [0, 900]) {
        const d    = new Date((windowTs + extraSecs + offsetH * 3600) * 1000);
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

  async _discoverByParentCategory(asset, out) {
    const sym        = asset.symbol.toLowerCase();
    const slug       = `${sym}-updown-15m`;
    const legacySlug = `${sym}-usd-up-down`;
    const nowSecs    = Math.floor(Date.now() / 1000);
    const [currentTs] = this._get15mTimestamps();
    const windowEndSecs = currentTs + INTERVAL_SECS * 2 + 120;

    for (const trySlug of [slug, legacySlug]) {
      try {
        const resp = await axios.get(`${this.baseUrl}/v1/categories/${trySlug}`, {
          headers: this._headers(),
          params:  { limit: 50 },
          timeout: 12_000,
        });
        const data  = resp.data?.data || resp.data;
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
        if (out.length > 0) break;
      } catch (_) {}
    }
  }

  async getMarkets(asset, options = {}) {
    if (!this.connected) return [];
    if (!asset.predictfun) return [];

    const allMarkets = [];

    if (allMarkets.length === 0) await this._discoverByParentCategory(asset, allMarkets);

    const kalshiTickers = options.kalshiTickers || [];
    if (allMarkets.length === 0 && kalshiTickers.length > 0) {
      await this._discoverByKalshiTickers(asset, kalshiTickers, allMarkets);
    }

    if (allMarkets.length === 0) {
      const [currentTs] = this._get15mTimestamps();
      for (const slug of this._buildCategorySlugs(asset, currentTs)) {
        if (allMarkets.length > 0) break;
        try {
          const resp  = await axios.get(`${this.baseUrl}/v1/categories/${encodeURIComponent(slug)}`, {
            headers: this._headers(),
            timeout: 10_000,
          });
          const data  = resp.data?.data || resp.data;
          const items = Array.isArray(data) ? data : (data?.markets || [data].filter(Boolean));
          for (const raw of items) {
            const normalized = this._normalize(raw, asset);
            if (normalized && !allMarkets.some(m => m.conditionId === normalized.conditionId)) {
              allMarkets.push(normalized);
            }
          }
          if (allMarkets.length > 0) console.log(`[PredictFun] ${asset.symbol} found via slug: ${slug}`);
        } catch (_) {}
      }
    }

    if (allMarkets.length === 0) {
      try { await this._discoverViaSearch(asset, allMarkets); } catch (_) {}
    }

    if (allMarkets.length > 0) await this._enrichTokenIds(allMarkets);

    for (const m of allMarkets) this.markets.set(m.conditionId, m);

    if (allMarkets.length > 0) {
      console.log(`[PredictFun] ${asset.symbol} — found ${allMarkets.length} market(s): "${allMarkets[0].question}"`);
    } else {
      console.log(`[PredictFun] ${asset.symbol} — no 15-min market found`);
    }

    return allMarkets;
  }

  async _discoverByKalshiTickers(asset, kalshiTickers, out) {
    const sym = asset.symbol.toLowerCase();
    for (const ticker of kalshiTickers) {
      try {
        const resp  = await axios.get(`${this.baseUrl}/v1/markets`, {
          headers: this._headers(),
          params:  { kalshiMarketTicker: ticker, limit: 5 },
          timeout: 10_000,
        });
        const data  = resp.data?.data || resp.data;
        const items = Array.isArray(data) ? data : (data?.markets || data?.items || []);
        for (const raw of items) {
          const kField       = raw.kalshiMarketTicker;
          const fieldPresent = Object.prototype.hasOwnProperty.call(raw, 'kalshiMarketTicker');
          if (fieldPresent && (!kField || kField !== ticker)) continue;
          const title = (raw.title || raw.question || '').toLowerCase();
          if (!title.includes(sym)) continue;
          const normalized = this._normalize(raw, asset);
          if (normalized && !out.some(m => m.conditionId === normalized.conditionId)) {
            out.push(normalized);
            console.log(`[PredictFun] ${asset.symbol} found via kalshiTicker=${ticker}: "${raw.title}"`);
          }
        }
      } catch (err) {
        console.warn(`[PredictFun] kalshiTicker=${ticker} search failed: ${err.response?.status || err.message}`);
      }
    }
  }

  async _discoverViaSearch(asset, out) {
    const sym   = asset.symbol.toLowerCase();
    const passes = [
      { label: 'variant+open', baseParams: { marketVariant: 'CRYPTO_UP_DOWN', status: 'OPEN', limit: 20 }, maxPages: 10 },
      { label: 'variant-only', baseParams: { marketVariant: 'CRYPTO_UP_DOWN', limit: 20 },                maxPages: 10 },
      { label: 'status=OPEN',  baseParams: { status: 'OPEN', limit: 20 },                                 maxPages: 80 },
    ];

    for (const { label, baseParams, maxPages } of passes) {
      let cursor = null;
      let page   = 0;
      while (page < maxPages) {
        const params = { ...baseParams };
        if (cursor) params.cursor = cursor;
        try {
          const resp       = await axios.get(`${this.baseUrl}/v1/markets`, { headers: this._headers(), params, timeout: 10_000 });
          const body       = resp.data;
          const data       = body?.data || body;
          const items      = Array.isArray(data) ? data : (data?.markets || data?.items || []);
          const nextCursor = body?.cursor || null;
          for (const raw of items) {
            if (raw.status === 'RESOLVED' || raw.tradingStatus === 'CLOSED') continue;
            const title = (raw.title || raw.question || raw.categorySlug || '').toLowerCase();
            if (!title.includes(sym) && !title.includes('bitcoin')) continue;
            const normalized = this._normalize(raw, asset);
            if (normalized && normalized.active && !out.some(m => m.conditionId === normalized.conditionId)) out.push(normalized);
          }
          if (out.length > 0) { console.log(`[PredictFun] ${asset.symbol} found via search pass=${label} page ${page + 1}`); return; }
          if (!nextCursor) break;
          cursor = nextCursor;
          page++;
        } catch (err) {
          console.warn(`[PredictFun] search pass=${label} page ${page + 1} failed: ${err.response?.status || err.message}`);
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
    const outcomes    = Array.isArray(raw.outcomes) ? raw.outcomes : [];
    const upOutcome   = outcomes.find(o => o.indexSet === 1 || o.name?.toLowerCase() === 'up');
    const downOutcome = outcomes.find(o => o.indexSet === 2 || o.name?.toLowerCase() === 'down');
    const yesTokenId  = String(upOutcome?.onChainId   || '');
    const noTokenId   = String(downOutcome?.onChainId || '');
    const isNegRisk      = raw.isNegRisk      === true;
    const isYieldBearing = raw.isYieldBearing === true;
    const feeRateBps     = parseInt(raw.feeRateBps || 100, 10);
    const endDate        = raw.endDate || raw.endAt || null;
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
    if (strike != null) { strike = parseFloat(strike); if (isNaN(strike)) strike = null; }
    return {
      platform: 'predictfun', asset: asset.symbol, conditionId,
      question: raw.title || raw.question || '', slug: raw.categorySlug || raw.slug || conditionId,
      outcomes: outcomes.map(o => o.name), yesTokenId, noTokenId,
      yesPrice: 0, noPrice: 0, yesAsk: 0, noAsk: 0, yesBid: 0, noBid: 0,
      volume: parseFloat(raw.volume || 0), liquidity: parseFloat(raw.liquidity || 0),
      endDate, active: raw.tradingStatus !== 'CLOSED' && raw.status !== 'RESOLVED' && raw.status !== 'CLOSED',
      closed: raw.status === 'RESOLVED' || raw.tradingStatus === 'CLOSED',
      strike, feeRateBps, isNegRisk, isYieldBearing, marketId: raw.id,
      lastUpdated: new Date().toISOString(),
    };
  }

  async _enrichTokenIds(markets) {
    for (const m of markets) {
      if (m.yesTokenId && m.noTokenId) continue;
      if (!m.marketId) continue;
      try {
        const resp = await axios.get(`${this.baseUrl}/v1/markets/${m.marketId}`, { headers: this._headers(), timeout: 8_000 });
        const raw  = resp.data?.data || resp.data;
        if (!raw) continue;
        const outcomes = Array.isArray(raw.outcomes) ? raw.outcomes : [];
        const upOut    = outcomes.find(o => o.indexSet === 1 || o.name?.toLowerCase() === 'up');
        const downOut  = outcomes.find(o => o.indexSet === 2 || o.name?.toLowerCase() === 'down');
        if (upOut?.onChainId)   m.yesTokenId = String(upOut.onChainId);
        if (downOut?.onChainId) m.noTokenId  = String(downOut.onChainId);
        console.log(`[PredictFun] ${m.asset} tokenIds enriched — yes="${m.yesTokenId}" no="${m.noTokenId}"`);
      } catch (err) {
        console.debug(`[PredictFun] _enrichTokenIds(${m.marketId}) failed: ${err.message}`);
      }
    }
  }

  /* ---------------------------------------------------------- */
  /*  Orderbook REST — INALTERADO                               */
  /* ---------------------------------------------------------- */

  async getOrderbook(tokenId) {
    const market = this._findMarketByTokenId(tokenId)
               || [...this.markets.values()].find(m => m.conditionId === tokenId);
    if (!market) return null;
    const data = await this._fetchRawOrderbook(market.marketId, market.slug || market.conditionId);
    if (!data) return null;
    return this._normalizeBook(data, tokenId, market);
  }

  async _fetchRawOrderbook(marketId, slugOrId) {
    const candidates = [
      `/v1/markets/${marketId}/orderbook`,
      `/v1/orderbook/${marketId}`,
      `/v1/orderbooks/${marketId}`,
      `/v1/orderbook?marketId=${marketId}`,
    ];
    for (const path of candidates) {
      try {
        const url  = path.includes('?')
          ? `${this.baseUrl}${path.split('?')[0]}?${path.split('?')[1]}`
          : `${this.baseUrl}${path}`;
        const resp = await axios.get(url, { headers: this._headers(), timeout: 10_000 });
        const data = resp.data?.data || resp.data;
        if (data && (data.bids || data.asks)) {
          console.log(`[PredictFun] orderbook endpoint confirmed: ${path}`);
          return data;
        }
      } catch (err) {
        if (err.response?.status === 404) continue;
      }
    }
    console.log(`[PredictFun] all orderbook endpoints returned 404 for marketId=${marketId}`);
    return null;
  }

  _normalizeBook(data, tokenId, market) {
    if (!data) return null;
    const isYes   = tokenId === market.yesTokenId;
    const rawBids = (data.bids || []).map(l => ({ price: parseFloat(l.price || l[0]), size: parseFloat(l.size || l[1]) }));
    const rawAsks = (data.asks || []).map(l => ({ price: parseFloat(l.price || l[0]), size: parseFloat(l.size || l[1]) }));
    if (isYes) {
      return { bids: rawBids.sort((a,b) => b.price - a.price), asks: rawAsks.sort((a,b) => a.price - b.price) };
    }
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
  /*  WebSocket — INALTERADO                                    */
  /* ---------------------------------------------------------- */

  subscribeWs(markets = []) {
    for (const m of markets) {
      if (m.conditionId) {
        this._subscribedAssets.set(m.conditionId, {
          asset: m.asset, marketId: m.marketId,
          yesTokenId: m.yesTokenId, noTokenId: m.noTokenId, conditionId: m.conditionId,
        });
      }
    }
    this._primeBooks(markets).catch(() => {});
    if (!this._bookRefreshTimer) {
      this._bookRefreshTimer = setInterval(() => this._refreshBooksIfStale(), 30_000);
      if (this._bookRefreshTimer.unref) this._bookRefreshTimer.unref();
    }
    if (this._ws && this._wsConnected) { this._sendSubscriptions(markets); return; }
    if (this._ws) return;
    this._connectWs();
  }

  async _primeBooks(markets) {
    for (const m of markets) {
      if (!m.marketId) continue;
      const data = await this._fetchRawOrderbook(m.marketId, m.slug || m.conditionId);
      if (data) {
        this._applyBookUpdate(m.marketId, data);
        const cached = this.markets.get(m.conditionId);
        if (cached) console.log(`[PredictFun] ${m.asset} book primed: yesAsk=${cached.yesAsk?.toFixed(4)} noAsk=${cached.noAsk?.toFixed(4)}`);
      }
    }
  }

  async _refreshBooksIfStale() {
    const now = Date.now();
    for (const [conditionId, info] of this._subscribedAssets) {
      const lastTick = this._lastTickAt.get(conditionId) || 0;
      if (now - lastTick < 25_000) continue;
      const data = await this._fetchRawOrderbook(info.marketId, conditionId).catch(() => null);
      if (data) this._applyBookUpdate(info.marketId, data);
    }
  }

  _connectWs() {
    try { this._ws = new WebSocket(WS_URL); } catch (err) {
      console.error('[PredictFun WS] failed to create connection:', err.message);
      this._scheduleReconnect(); return;
    }
    this._ws.on('open', () => {
      console.log('[PredictFun WS] connected');
      this._wsConnected    = true;
      this._lastPongAt     = Date.now();
      this._reconnectDelay = 1000;
      if (this._subscribedAssets.size > 0) this._sendSubscriptions([...this._subscribedAssets.values()]);
    });
    this._ws.on('message', (raw) => {
      try { this._handleWsMessage(JSON.parse(raw.toString())); } catch (e) { console.error('[PredictFun WS] parse error:', e.message); }
    });
    this._ws.on('close', (code) => {
      console.log(`[PredictFun WS] closed (code: ${code})`);
      this._wsCleanup(); this._scheduleReconnect();
    });
    this._ws.on('error', (err) => { console.error('[PredictFun WS] error:', err.message); });
  }

  _handleWsMessage(msg) {
    if (!msg) return;
    if (msg.type === 'M' && msg.topic === 'heartbeat') {
      this._lastPongAt = Date.now();
      this._wsSend({ method: 'heartbeat', data: msg.data });
      return;
    }
    if (msg.type === 'R') return;
    if (msg.type === 'M' && typeof msg.topic === 'string' && msg.topic.startsWith('predictOrderbook/')) {
      const marketId = parseInt(msg.topic.split('/')[1], 10);
      if (!isNaN(marketId) && msg.data) this._applyBookUpdate(marketId, msg.data);
      return;
    }
    if (msg.type && msg.type !== 'M') console.log(`[PredictFun WS] unhandled message type=${msg.type} topic=${msg.topic}`);
  }

  _applyBookUpdate(marketId, data) {
    let conditionId = null;
    for (const [cid, info] of this._subscribedAssets) {
      if (info.marketId === marketId) { conditionId = cid; break; }
    }
    if (!conditionId) return;
    const bidsArr = Array.isArray(data.bids) ? data.bids : [];
    const asksArr = Array.isArray(data.asks) ? data.asks : [];
    let book = this._books.get(conditionId);
    if (!book) { book = { bids: new Map(), asks: new Map() }; this._books.set(conditionId, book); }
    book.bids.clear(); book.asks.clear();
    for (const l of bidsArr) { const p = parseFloat(l.price || l[0]); const s = parseFloat(l.size || l[1]); if (p > 0 && s > 0) book.bids.set(p.toString(), s); }
    for (const l of asksArr) { const p = parseFloat(l.price || l[0]); const s = parseFloat(l.size || l[1]); if (p > 0 && s > 0) book.asks.set(p.toString(), s); }
    this._lastTickAt.set(conditionId, Date.now());
    this._propagateBook(conditionId);
  }

  _propagateBook(conditionId) {
    const book   = this._books.get(conditionId);
    if (!book) return;
    const cached = this.markets.get(conditionId);
    if (!cached) return;
    const upBestAsk = this._bestPrice(book.asks, 'min');
    const upBestBid = this._bestPrice(book.bids, 'max');
    if (upBestAsk != null) { cached.yesAsk = upBestAsk; cached.yesPrice = upBestAsk; }
    if (upBestBid != null)   cached.yesBid = upBestBid;
    if (upBestBid != null) { cached.noAsk = Math.round((1 - upBestBid) * 1000) / 1000; cached.noPrice = cached.noAsk; }
    if (upBestAsk != null)   cached.noBid = Math.round((1 - upBestAsk) * 1000) / 1000;
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
    const now   = Date.now();
    if (isYes) {
      const sortedAsks = [...book.asks.entries()].map(([p,s]) => ({ price: parseFloat(p), size: s })).filter(l => l.price > 0 && l.size > 0).sort((a,b) => a.price - b.price).slice(0, 5);
      const sortedBids = [...book.bids.entries()].map(([p,s]) => ({ price: parseFloat(p), size: s })).filter(l => l.price > 0 && l.size > 0).sort((a,b) => b.price - a.price).slice(0, 5);
      const lastAt = this._lastTickAt.get(conditionId) || 0;
      return { bestAsk: sortedAsks[0]?.price ?? null, bestAskSize: sortedAsks[0]?.size ?? 0, bestBid: sortedBids[0]?.price ?? null, bestBidSize: sortedBids[0]?.size ?? 0, asks: sortedAsks, bids: sortedBids, ageMs: lastAt > 0 ? now - lastAt : -1, wsAlive: this._wsConnected && (now - this._lastPongAt) < 20_000 };
    }
    const upAsks  = [...book.asks.entries()].map(([p,s]) => ({ price: parseFloat(p), size: s })).filter(l => l.price > 0 && l.size > 0).sort((a,b) => a.price - b.price);
    const upBids  = [...book.bids.entries()].map(([p,s]) => ({ price: parseFloat(p), size: s })).filter(l => l.price > 0 && l.size > 0).sort((a,b) => b.price - a.price);
    const downAsks = upBids.map(l => ({ price: Math.round((1 - l.price) * 1000) / 1000, size: l.size })).sort((a,b) => a.price - b.price).slice(0, 5);
    const downBids = upAsks.map(l => ({ price: Math.round((1 - l.price) * 1000) / 1000, size: l.size })).sort((a,b) => b.price - a.price).slice(0, 5);
    const lastAt   = this._lastTickAt.get(conditionId) || 0;
    return { bestAsk: downAsks[0]?.price ?? null, bestAskSize: downAsks[0]?.size ?? 0, bestBid: downBids[0]?.price ?? null, bestBidSize: downBids[0]?.size ?? 0, asks: downAsks, bids: downBids, ageMs: lastAt > 0 ? now - lastAt : -1, wsAlive: this._wsConnected && (now - this._lastPongAt) < 20_000 };
  }

  _sendSubscriptions(markets) {
    const marketIds = [];
    for (const m of markets) { if (m.marketId) marketIds.push(m.marketId); }
    if (marketIds.length === 0) return;
    console.log(`[PredictFun WS] subscribing to ${marketIds.length} market streams`);
    for (const id of marketIds) {
      this._wsSend({ method: 'subscribe', requestId: this._reqId++, params: [`predictOrderbook/${id}`] });
    }
  }

  unsubscribeMarkets(conditionIds) {
    for (const id of conditionIds) this._subscribedAssets.delete(id);
  }

  _wsSend(obj) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) this._ws.send(JSON.stringify(obj));
  }

  _wsCleanup() {
    this._wsConnected = false;
    if (this._ws) { this._ws.removeAllListeners(); this._ws = null; }
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer    = null;
      this._reconnectDelay    = Math.min(this._reconnectDelay * 2, this._maxReconnectDelay);
      this._connectWs();
    }, this._reconnectDelay);
  }

  /* ---------------------------------------------------------- */
  /*  Order placement — usa SDK oficial                         */
  /* ---------------------------------------------------------- */

  /**
   * Fluxo segundo doc oficial:
   * https://dev.predict.fun/how-to-create-or-cancel-orders-679306m0.md#how-to-create-a-market-order
   *
   * 1. getMarketOrderAmounts() — calcula makerAmount/takerAmount a partir do orderbook
   * 2. buildOrder("MARKET", { side, tokenId, makerAmount, takerAmount, ... })
   *    Com predictAccount configurado no make(), maker e signer são setados
   *    automaticamente para predictAccount.
   * 3. buildTypedData(order, { isNegRisk, isYieldBearing })
   * 4. signTypedDataOrder(typedData) — assina com a Privy EOA
   * 5. buildTypedDataHash(typedData)
   * 6. POST /v1/orders com { order: { ...signedOrder, hash }, pricePerShare, strategy, slippageBps }
   */
  async placeOrder(order) {
    if (!this.connected) {
      console.error('[PredictFun] cannot place order — not connected');
      return null;
    }
    await this._ensureOrderBuilder();
    if (!this._orderBuilder) {
      console.error('[PredictFun] cannot place order — OrderBuilder not initialized');
      this._lastOrderError = { type: 'other', message: 'OrderBuilder not initialized' };
      return null;
    }
    if (!this.predictAccount) {
      console.error('[PredictFun] cannot place order — predictAccountAddress not configured');
      this._lastOrderError = { type: 'other', message: 'predictAccountAddress not set' };
      return null;
    }

    this._lastOrderError = null;

    const tokenId = order.tokenId;
    const side    = order.side === 'BUY' ? Side.BUY : Side.SELL;  // SDK: Side.BUY=0, Side.SELL=1
    const price   = Math.round(parseFloat(order.price) * 1e4) / 1e4;
    const size    = parseFloat(order.size);

    const market = this._findMarketByTokenId(tokenId);
    if (!market) {
      console.error(`[PredictFun] cannot place order — market not found for tokenId ${String(tokenId).slice(0,16)}...`);
      this._lastOrderError = { type: 'other', message: 'market not found' };
      return null;
    }

    const feeRateBps = market.feeRateBps || 100;

    try {
      // Step 1: calcular amounts via SDK
      // Doc: getMarketOrderAmounts({ side, quantityWei, slippageBps }, book)
      // O book precisa estar no formato { asks: [[price, size], ...], bids: [...] }
      const rawBook = this._getRawBookForSdk(market, tokenId);

      const { makerAmount, takerAmount, pricePerShare, slippageBps } =
        this._orderBuilder.getMarketOrderAmounts(
          {
            side,
            quantityWei:    toWei(size),
            slippageBps:    50n,  // 0.5% slippage
            ...(side === Side.BUY ? { isMinAmountOut: true } : {}),
          },
          rawBook,
        );

      // Step 2: buildOrder — maker/signer setados automaticamente para predictAccount pelo SDK
      // Doc: "Create the order (maker and signer are automatically set to the predictAccount)"
      const builtOrder = this._orderBuilder.buildOrder('MARKET', {
        side,
        tokenId:     tokenId,
        makerAmount,
        takerAmount,
        nonce:       0n,
        feeRateBps,
      });

      // Step 3: buildTypedData
      const typedData = this._orderBuilder.buildTypedData(builtOrder, {
        isNegRisk:      market.isNegRisk      || false,
        isYieldBearing: market.isYieldBearing || false,
      });

      // Step 4: signTypedDataOrder — assina com a Privy EOA
      const signedOrder = await this._orderBuilder.signTypedDataOrder(typedData);

      // Step 5: hash
      const hash = this._orderBuilder.buildTypedDataHash(typedData);

      console.log(`[PredictFun] DEBUG AMOUNTS → size=${size} price=${price} feeRateBps=${feeRateBps} | makerAmount=${makerAmount} takerAmount=${takerAmount}`);

      // Step 6: POST /v1/orders
      await this._ensureJwt();
      // Doc: isMinAmountOut só se aplica a BUY orders com slippage
      // https://dev.predict.fun/how-to-create-or-cancel-orders-679306m0.md#how-to-apply-slippage
      const bodyData = {
        order:         { ...signedOrder, hash },
        pricePerShare: pricePerShare.toString(),
        strategy:      'MARKET',
      };
      if (slippageBps != null) bodyData.slippageBps = slippageBps.toString();
      if (side === Side.BUY)   bodyData.isMinAmountOut = true;
      const body = { data: bodyData };

      const t0   = Date.now();
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

      const orderId   = String(rdata.orderId);
      // Doc: GET /v1/orders/{hash} — o endpoint correto para consultar uma ordem específica
      // GET /v1/orders não aceita orderId como query param (só first/after/status)
      const orderHash = rdata.orderHash || hash;
      console.log(`[PredictFun] order placed orderId=${orderId} hash=${orderHash.slice(0,12)}... — polling for fill...`);

      const fillData = await this._pollFill(orderHash, 10000);
      if (fillData) {
        return {
          orderId,
          orderHash:    rdata.orderHash || hash,
          takingAmount: fillData.amountReceived,
          makingAmount: fillData.amountSpent,
          status:       fillData.status,
          fillPrice:    fillData.fillPrice,
          filledShares: fillData.filledShares,
        };
      }

      console.warn(`[PredictFun] fill poll timeout for orderId=${orderId}`);
      this._lastOrderError = { type: 'liquidity', message: 'fill timeout — order may be pending' };
      return null;
    } catch (err) {
      const status  = err.response?.status;
      const errBody = JSON.stringify(err.response?.data || err.message).slice(0, 300);
      console.error(`[PredictFun] POST /v1/orders FAILED (HTTP ${status}): ${errBody}`);
      this._lastOrderError = this._classifyOrderError(errBody, status);
      return null;
    }
  }

  /**
   * Converte o book interno (Map) para o formato esperado pelo SDK:
   * { asks: [[price, size], ...], bids: [[price, size], ...] }
   */
  _getRawBookForSdk(market, tokenId) {
    const conditionId = market.conditionId;
    const book        = this._books.get(conditionId);
    const isYes       = tokenId === market.yesTokenId;
    if (!book) return { asks: [], bids: [] };

    const toArr = (map) => [...map.entries()]
      .map(([p, s]) => [parseFloat(p), s])
      .filter(([p, s]) => p > 0 && s > 0);

    if (isYes) {
      return {
        asks: toArr(book.asks).sort((a,b) => a[0] - b[0]),
        bids: toArr(book.bids).sort((a,b) => b[0] - a[0]),
      };
    }
    // DOWN token: derivar do complemento do UP book
    const upAsks = toArr(book.asks).sort((a,b) => a[0] - b[0]);
    const upBids = toArr(book.bids).sort((a,b) => b[0] - a[0]);
    return {
      asks: upBids.map(([p,s]) => [Math.round((1 - p) * 1000) / 1000, s]).sort((a,b) => a[0] - b[0]),
      bids: upAsks.map(([p,s]) => [Math.round((1 - p) * 1000) / 1000, s]).sort((a,b) => b[0] - a[0]),
    };
  }

  // Doc: GET /v1/orders/{hash} — busca ordem específica pelo hash
  // GET /v1/orders não aceita orderId como param — só first/after/status
  async _pollFill(orderHash, maxMs) {
    const POLL_INTERVAL = 500;
    const deadline      = Date.now() + maxMs;
    await this._ensureJwt();

    while (Date.now() < deadline) {
      try {
        const resp = await axios.get(`${this.baseUrl}/v1/orders/${orderHash}`, {
          headers: this._headers(),
          timeout: 5_000,
        });
        const o = resp.data?.data || resp.data;
        if (o) {
          const status = o.status || '';
          if (status === 'FILLED' || status === 'PARTIALLY_FILLED') return this._parseFill(o);
          if (status === 'CANCELLED' || status === 'EXPIRED' || status === 'INVALIDATED') {
            console.warn(`[PredictFun] order ${orderHash.slice(0,12)}... status=${status}`);
            this._lastOrderError = { type: 'liquidity', message: `order ${status}` };
            return null;
          }
          // status OPEN — ainda não preenchida, continua polling
        }
      } catch (err) {
        // 404 = ordem ainda não indexada, continua tentando
        if (err.response?.status !== 404) {
          console.debug(`[PredictFun] _pollFill error: ${err.response?.status || err.message}`);
        }
      }
      await new Promise(r => setTimeout(r, POLL_INTERVAL));
    }
    return null;
  }

  _parseFill(order) {
    const raw    = order.order || {};
    const side   = raw.side === 0 || raw.side === 'BUY' ? 'BUY' : 'SELL';
    const amountFilled = BigInt(order.amountFilled || order.amount || 0);
    if (side === 'BUY') {
      const sharesReceived = Number(amountFilled) / Number(WEI);
      const usdtSpent      = Number(BigInt(raw.makerAmount || 0)) / Number(WEI);
      return { filledShares: sharesReceived, amountReceived: String(sharesReceived), amountSpent: String(usdtSpent), fillPrice: sharesReceived > 0 ? usdtSpent / sharesReceived : 0, status: order.status };
    }
    const sharesGiven  = Number(BigInt(raw.makerAmount || 0)) / Number(WEI);
    const usdtReceived = Number(amountFilled) / Number(WEI);
    return { filledShares: sharesGiven, amountReceived: String(usdtReceived), amountSpent: String(sharesGiven), fillPrice: sharesGiven > 0 ? usdtReceived / sharesGiven : 0, status: order.status };
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
  /*  Interface genérica — INALTERADA                           */
  /* ---------------------------------------------------------- */

  get floorPrice() { return 0.001; }
  setFeeRate(rate) { if (typeof rate === 'number' && rate >= 0) this.feeRate = rate; }
  getMarketKey(market) { return market.conditionId; }
  getSideToken(market, side) { return side === 'yes' || side === 'up' ? market.yesTokenId : market.noTokenId; }

  getBestAsk(market, side) {
    const snap = this.getLiveSnapshot(this.getSideToken(market, side));
    return snap?.bestAsk ?? null;
  }
  getBestBid(market, side) {
    const snap = this.getLiveSnapshot(this.getSideToken(market, side));
    return snap?.bestBid ?? null;
  }

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
    return this.getLiveSnapshot(this.getSideToken(market, side));
  }

  getDepthAtCeiling(market, side, ceilingPrice) {
    const snap = this.getLiveSnapshotForSide(market, side);
    if (!snap?.asks?.length) return 0;
    let total = 0;
    for (const a of snap.asks) { if (a.price <= ceilingPrice + 1e-9) total += a.size; }
    return total;
  }

  simulateBuyFill(market, side, qty, ceilingPrice) {
    const empty = { qtyFillable: 0, totalCost: 0, avgFillPrice: 0, depleted: true, levelsUsed: 0 };
    const snap  = this.getLiveSnapshotForSide(market, side);
    if (!snap?.asks?.length || !(qty > 0)) return empty;
    const sorted = [...snap.asks].sort((a,b) => a.price - b.price);
    let remaining = qty, totalCost = 0, qtyFillable = 0, levelsUsed = 0;
    for (const a of sorted) {
      if (a.price > ceilingPrice + 1e-9) break;
      const consume = Math.min(remaining, a.size);
      totalCost += consume * a.price; qtyFillable += consume; remaining -= consume; levelsUsed++;
      if (remaining <= 0) break;
    }
    return { qtyFillable, totalCost, avgFillPrice: qtyFillable > 0 ? totalCost / qtyFillable : 0, depleted: remaining > 0, levelsUsed };
  }

  simulateSellFill(market, side, qty) {
    const empty   = { qtyFillable: 0, netRevenue: 0, avgFillPrice: 0, depleted: true, levelsUsed: 0, haircutApplied: 0 };
    const snap    = this.getLiveSnapshotForSide(market, side);
    if (!snap?.bids?.length || !(qty > 0)) return empty;
    const feeRate = this._marketFeeRate(market);
    let remaining = qty, grossRevenue = 0, netRaw = 0, qtyFillable = 0, levelsUsed = 0;
    for (const b of snap.bids) {
      if (!(b.price > 0) || !(b.size > 0)) continue;
      const consume        = Math.min(remaining, b.size);
      const sellFeePerShare = feeRate > 0 ? feeRate * Math.min(b.price, 1 - b.price) : 0;
      grossRevenue += consume * b.price;
      netRaw       += consume * (b.price - sellFeePerShare);
      qtyFillable  += consume; remaining -= consume; levelsUsed++;
      if (remaining <= 0) break;
    }
    if (qtyFillable === 0) return empty;
    const haircutScale = config.execution?.predictfunHaircutScale ?? 0.60;
    const avgRawBid    = grossRevenue / qtyFillable;
    const haircut      = Math.min(0.30, (1 - avgRawBid) * haircutScale);
    const netRevenue   = netRaw * (1 - haircut);
    return { qtyFillable, netRevenue, avgFillPrice: qtyFillable > 0 ? netRevenue / qtyFillable : 0, depleted: remaining > 0, levelsUsed, haircutApplied: haircut };
  }

  async placeBuyOrder(market, side, price, qty)  { return this.placeOrder({ tokenId: this.getSideToken(market, side), price, size: qty, side: 'BUY'  }); }
  async placeSellOrder(market, side, price, qty) { return this.placeOrder({ tokenId: this.getSideToken(market, side), price, size: qty, side: 'SELL' }); }

  async getSideTokenBalance(market, side) {
    return this.getTokenBalance(this.getSideToken(market, side));
  }

  extractBuyFillQty(result, _intendedPrice) { return result ? parseFloat(result.filledShares || 0) : 0; }

  extractSellOutcome(result) {
    if (!result) return { soldQty: 0, takingAmount: 0, orderId: null };
    return { soldQty: parseFloat(result.filledShares || 0), takingAmount: parseFloat(result.takingAmount || 0), orderId: result.orderId || null };
  }

  extractFillPrice(orderResult) {
    if (!orderResult) return null;
    const fp = parseFloat(orderResult.fillPrice || 0);
    return fp > 0 ? fp : null;
  }

  async getHedgeBids(tokenId) {
    try {
      const snap = this.getLiveSnapshot(tokenId);
      if (snap?.bids?.length) return snap.bids.map(l => ({ price: l.price, size: l.size, source: 'ws' }));
    } catch (_) {}
    try {
      const book = await this.getOrderbook(tokenId);
      if (book?.bids?.length) {
        return book.bids.map(l => ({ price: parseFloat(l.price), size: parseFloat(l.size) }))
          .filter(l => l.price > 0 && l.size > 0).sort((a,b) => b.price - a.price).slice(0, 5)
          .map(l => ({ ...l, source: 'rest' }));
      }
    } catch (_) {}
    return [];
  }

  /* ---------------------------------------------------------- */
  /*  Cleanup                                                    */
  /* ---------------------------------------------------------- */

  destroy() {
    if (this._ws) { this._ws.close(); this._wsCleanup(); }
    if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
    if (this._bookRefreshTimer) { clearInterval(this._bookRefreshTimer); this._bookRefreshTimer = null; }
  }
}

module.exports = PredictFunConnector;
