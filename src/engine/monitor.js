/**
 * Global monitor — multi-platform, event-driven via WebSocket streams.
 *
 * Architecture:
 *   1. REST discovery: finds current 15-min markets on all enabled platforms
 *   2. WS subscription: streams real-time prices from every platform
 *   3. Each platform tick → updates per-asset cache → runs decision engine
 *      across the FULL set of platforms (every pair, every leg combo)
 *   4. Discovery timer: every 15s checks if a new 15-min window opened →
 *      re-discovers markets and resubscribes WS
 *
 * The constructor takes a `platforms` array — each item is `{name, connector}`.
 * Kalshi (when present) is discovered first so its tickers can seed predict.fun's
 * kalshiMarketTicker lookup. All other platforms run in parallel.
 */

const EventEmitter = require('events');
const { ASSETS } = require('../config/assets');
const userStore = require('../store/users');

const DISCOVERY_INTERVAL_MS = 15_000;
// Faster WS staleness detection — was 120s. Cut to 30s so a hung Polymarket
// stream gets reconnected within ~45s instead of ~3min. Health check itself
// stays cheap (single Date.now() compare per platform).
const STALE_THRESHOLD_SECS  = 30;
const WS_HEALTH_INTERVAL_MS = 15_000;

class Monitor extends EventEmitter {
  /**
   * @param {Array<{name, connector}>} platforms
   * @param {DecisionEngine} decisionEngine
   * @param {Dispatcher} dispatcher
   * @param {RtdsClient} rtdsClient
   */
  constructor(platforms, decisionEngine, dispatcher, rtdsClient) {
    super();
    this.platforms = Array.isArray(platforms) ? platforms.filter(p => p && p.connector) : [];
    this.decisionEngine = decisionEngine || null;
    this.dispatcher = dispatcher || null;
    this.rtdsClient = rtdsClient || null;
    this.running = false;
    this._discoveryTimer = null;
    this._currentWindowTs = 0;

    // Cached per-asset price snapshots:
    //   prices[symbol] = { [platformName]: [markets], updatedAt }
    this.prices = {};
    this.opportunities = [];

    // Round-keyed trade tracking (per asset)
    this._executedThisRound = new Map();
    this._lastFailTs = {};
    this._hedgeCount = {};
    this._hedgeCooldownUntil = {};
    this._tradeCountThisRound = {};
    this._lastSuccessTs = {};

    // Per-platform tick handler bindings + last-tick timestamp.
    this._tickHandlers = new Map();        // platformName → bound handler
    this._lastPlatformTickAt = new Map();  // platformName → ms

    // Lifecycle binding (Kalshi-only — fired when a new market activates).
    this._onKalshiMarketLifecycle = this._onKalshiMarketLifecycleHandler.bind(this);
    this._onRtdsStrike = this._onRtdsStrikeHandler.bind(this);
  }

  /* ---------------------------------------------------------- */
  /*  Helpers                                                    */
  /* ---------------------------------------------------------- */

  _getKalshi() { return this.platforms.find(p => p.name === 'kalshi')?.connector || null; }

  getEnabledAssets() {
    const users = userStore.listUsers();
    const filters = users.length > 0 ? userStore.getFilters(users[0]) : null;
    return Object.values(ASSETS).filter((a) => {
      if (filters && filters[a.symbol] != null) {
        const f = filters[a.symbol];
        return typeof f === 'object' ? f.enabled !== false : f !== false;
      }
      return a.enabled;
    });
  }

  _getUserFilters() {
    const users = userStore.listUsers();
    return users.length > 0 ? userStore.getFilters(users[0]) : null;
  }

  _getUserSettings() {
    const users = userStore.listUsers();
    return users.length > 0 ? userStore.getSettings(users[0]) : null;
  }

  getSnapshot() {
    return {
      prices: this.prices,
      opportunities: this.opportunities,
      lastPoll: this._lastUpdate || null,
      polling: this.running,
    };
  }

  _getCurrentWindowTs() {
    return Math.floor(Date.now() / 1000 / 900) * 900;
  }

  /**
   * True when the Kalshi cache looks fully settled (≥50% of markets pinned at
   * 0.95+ or 0.05−). Used to force a re-discovery shortly after rotation.
   */
  _checkIfKalshiSettled() {
    let settled = 0, total = 0;
    for (const sym of Object.keys(this.prices)) {
      const k = this.prices[sym]?.kalshi || [];
      for (const m of k) {
        total++;
        if (m.yesAsk >= 0.95 || m.yesAsk <= 0.05) settled++;
      }
    }
    return total > 0 && settled / total > 0.5;
  }

  /* ---------------------------------------------------------- */
  /*  Start / Stop                                               */
  /* ---------------------------------------------------------- */

  async start() {
    this.running = true;
    const assets = this.getEnabledAssets();
    console.log('[Monitor] starting — multi-platform mode');
    console.log('[Monitor] platforms:', this.platforms.map(p => p.name).join(', '));
    console.log('[Monitor] tracking:', assets.map(a => a.symbol).join(', '));

    await this._discoverMarkets();

    // Bind a generic tick handler per platform.
    const now = Date.now();
    for (const p of this.platforms) {
      const handler = (market) => this._onPlatformTick(p.name, market);
      p.connector.on('tick', handler);
      this._tickHandlers.set(p.name, handler);
      this._lastPlatformTickAt.set(p.name, now);
    }

    // Kalshi market_lifecycle_v2: subscribe to new tickers as they activate.
    const kalshi = this._getKalshi();
    if (kalshi) kalshi.on('marketLifecycle', this._onKalshiMarketLifecycle);

    if (this.rtdsClient) this.rtdsClient.on('strike', this._onRtdsStrike);

    this._discoveryTimer = setInterval(() => this._checkWindowRotation(), DISCOVERY_INTERVAL_MS);
    this._kalshiPriceTimer = setInterval(() => this._refreshKalshiPrices(), DISCOVERY_INTERVAL_MS);
    this._wsHealthTimer = setInterval(() => this._checkWsHealth(), WS_HEALTH_INTERVAL_MS);

    console.log('[Monitor] live — reacting to every WS tick across all platforms');
  }

  stop() {
    this.running = false;
    if (this._discoveryTimer) clearInterval(this._discoveryTimer);
    if (this._kalshiPriceTimer) clearInterval(this._kalshiPriceTimer);
    if (this._wsHealthTimer) clearInterval(this._wsHealthTimer);
    this._discoveryTimer = this._kalshiPriceTimer = this._wsHealthTimer = null;

    for (const p of this.platforms) {
      const h = this._tickHandlers.get(p.name);
      if (h) p.connector.removeListener('tick', h);
    }
    this._tickHandlers.clear();

    const kalshi = this._getKalshi();
    if (kalshi) kalshi.removeListener('marketLifecycle', this._onKalshiMarketLifecycle);
    if (this.rtdsClient) this.rtdsClient.removeListener('strike', this._onRtdsStrike);
    console.log('[Monitor] stopped');
  }

  _checkWsHealth() {
    if (!this.running) return;
    const now = Date.now();
    const lines = [];
    for (const p of this.platforms) {
      const last = this._lastPlatformTickAt.get(p.name) || 0;
      const ageS = Math.round((now - last) / 1000);
      const status = ageS <= STALE_THRESHOLD_SECS ? 'OK' : 'STALE';
      lines.push(`${p.name}=${status} (${ageS}s ago)`);

      if (ageS > STALE_THRESHOLD_SECS) {
        console.warn(`[Monitor] WS STALE: ${p.name} — no tick for ${ageS}s. Reconnecting...`);
        try {
          p.connector.destroy?.();
          // Resubscribe with whatever each connector tracks internally.
          if (p.name === 'kalshi') {
            const tickers = [...(p.connector._subscribedTickers || [])];
            if (tickers.length > 0) p.connector.subscribeWs(tickers).catch(() => {});
          } else {
            const subs = [...(p.connector._subscribedAssets?.values() || [])];
            if (subs.length > 0) p.connector.subscribeWs(subs);
          }
        } catch (err) {
          console.warn(`[Monitor] reconnect ${p.name}: ${err.message}`);
        }
      }
    }
    console.log(`[Monitor] WS health: ${lines.join(', ')}`);
  }

  /* ---------------------------------------------------------- */
  /*  Discovery + WS subscription                                */
  /* ---------------------------------------------------------- */

  async _discoverMarkets() {
    const assets = this.getEnabledAssets();
    const ts = new Date().toISOString();
    this._lastUpdate = ts;
    this._currentWindowTs = this._getCurrentWindowTs();

    this.prices = {};
    this.opportunities = [];

    const kalshi = this._getKalshi();

    for (const asset of assets) {
      try {
        // 1. Kalshi first (if present) so its tickers can seed predict.fun's
        //    kalshiMarketTicker lookup.
        let kalshiMarkets = [];
        if (kalshi) {
          kalshiMarkets = await kalshi.getMarkets(asset);
        }

        // 2. All other platforms in parallel.
        const otherPlatforms = this.platforms.filter(p => p.name !== 'kalshi');
        const otherResults = await Promise.all(
          otherPlatforms.map(p => p.connector.getMarkets(asset, {
            kalshiTickers: kalshiMarkets.map(m => m.ticker),
          }).catch(err => {
            console.error(`[Monitor] ${p.name}.getMarkets(${asset.symbol}) error:`, err.message);
            return [];
          })),
        );

        // 3. Build per-asset price snapshot.
        const assetPrices = { updatedAt: ts };
        if (kalshi) assetPrices.kalshi = kalshiMarkets;
        otherPlatforms.forEach((p, idx) => {
          assetPrices[p.name] = otherResults[idx];
        });
        this.prices[asset.symbol] = assetPrices;

        // 4. Inject RTDS strike into platforms that don't have their own.
        if (this.rtdsClient) {
          const rtdsStrike = this.rtdsClient.getStrike(asset.symbol, this._currentWindowTs);
          for (const p of otherPlatforms) {
            const list = assetPrices[p.name] || [];
            for (const m of list) {
              if (m.strike == null) m.strike = rtdsStrike;
            }
          }
          if (rtdsStrike != null) {
            const dec = rtdsStrike >= 1000 ? 2 : rtdsStrike >= 10 ? 4 : 5;
            console.log(`[Monitor] ${asset.symbol} RTDS strike: $${rtdsStrike.toFixed(dec)}`);
          }
        }

        // Stats log per platform
        const counts = this.platforms.map(p => `${p.name}: ${(assetPrices[p.name] || []).length}`);
        console.log(`[Monitor] ${asset.symbol} — ${counts.join(', ')}`);

        // Emit + initial scan.
        this.emit('prices', { asset: asset.symbol, ...assetPrices, timestamp: ts });
        if (this.decisionEngine) this._runScan(asset.symbol);
      } catch (err) {
        console.error(`[Monitor] discovery error for ${asset.symbol}:`, err.message);
      }
    }

    // Subscribe each platform to its tracked markets.
    for (const p of this.platforms) {
      const allMarketsForPlatform = [];
      for (const sym of Object.keys(this.prices)) {
        const list = this.prices[sym]?.[p.name] || [];
        allMarketsForPlatform.push(...list);
      }
      if (allMarketsForPlatform.length === 0) continue;
      try {
        if (p.name === 'kalshi') {
          // Kalshi subscribes by ticker
          const tickers = allMarketsForPlatform.map(m => m.ticker).filter(Boolean);
          if (tickers.length > 0) await p.connector.subscribeWs(tickers);
        } else {
          // Poly/Predict subscribe with full market objects
          p.connector.subscribeWs(allMarketsForPlatform);
        }
      } catch (err) {
        console.error(`[Monitor] subscribe ${p.name} error:`, err.message);
      }
    }

    const summary = this.platforms.map(p => {
      const total = Object.values(this.prices).reduce((sum, ap) => sum + (ap[p.name]?.length || 0), 0);
      return `${p.name}=${total}`;
    });
    console.log(`[Monitor] subscribed — ${summary.join(', ')}`);
  }

  async _checkWindowRotation() {
    if (!this.running) return;
    const currentWindow = this._getCurrentWindowTs();
    if (currentWindow === this._currentWindowTs) return;

    const __t0 = Date.now();
    console.log(`[Monitor] === NEW 15-MIN WINDOW === (${this._currentWindowTs} → ${currentWindow})`);
    setImmediate(() => {
      const ms = Date.now() - __t0;
      if (ms > 200) console.warn(`[Perf] window rotation total: ${ms}ms (>200ms)`);
    });

    this._executedThisRound.clear();
    this._lastFailTs = {};
    this._hedgeCount = {};
    this._hedgeCooldownUntil = {};
    this._tradeCountThisRound = {};
    this._lastSuccessTs = {};

    if (this.dispatcher && this.dispatcher._strikeGuardBlocked) {
      console.log('[StrikeGuard] round rotated — block lifted');
      this.dispatcher._strikeGuardBlocked = false;
    }

    if (this.dispatcher && this.dispatcher._openPositions?.length > 0) {
      const assets = [...new Set(this.dispatcher._openPositions.map(p => p.asset))];
      console.log(`[EarlyExit] settling ${this.dispatcher._openPositions.length} position(s) for ${assets.join(', ')}`);
      for (const asset of assets) this.dispatcher.clearPositionsForAsset(asset);
    }

    // Stop subscriptions on every platform.
    const kalshi = this._getKalshi();
    if (kalshi) {
      const tickers = [...kalshi.markets.keys()];
      if (tickers.length > 0) kalshi.unsubscribeTickers(tickers);
      kalshi.markets.clear();
    }
    for (const p of this.platforms) {
      if (p.name === 'kalshi') continue;
      const subs = [...(p.connector._subscribedAssets?.keys() || [])];
      if (subs.length > 0 && p.connector.unsubscribeMarkets) p.connector.unsubscribeMarkets(subs);
      if (p.connector.markets?.clear) p.connector.markets.clear();
      // Force a fresh WS connection. Polymarket's CLOB WS does not reliably
      // send a book snapshot for newly-subscribed tokens on a long-lived
      // socket across round rotations — the new round's tokens sit silent
      // for 30s+. Closing the socket here means the rediscover's
      // subscribeWs() opens a fresh connection, and the server always sends
      // a snapshot on the open handler. Predict.fun's WS reconnects cleanly
      // through the same path.
      if (p.connector.destroy) {
        try { p.connector.destroy(); } catch (_) {}
      }
    }
    this.prices = {};
    this.opportunities = [];
    this._currentWindowTs = currentWindow;
    this.emit('prices', { cleared: true, timestamp: new Date().toISOString() });

    console.log('[Monitor] step 2: RTDS capturing strikes (automatic)...');
    console.log('[Monitor] step 3: waiting 5s then discovering markets...');
    await new Promise(r => setTimeout(r, 5000));
    await this._discoverMarkets();

    // Retry if Kalshi has no markets yet (post-rotation delay)
    const retryDelays = [8_000, 12_000];
    for (const delay of retryDelays) {
      const hasNoKalshi = Object.values(this.prices).some(p => !p.kalshi || p.kalshi.length === 0);
      const settled = this._checkIfKalshiSettled();
      if (!hasNoKalshi && !settled) break;
      console.log(`[Monitor] Kalshi ${hasNoKalshi ? '0 markets' : 'settled'} — retry in ${delay/1000}s`);
      await new Promise(r => setTimeout(r, delay));
      if (kalshi) kalshi.markets.clear();
      await this._discoverMarkets();
    }
    console.log('[Monitor] round ready — scanning active');
  }

  /**
   * Periodic Kalshi price refresh via REST. Catches the case where the new
   * 15-min window opens but Kalshi hasn't yet pushed initial prices via WS.
   */
  async _refreshKalshiPrices() {
    if (!this.running) return;
    const kalshi = this._getKalshi();
    if (!kalshi) return;

    const assets = this.getEnabledAssets();
    const ts = new Date().toISOString();
    for (const asset of assets) {
      try {
        const cached = this.prices[asset.symbol];
        if (!cached) continue;
        const list = cached.kalshi || [];
        const hasNoMarkets = list.length === 0;
        const hasEmpty = list.some(m => m.yesAsk === 0);
        if (!hasNoMarkets && !hasEmpty) continue;

        const fresh = await kalshi.getMarkets(asset);
        if (!fresh.length) continue;
        const hasNewPrices = fresh.some(m => m.yesAsk > 0);
        if (!hasNewPrices && !hasNoMarkets) continue;

        cached.kalshi = fresh;
        cached.updatedAt = ts;
        this._lastUpdate = ts;

        const tickers = fresh.map(m => m.ticker);
        await kalshi.subscribeWs(tickers);

        if (this.rtdsClient) {
          const strike = this.rtdsClient.getStrike(asset.symbol, this._currentWindowTs);
          if (strike != null) {
            for (const p of this.platforms) {
              if (p.name === 'kalshi') continue;
              const arr = cached[p.name] || [];
              for (const m of arr) if (m.strike == null) m.strike = strike;
            }
          }
        }

        console.log(`[Monitor] ${asset.symbol} Kalshi ${hasNoMarkets ? 'discovered' : 'refreshed'} via REST (${fresh.length})`);
        this.emit('prices', { asset: asset.symbol, ...cached, timestamp: ts });
        if (this.decisionEngine) this._runScan(asset.symbol);
      } catch (_) {}
    }
  }

  /* ---------------------------------------------------------- */
  /*  Tick handlers                                              */
  /* ---------------------------------------------------------- */

  _onKalshiMarketLifecycleHandler({ ticker }) {
    if (!this.running) return;
    const kalshi = this._getKalshi();
    if (!kalshi) return;
    const assets = this.getEnabledAssets();
    const asset = assets.find(a => a.kalshi?.seriesTicker && ticker.startsWith(a.kalshi.seriesTicker));
    if (!asset) return;
    if (kalshi._subscribedTickers.has(ticker)) return;

    console.log(`[Monitor] new ${asset.symbol} market via lifecycle: ${ticker}`);
    kalshi.subscribeWs([ticker]).catch(err => console.warn(`[Monitor] lifecycle subscribe: ${err.message}`));

    kalshi.getMarkets(asset).then(markets => {
      if (!markets.length) return;
      if (!this.prices[asset.symbol]) this.prices[asset.symbol] = { updatedAt: new Date().toISOString() };
      this.prices[asset.symbol].kalshi = markets;
      this.prices[asset.symbol].updatedAt = new Date().toISOString();
      this._runScan(asset.symbol);
    }).catch(() => {});
  }

  _onPlatformTick(platformName, market) {
    if (!this.running || !market?.asset) return;
    this._lastPlatformTickAt.set(platformName, Date.now());

    const symbol = market.asset;
    const ts = new Date().toISOString();
    this._lastUpdate = ts;

    if (!this.prices[symbol]) this.prices[symbol] = { updatedAt: ts };
    const cached = this.prices[symbol];
    if (!Array.isArray(cached[platformName])) cached[platformName] = [];

    // Match by platform-specific identity:
    //   Kalshi: ticker, others: conditionId
    const list = cached[platformName];
    const idx = platformName === 'kalshi'
      ? list.findIndex(m => m.ticker === market.ticker)
      : list.findIndex(m => m.conditionId === market.conditionId);
    if (idx >= 0) list[idx] = market;
    else list.push(market);
    cached.updatedAt = ts;

    this.emit('prices', { asset: symbol, ...cached, timestamp: ts });

    if (this.decisionEngine) this._runScan(symbol);
  }

  _onRtdsStrikeHandler({ symbol, price, roundOpenTs }) {
    if (!this.running) return;
    if (roundOpenTs !== this._currentWindowTs) return;
    const cached = this.prices[symbol];
    if (!cached) return;

    let updated = false;
    for (const p of this.platforms) {
      if (p.name === 'kalshi') continue; // Kalshi has its own floor_strike
      const arr = cached[p.name];
      if (!Array.isArray(arr)) continue;
      for (const m of arr) {
        if (m.strike == null) {
          m.strike = price;
          updated = true;
        }
      }
    }

    if (updated) {
      const dec = price >= 1000 ? 2 : price >= 10 ? 4 : 5;
      console.log(`[Monitor] ${symbol} strike updated from RTDS: $${price.toFixed(dec)}`);
      this.emit('prices', { asset: symbol, ...cached, timestamp: new Date().toISOString() });
      this._runScan(symbol);
    }
  }

  /* ---------------------------------------------------------- */
  /*  Scan + dispatch                                            */
  /* ---------------------------------------------------------- */

  _runScan(symbol) {
    const cached = this.prices[symbol];
    if (!cached) return;

    // Build platforms[] for the decisionEngine.
    const platformsForScan = this.platforms.map(p => ({
      name: p.name,
      connector: p.connector,
      markets: cached[p.name] || [],
    })).filter(p => p.markets.length > 0);

    if (platformsForScan.length < 2) return; // need at least 2 to arb

    const filters = this._getUserFilters();
    const f = filters?.[symbol];
    const overrides = {};
    if (f && typeof f === 'object') {
      if (f.minRoiPct != null) overrides.minRoiPct = f.minRoiPct;
      if (f.maxStrikeDiff != null) overrides.maxStrikeDiff = f.maxStrikeDiff;
      if (f.entrySize != null) overrides.entrySize = f.entrySize;
      if (f.earlyExitPct != null) overrides.earlyExitPct = f.earlyExitPct;
    }

    const opps = this.decisionEngine.scan(symbol, platformsForScan, overrides);

    let bestOpp = null;
    for (const o of opps) {
      if (!bestOpp || o.roiPct > bestOpp.roiPct) bestOpp = o;
    }
    const filtered = bestOpp ? [bestOpp] : [];
    // Strip connector refs before broadcast — connector objects contain WebSocket/EventEmitter
    // internals that cause JSON.stringify to exceed the V8 call stack limit.
    // The dispatcher uses `bestOpp` directly (still has connectors); this.opportunities is UI-only.
    const broadcastOpps = filtered.map(o => {
      if (!o) return o;
      const legA = o.legA ? { ...o.legA } : o.legA;
      const legB = o.legB ? { ...o.legB } : o.legB;
      if (legA) delete legA.connector;
      if (legB) delete legB.connector;
      return { ...o, legA, legB };
    });
    this.opportunities = [...this.opportunities.filter(o => o.asset !== symbol), ...broadcastOpps];
    this.emit('opportunities', this.opportunities);

    if (!this.dispatcher || !bestOpp || !bestOpp.execute) return;

    const settings = this._getUserSettings() || {};
    const ENTRY_CUTOFF_SECS = settings.entryCutoffSecs ?? 180;
    const RETRY_COOLDOWN_MS = (settings.retryCooldownSecs ?? 5) * 1000;
    const REENTRY_COOLDOWN_MS = (settings.reentryCooldownSecs ?? 60) * 1000;
    const HEDGE_COOLDOWN_MS = (settings.hedgeCooldownSecs ?? 30) * 1000;
    const MAX_HEDGES_PER_ROUND = settings.maxHedgesPerRound ?? 5;

    const roundKey = `${symbol}-${this._currentWindowTs}`;
    if (this._executedThisRound.get(roundKey) === 'success') return;
    if (this._executedThisRound.get(roundKey) === 'pending') return;

    if (ENTRY_CUTOFF_SECS > 0) {
      const nowSec = Math.floor(Date.now() / 1000);
      const roundEndSec = this._currentWindowTs + 900;
      if (nowSec >= roundEndSec - ENTRY_CUTOFF_SECS) {
        if (!this._lastCutoffLog || Date.now() - this._lastCutoffLog >= 30_000) {
          this._lastCutoffLog = Date.now();
          console.log(`[Monitor] ${symbol}: ENTRY BLOCKED — ${roundEndSec - nowSec}s left (cutoff=${ENTRY_CUTOFF_SECS}s)`);
        }
        return;
      }
    }

    if (RETRY_COOLDOWN_MS > 0 && this._lastFailTs[symbol]
      && (Date.now() - this._lastFailTs[symbol]) < RETRY_COOLDOWN_MS) return;
    if (this._hedgeCooldownUntil[symbol] && Date.now() < this._hedgeCooldownUntil[symbol]) return;
    if (REENTRY_COOLDOWN_MS > 0 && this._lastSuccessTs[symbol]
      && (Date.now() - this._lastSuccessTs[symbol]) < REENTRY_COOLDOWN_MS) return;

    this._executedThisRound.set(roundKey, 'pending');
    this.dispatcher.execute(bestOpp).then(trade => {
      if (trade && trade.success) {
        this._executedThisRound.set(roundKey, 'success');
        this._tradeCountThisRound[roundKey] = (this._tradeCountThisRound[roundKey] || 0) + 1;
        console.log(`[Monitor] ${symbol}: trade SUCCESS #${this._tradeCountThisRound[roundKey]} — locked`);
      } else if (trade && trade.partial) {
        this._hedgeCount[symbol] = (this._hedgeCount[symbol] || 0) + 1;
        if (this._hedgeCount[symbol] >= MAX_HEDGES_PER_ROUND) {
          this._executedThisRound.set(roundKey, 'success');
          console.error(`[Monitor] ${symbol}: PARTIAL #${this._hedgeCount[symbol]} — LOCKED (max=${MAX_HEDGES_PER_ROUND})`);
        } else {
          this._executedThisRound.delete(roundKey);
          this._hedgeCooldownUntil[symbol] = Date.now() + HEDGE_COOLDOWN_MS;
          console.warn(`[Monitor] ${symbol}: PARTIAL #${this._hedgeCount[symbol]}/${MAX_HEDGES_PER_ROUND} — retry in ${HEDGE_COOLDOWN_MS/1000}s`);
        }
      } else {
        this._executedThisRound.delete(roundKey);
        this._lastFailTs[symbol] = Date.now();
        const reason = trade?.reason || 'failed';
        console.log(`[Monitor] ${symbol}: ${reason}`);
      }
      if (trade) this.emit('trade', trade);
    }).catch(err => {
      this._executedThisRound.delete(roundKey);
      this._lastFailTs[symbol] = Date.now();
      console.error('[Monitor] dispatch error:', err.message);
    });
  }

  /**
   * Called from the dispatcher after a successful early exit — releases the
   * lock so the asset can re-enter (subject to REENTRY_COOLDOWN_MS).
   */
  unlockAssetForRound(symbol) {
    const settings = this._getUserSettings() || {};
    const MAX_TRADES_PER_ROUND = settings.maxTradesPerRound ?? 2;
    const roundKey = `${symbol}-${this._currentWindowTs}`;
    const tradeCount = this._tradeCountThisRound[roundKey] || 0;
    if (MAX_TRADES_PER_ROUND > 0 && tradeCount >= MAX_TRADES_PER_ROUND) {
      console.log(`[Monitor] ${symbol}: max trades/round reached (${tradeCount}/${MAX_TRADES_PER_ROUND})`);
      return;
    }
    if (this._executedThisRound.has(roundKey)) {
      this._executedThisRound.delete(roundKey);
      this._lastSuccessTs[symbol] = Date.now();
      console.log(`[Monitor] ${symbol}: UNLOCKED after early exit`);
    }
  }
}

module.exports = Monitor;
