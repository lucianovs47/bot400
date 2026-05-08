/**
 * Parallel dispatcher — multi-platform execution.
 *
 * Generic over arbitrary platform pairs. The signal carries the full leg
 * description (`{platform, connector, market, side, price, fee}`); the
 * dispatcher just calls `connector.placeBuyOrder(...)` / `placeSellOrder(...)`
 * without knowing which platform is which.
 *
 * Strategy ("True Parallel"):
 *   - Pre-validate the arb against live top-of-book reads (WS fast-path
 *     when possible, REST fallback otherwise)
 *   - Fire BOTH legs simultaneously via Promise.allSettled
 *   - Handle the four outcome quadrants: OK/OK, OK/FAIL, FAIL/OK, FAIL/FAIL
 *   - Emergency-hedge any leg that fills alone
 *   - Track matched contracts for early-exit monitoring
 *
 * Order types: each connector exposes `placeBuyOrder(market, side, price, qty)`.
 * The price acts as a ceiling — Kalshi IOC matches up to that limit, Poly FAK
 * sweeps up to it, Predict MARKET sweeps within slippage. The dispatcher just
 * sets a sensible ceiling based on ROI headroom.
 */

const config = require('../config');
const userStore = require('../store/users');

const WS_LIVECHECK_MAX_AGE_MS = 5_000;
const ceilToCent  = (x) => Math.ceil( Math.round(x * 1e10) / 1e10 * 100) / 100;
const roundToCent = (x) => Math.round(Math.round(x * 1e10) / 1e10 * 100) / 100;

class Dispatcher {
  /**
   * @param {CapitalGuard} capitalGuard
   * @param {Array<{name, connector}>} platforms — used for tick listening (early exit).
   */
  constructor(capitalGuard, platforms = []) {
    this.capitalGuard = capitalGuard;
    this.platforms = Array.isArray(platforms) ? platforms.filter(p => p?.connector) : [];
    this.tradeMode = config.tradeMode;
    this.trades = [];
    this.maxTrades = 500;
    this._executingAssets = new Map();
    this.hedgeLog = [];
    this._openPositions = [];
    this._earlyExitListening = false;
    this._tickHandler = null;
    this._checkingExits = false;
    this._strikeGuardBlocked = false;
    this.monitor = null;

    this.useWsLivecheck = true;
    this.maxWsAgeForLive = 300;
  }

  setPlatforms(platforms) {
    const wasListening = this._earlyExitListening;
    if (wasListening) this.stopEarlyExitMonitor();
    this.platforms = Array.isArray(platforms) ? platforms.filter(p => p?.connector) : [];
    if (wasListening) this.startEarlyExitMonitor();
  }

  /* ============================================================ */
  /*  Public state accessors                                       */
  /* ============================================================ */

  getTrades() { return this.trades; }
  getHedgeLog() { return this.hedgeLog; }
  getOpenPositions() { return this._openPositions; }
  getState() {
    return {
      tradeMode: this.tradeMode,
      executingAssets: [...this._executingAssets.keys()],
      openPositions: this._openPositions.length,
      totalTrades: this.trades.length,
      successfulTrades: this.trades.filter(t => t.success).length,
      partialTrades: this.trades.filter(t => t.partial).length,
      hedgedTrades: this.trades.filter(t => t.hedged).length,
      blockedTrades: this.trades.filter(t => t.blocked).length,
      dryRunTrades: this.trades.filter(t => t.dryRun).length,
    };
  }

  /* ============================================================ */
  /*  Helpers                                                      */
  /* ============================================================ */

  _calcWorstFee(legA, priceA, legB, priceB) {
    const feeA = legA.connector.calcEntryFee(legA.market, priceA);
    const feeB = legB.connector.calcEntryFee(legB.market, priceB);
    return ceilToCent(feeA + feeB);
  }

  /**
   * Hedge cap — for CTF platforms, after BUY at price P each contract may
   * require $1.00 collateral to SELL back. So balance >= qty × (price + 1).
   * Kalshi: balance / price (entry cost only).
   * Returns the maximum hedgeable qty, or null if balance unknown.
   */
  _hedgeCap(leg, price) {
    if (!this.capitalGuard) return null;
    const bal = this.capitalGuard.getBalance(leg.platform);
    if (bal == null || bal <= 0) return null;
    if (leg.platform === 'kalshi') return Math.floor(bal / Math.max(0.01, price));
    return Math.floor(bal / (price + 1.0));
  }

  /* ============================================================ */
  /*  Live book fetch                                              */
  /* ============================================================ */

  async _fetchSnapshotForLeg(leg) {
    if (this.useWsLivecheck) {
      const snap = leg.connector.getLiveSnapshotForSide(leg.market, leg.side);
      const ok = snap
        && (snap.bestAsk != null)
        && (snap.wsAlive !== false)
        && (snap.ageMs < WS_LIVECHECK_MAX_AGE_MS);
      if (ok) return { snap, source: 'ws', ageMs: snap.ageMs, wsAlive: true };
      const reasons = [];
      if (!snap) reasons.push('no-snap');
      else {
        if (snap.bestAsk == null) reasons.push('no-ask');
        if (snap.wsAlive === false) reasons.push('ws-dead');
        if (snap.ageMs >= WS_LIVECHECK_MAX_AGE_MS) reasons.push(`stale(${snap.ageMs}ms)`);
      }
      console.log(`[Dispatcher] ${leg.platform} WS miss (${reasons.join(',')}) — REST fallback`);
    }

    try {
      const tokenOrTicker = leg.connector.getSideToken(leg.market, leg.side);
      const book = await leg.connector.getOrderbook(tokenOrTicker);
      if (!book) return { snap: null, source: 'rest-fail' };
      let asks, bids;
      if (leg.platform === 'kalshi') {
        const sameSide = leg.side === 'yes' ? book.yes : book.no;
        const oppSide  = leg.side === 'yes' ? book.no  : book.yes;
        bids = (sameSide || []).map(l => ({ price: l.price, size: l.qty }))
          .filter(b => b.price > 0 && b.size > 0).sort((a, b) => b.price - a.price);
        asks = (oppSide || []).map(l => ({ price: Math.round((1 - l.price) * 100) / 100, size: l.qty }))
          .filter(a => a.price > 0 && a.price < 1 && a.size > 0).sort((a, b) => a.price - b.price);
      } else {
        asks = (book.asks || []).map(l => ({ price: parseFloat(l.price || l[0]), size: parseFloat(l.size || l[1]) }))
          .filter(a => a.price > 0 && a.size > 0).sort((a, b) => a.price - b.price);
        bids = (book.bids || []).map(l => ({ price: parseFloat(l.price || l[0]), size: parseFloat(l.size || l[1]) }))
          .filter(b => b.price > 0 && b.size > 0).sort((a, b) => b.price - a.price);
      }
      const bestAsk = asks[0]?.price ?? null;
      const bestBid = bids[0]?.price ?? null;
      return {
        snap: { asks, bids, bestAsk, bestBid, ageMs: 0, wsAlive: false },
        source: 'rest', ageMs: 0, wsAlive: false,
      };
    } catch (err) {
      console.error(`[Dispatcher] ${leg.platform} book FETCH FAILED: ${err.message}`);
      return { snap: null, source: 'rest-error' };
    }
  }

  async _fetchLiveBooks(signal) {
    const [a, b] = await Promise.all([
      this._fetchSnapshotForLeg(signal.legA),
      this._fetchSnapshotForLeg(signal.legB),
    ]);
    return { a, b };
  }

  /* ============================================================ */
  /*  Pre-flight live ROI re-validation                            */
  /* ============================================================ */

  _validateLive({ signal, priceA, priceB, maxUnits }) {
    if (!(priceA > 0) || !(priceB > 0)) return { ok: false, reason: 'invalid prices' };
    const combined = priceA + priceB;
    if (combined >= 1.0) return { ok: false, reason: `no arb (combined=$${combined.toFixed(4)})`, units: 0 };

    const feeA = signal.legA.connector.calcEntryFee(signal.legA.market, priceA);
    const feeB = signal.legB.connector.calcEntryFee(signal.legB.market, priceB);
    const worstFee = feeA + feeB;
    const grossSpread = 1.0 - combined;
    const netProfit = grossSpread - worstFee;
    if (netProfit <= 0) return { ok: false, reason: `fees kill (gross=${grossSpread.toFixed(4)} fees=${worstFee.toFixed(4)})`, units: 0 };
    const roiPct = (netProfit / combined) * 100;
    const minRoi = signal.minRoiPct ?? 0;
    if (roiPct < minRoi) return { ok: false, reason: `ROI=${roiPct.toFixed(2)}% < min=${minRoi}%`, units: 0 };

    let units = maxUnits;
    const entrySize = signal.entrySize ?? null;
    if (entrySize != null && entrySize > 0) {
      const cap = Math.floor(entrySize / combined);
      if (cap < units) units = cap;
    }
    if (units < 1) return { ok: false, reason: `entrySize cap (${entrySize}/${combined.toFixed(4)})`, units: 0 };
    return { ok: true, units, roiPct, netProfit, totalCostPerUnit: combined };
  }

  /* ============================================================ */
  /*  Main execute()                                               */
  /* ============================================================ */

  async execute(signal) {
    const asset = signal.asset;
    if (this._executingAssets.get(asset)) return { success: false, reason: `busy:${asset}` };
    this._executingAssets.set(asset, true);

    try {
      const isDryRun = this.tradeMode !== 'live';

      if (this._strikeGuardBlocked && !isDryRun) {
        return this._recordTrade(signal, { success: false, reason: 'strike guard active', blocked: true });
      }
      if (this.capitalGuard && !isDryRun) {
        const allowed = this.capitalGuard.canTrade();
        if (!allowed.ok) return this._recordTrade(signal, { success: false, reason: allowed.reason, blocked: true });
      }

      const units = signal.sizing?.units || 0;

      if (isDryRun) {
        return this._recordTrade(signal, { success: true, dryRun: true, legAResult: { simulated: true }, legBResult: { simulated: true } });
      }
      if (units <= 0) return this._recordTrade(signal, { success: false, reason: 'no units' });

      // Tokens / readiness checks per leg
      for (const leg of [signal.legA, signal.legB]) {
        const tok = leg.connector.getSideToken(leg.market, leg.side);
        if (!tok) return this._recordTrade(signal, { success: false, blocked: true, reason: `${leg.platform} ${leg.side} token missing` });
        if (typeof leg.connector.isReady === 'function' && !leg.connector.isReady()) {
          if (leg.platform !== 'kalshi') {
            return this._recordTrade(signal, { success: false, blocked: true, reason: `${leg.platform} not ready` });
          }
        }
      }

      // 1. Fetch live books
      const { a: bookA, b: bookB } = await this._fetchLiveBooks(signal);
      if (!bookA.snap || !bookB.snap) {
        const failed = [];
        if (!bookA.snap) failed.push(signal.legA.platform);
        if (!bookB.snap) failed.push(signal.legB.platform);
        return this._recordTrade(signal, { success: false, reason: `book fetch failed: ${failed.join(',')}` });
      }
      const priceA = bookA.snap.bestAsk;
      const priceB = bookB.snap.bestAsk;
      if (priceA == null || priceB == null) {
        return this._recordTrade(signal, { success: false, reason: 'no live ask' });
      }

      // 2. Depth at +10¢ ceiling
      const depthA = signal.legA.connector.getDepthAtCeiling(signal.legA.market, signal.legA.side, priceA + 0.10);
      const depthB = signal.legB.connector.getDepthAtCeiling(signal.legB.market, signal.legB.side, priceB + 0.10);

      // 3. Dynamic entry size (per-platform depth buffer).
      // Factors come from user settings (depthFactors); fallback to config.execution
      // values, then to baked defaults. Kalshi uses MIN_BUF=3 (fast IOC), others MIN_BUF=5.
      const userSettings = (() => {
        try {
          const users = userStore.listUsers();
          return users.length > 0 ? userStore.getSettings(users[0]) : null;
        } catch (_) { return null; }
      })();
      const userDF = userSettings?.depthFactors || {};
      const _bufFor = (platform, qty) => {
        const minBuf = platform === 'kalshi' ? 3 : 5;
        let factor;
        if (platform === 'kalshi') {
          factor = userDF.kalshi ?? 1.0;
        } else if (platform === 'predictfun') {
          factor = userDF.predictfun ?? config.execution?.predictfunDepthFactor ?? 1.5;
        } else {
          factor = userDF.polymarket ?? config.execution?.polyDepthFactor ?? 1.5;
        }
        return Math.max(minBuf, Math.ceil(qty * factor));
      };
      let adjustedUnits = units;
      const MIN_CONTRACTS = 3;
      const bufA = _bufFor(signal.legA.platform, adjustedUnits);
      const bufB = _bufFor(signal.legB.platform, adjustedUnits);
      const maxByDepth = Math.min(Math.floor(depthA) - bufA, Math.floor(depthB) - bufB);

      if (maxByDepth < MIN_CONTRACTS) {
        const rawA = Math.floor(depthA);
        const rawB = Math.floor(depthB);
        if (rawA >= MIN_CONTRACTS && rawB >= MIN_CONTRACTS) {
          console.log(`[Dispatcher] smart entry: buffered=${maxByDepth}<${MIN_CONTRACTS}, raw A=${rawA} B=${rawB} — using min`);
          adjustedUnits = MIN_CONTRACTS;
        } else {
          return this._recordTrade(signal, {
            success: false, skipped: true,
            reason: `depth: min(A=${depthA.toFixed(1)}-${bufA},B=${depthB.toFixed(1)}-${bufB})=${maxByDepth} < ${MIN_CONTRACTS}`,
          });
        }
      } else if (maxByDepth < adjustedUnits) {
        console.log(`[Dispatcher] dynamic entry: ${adjustedUnits}→${maxByDepth} (A=${depthA.toFixed(1)}-${bufA}, B=${depthB.toFixed(1)}-${bufB})`);
        adjustedUnits = maxByDepth;
      }

      // 4. Validate live ROI + entrySize cap
      const valid = this._validateLive({ signal, priceA, priceB, maxUnits: adjustedUnits });
      if (!valid.ok) {
        console.warn(`[Dispatcher] ABORT pre-fire — ${valid.reason} | A@${priceA} B@${priceB}`);
        return this._recordTrade(signal, { success: false, reason: valid.reason });
      }
      adjustedUnits = valid.units;

      // 5. Hedge cap per leg
      for (const leg of [signal.legA, signal.legB]) {
        const cap = this._hedgeCap(leg, leg === signal.legA ? priceA : priceB);
        if (cap != null && cap < adjustedUnits) {
          if (cap < 1) {
            return this._recordTrade(signal, { success: false, reason: `${leg.platform} balance too low for hedge` });
          }
          console.log(`[Dispatcher] capped ${adjustedUnits}→${cap} (${leg.platform} hedge safety)`);
          adjustedUnits = cap;
        }
      }

      // 6. ROI headroom → FAK ceiling per leg
      const minRoiPct = signal.minRoiPct ?? 3;
      const ask0Fee = this._calcWorstFee(signal.legA, priceA, signal.legB, priceB);
      const ask0Combined = priceA + priceB;
      const ask0Roi = ask0Combined > 0 ? ((1 - ask0Combined - ask0Fee) / ask0Combined) * 100 : 0;
      const headroomPct = Math.max(0, ask0Roi - minRoiPct);
      const headroomCents = Math.floor(headroomPct / 1.1);

      const ceilingA = signal.legA.platform === 'kalshi'
        ? Math.round(priceA * 100) / 100
        : Math.round((priceA + (headroomCents + 1) * 0.01) * 100) / 100;
      const ceilingB = signal.legB.platform === 'kalshi'
        ? Math.round(priceB * 100) / 100
        : Math.round((priceB + (headroomCents + 1) * 0.01) * 100) / 100;

      // 6.5 Min order notional check — Polymarket and Predict.fun reject orders below ~$1.
      // Without this, the bot fills the cheap leg (Kalshi), the expensive leg (Poly/Predict)
      // gets rejected for "min size: $1", and we hedge at a loss.
      for (const [leg, ceiling] of [[signal.legA, ceilingA], [signal.legB, ceilingB]]) {
        const minNotional = leg.connector.minOrderNotional || 0;
        if (minNotional > 0) {
          const notional = adjustedUnits * ceiling;
          if (notional < minNotional) {
            return this._recordTrade(signal, {
              success: false,
              reason: `${leg.platform} notional $${notional.toFixed(2)} < min $${minNotional.toFixed(2)} (qty=${adjustedUnits} × ceil=$${ceiling.toFixed(3)})`,
            });
          }
        }
      }

      // 7. Book walk per leg → effective fill price
      const walkA = signal.legA.connector.simulateBuyFill(signal.legA.market, signal.legA.side, adjustedUnits, ceilingA);
      const walkB = signal.legB.connector.simulateBuyFill(signal.legB.market, signal.legB.side, adjustedUnits, ceilingB);
      const avgFillA = walkA.qtyFillable === adjustedUnits ? walkA.avgFillPrice : ceilingA;
      const avgFillB = walkB.qtyFillable === adjustedUnits ? walkB.avgFillPrice : ceilingB;

      const effFee = this._calcWorstFee(signal.legA, avgFillA, signal.legB, avgFillB);
      const effCombined = avgFillA + avgFillB;
      const effNet = 1.0 - effCombined - effFee;
      const effRoi = effCombined > 0 ? (effNet / effCombined) * 100 : 0;
      if (effNet <= 0) {
        return this._recordTrade(signal, { success: false, reason: `net negative after walk (roi=${effRoi.toFixed(1)}%)` });
      }

      console.log(
        `[Dispatcher] ${signal.asset} ${signal.leg}: ` +
        `A=${signal.legA.platform}.${signal.legA.side}@${avgFillA.toFixed(3)}(ceil=${ceilingA}) + ` +
        `B=${signal.legB.platform}.${signal.legB.side}@${avgFillB.toFixed(3)}(ceil=${ceilingB}) ` +
        `roi=${effRoi.toFixed(1)}% headroom=${headroomPct.toFixed(1)}% qty=${adjustedUnits}`,
      );

      // 8. Fire both legs in parallel
      const t0 = Date.now();
      const [resA, resB] = await Promise.allSettled([
        signal.legA.connector.placeBuyOrder(signal.legA.market, signal.legA.side, ceilingA, adjustedUnits),
        signal.legB.connector.placeBuyOrder(signal.legB.market, signal.legB.side, ceilingB, adjustedUnits),
      ]);
      const totalMs = Date.now() - t0;

      const resultA = resA.status === 'fulfilled' ? resA.value : null;
      const resultB = resB.status === 'fulfilled' ? resB.value : null;
      if (resA.status === 'rejected') console.error(`[Dispatcher] ${signal.legA.platform} ex: ${resA.reason?.message}`);
      if (resB.status === 'rejected') console.error(`[Dispatcher] ${signal.legB.platform} ex: ${resB.reason?.message}`);

      const filledA = signal.legA.connector.extractBuyFillQty(resultA, ceilingA);
      const filledB = signal.legB.connector.extractBuyFillQty(resultB, ceilingB);

      console.log(
        `[Dispatcher] PARALLEL settled in ${totalMs}ms — ` +
        `A: ${filledA > 0 ? `OK ${filledA}x@$${ceilingA}` : 'FAIL'}, ` +
        `B: ${filledB > 0 ? `OK ${filledB}x@$${ceilingB}` : 'FAIL'}`,
      );

      const matchedQty = Math.min(filledA, filledB);
      let hedgeResult = null;

      if (filledA === 0 && filledB === 0) {
        return this._recordTrade(signal, { success: false, partial: false, legAResult: 'failed', legBResult: 'failed', totalMs });
      }
      if (filledA > 0 && filledB === 0) {
        console.warn(`[Dispatcher] A filled ${filledA}x, B 0 — hedging A`);
        hedgeResult = await this._emergencyHedge(signal.legA, filledA);
        return this._recordTrade(signal, {
          success: false, partial: true, hedged: !!hedgeResult, hedgeResult,
          legAResult: resultA, legBResult: 'failed', filledA, filledB: 0,
          ceilingA, ceilingB, fillA: avgFillA, fillB: 0, totalMs,
        });
      }
      if (filledB > 0 && filledA === 0) {
        console.warn(`[Dispatcher] B filled ${filledB}x, A 0 — hedging B`);
        hedgeResult = await this._emergencyHedge(signal.legB, filledB);
        return this._recordTrade(signal, {
          success: false, partial: true, hedged: !!hedgeResult, hedgeResult,
          legAResult: 'failed', legBResult: resultB, filledA: 0, filledB,
          ceilingA, ceilingB, fillA: 0, fillB: avgFillB, totalMs,
        });
      }

      // Both filled — handle qty mismatch
      if (filledA > filledB) {
        const excess = filledA - filledB;
        console.warn(`[Dispatcher] MISMATCH A=${filledA} B=${filledB} — hedging ${excess} on A`);
        hedgeResult = await this._emergencyHedge(signal.legA, excess);
      } else if (filledB > filledA) {
        const excess = filledB - filledA;
        console.warn(`[Dispatcher] MISMATCH A=${filledA} B=${filledB} — hedging ${excess} on B`);
        hedgeResult = await this._emergencyHedge(signal.legB, excess);
      }

      const success = matchedQty > 0 && filledA === filledB;
      const result = this._recordTrade(signal, {
        success, partial: filledA !== filledB, hedged: !!hedgeResult, hedgeResult,
        legAResult: resultA, legBResult: resultB, filledA, filledB,
        ceilingA, ceilingB, fillA: avgFillA, fillB: avgFillB, totalMs,
      });

      if (matchedQty > 0) {
        const takingA = parseFloat(resultA?.takingAmount || 0);
        const takingB = parseFloat(resultB?.takingAmount || 0);
        this._openPositions.push({
          id: result.id,
          asset: signal.asset,
          leg: signal.leg,
          qty: matchedQty,
          legA: this._buildLegState(signal.legA, avgFillA, ceilingA, takingA, Math.min(filledA, matchedQty)),
          legB: this._buildLegState(signal.legB, avgFillB, ceilingB, takingB, Math.min(filledB, matchedQty)),
          entryCost: (avgFillA + avgFillB) * matchedQty,
          entryRoiPct: effRoi > 0 ? effRoi : (signal.roiPct ?? 3),
          earlyExitPct: signal.earlyExitPct ?? null,
          takeProfitTarget: Math.max(0.05, (1.0 - (avgFillA + avgFillB)) * matchedQty * 2.0),
          entryTime: Date.now(),
          readyAt: Date.now() + 15_000,
          state: 'open',
          _exiting: false,
        });
        console.log(`[EarlyExit] tracking ${signal.asset} ${signal.leg} x${matchedQty} entryCost=$${(avgFillA+avgFillB).toFixed(3)} minExitROI=${(effRoi||signal.roiPct).toFixed(2)}%`);
      }

      return result;
    } finally {
      this._executingAssets.delete(asset);
    }
  }

  _buildLegState(legSignal, entryPrice, ceilingPrice, takingAmount, qtyMatched) {
    return {
      platform: legSignal.platform,
      connector: legSignal.connector,
      market: legSignal.market,
      side: legSignal.side,
      entryPrice,
      ceilingPrice,
      entryFee: legSignal.connector.calcEntryFee(legSignal.market, entryPrice),
      takingAmount,
      tokenBalance: null,
      balanceRefreshAt: null,
      balanceRefreshing: false,
      closed: false,
      exitRevenue: 0,
      qtyMatched,
    };
  }

  /* ============================================================ */
  /*  Emergency hedge                                              */
  /* ============================================================ */

  async _emergencyHedge(leg, qty) {
    const hedge = {
      timestamp: new Date().toISOString(),
      platform: leg.platform,
      side: leg.side,
      qty,
      attempts: [],
      estimatedSellPrice: 0,
      takingAmount: 0,
    };
    const MAX_ROUNDS = 8;
    const SETTLE_BASE_MS = 2000;
    const SETTLE_MAX_MS = 15000;
    const FLOOR = leg.connector.floorPrice ?? 0.001;

    let hedgedQty = 0;
    let dustExit = false;

    try {
      let round = 0;
      while (hedgedQty < qty && round < MAX_ROUNDS) {
        round++;
        const remaining = qty - hedgedQty;

        if (round > 1) {
          const waitMs = Math.min(SETTLE_BASE_MS + (round - 2) * 1000, SETTLE_MAX_MS);
          console.log(`[HEDGE] waiting ${waitMs/1000}s for settlement (round ${round})...`);
          await new Promise(r => setTimeout(r, waitMs));
        }

        let sellSize = remaining;
        const lastErr = leg.connector._lastOrderError;
        const clobTokens = lastErr?.type === 'balance' && lastErr.polyBalance != null ? lastErr.polyBalance : null;
        if (clobTokens != null) {
          const exact = Math.floor(clobTokens * 100) / 100;
          if (exact < 0.01 && hedgedQty > 0) {
            // partially hedged, near-zero remainder = fee dust — done
            console.warn(`[HEDGE] remaining balance ${exact} ≈ 0 — fee dust. hedged=${hedgedQty}/${qty}`);
            dustExit = true; break;
          }
          if (exact >= 0.01) {
            sellSize = Math.min(remaining, exact);
          }
          // exact=0 and hedgedQty=0: CLOB settlement lag right after BUY.
          // Fall through with sellSize=remaining — the wait above will let it settle.
          if (exact < 0.01 && hedgedQty === 0) {
            console.log(`[HEDGE] CLOB balance=0 after BUY (settlement lag) — retrying with ${remaining}x`);
          }
        }
        if (sellSize <= 0) {
          console.log('[HEDGE] sell cap = 0 — waiting...');
          continue;
        }

        try {
          const snap = leg.connector.getLiveSnapshotForSide(leg.market, leg.side);
          if (snap?.bestBid != null) {
            console.log(`[HEDGE] ${leg.platform} top: $${snap.bestBid.toFixed(3)}`);
            if (round === 1) hedge.estimatedSellPrice = snap.bestBid;
          }
        } catch (_) {}

        console.log(`[HEDGE] ${leg.platform} SELL: ${sellSize}x @$${FLOOR} (hedged: ${hedgedQty}/${qty})`);
        const result = await leg.connector.placeSellOrder(leg.market, leg.side, FLOOR, sellSize);
        const ok = result !== null;
        hedge.attempts.push({ platform: leg.platform, action: `SELL-${sellSize}x@${FLOOR}`, success: ok, result });

        if (ok) {
          const outcome = leg.connector.extractSellOutcome(result);
          const filled = outcome.soldQty > 0 ? outcome.soldQty : sellSize;
          hedgedQty += filled;
          if (outcome.takingAmount > 0) {
            hedge.takingAmount += outcome.takingAmount;
            console.log(`[HEDGE] takingAmount=$${outcome.takingAmount.toFixed(4)} (cum: $${hedge.takingAmount.toFixed(4)})`);
          }
          console.log(`[HEDGE] SELL success: ${filled}x (total: ${hedgedQty}/${qty})`);
        } else {
          const err = leg.connector._lastOrderError;
          if (err?.type === 'market_closed') {
            console.warn('[HEDGE] market closed — stopping retry');
            break;
          }
          console.log(`[HEDGE] SELL failed (${err?.type || 'unknown'})`);
        }
      }

      if (hedgedQty >= qty) console.log(`[HEDGE] SUCCESS — closed all ${hedgedQty}`);
      else if (dustExit) console.warn(`[HEDGE] DUST EXIT — closed ${hedgedQty}/${qty}`);
      else if (hedgedQty > 0) console.warn(`[HEDGE] PARTIAL — ${hedgedQty}/${qty}`);
      else console.error('[HEDGE] CRITICAL — 0 closed');
    } catch (err) {
      console.error('[HEDGE] error:', err.message);
      hedge.error = err.message;
    }

    this.hedgeLog.push(hedge);
    if (this.hedgeLog.length > 100) this.hedgeLog = this.hedgeLog.slice(-100);
    hedge.hedgedQty = hedgedQty;
    return hedge;
  }

  /* ============================================================ */
  /*  Trade record + PnL                                           */
  /* ============================================================ */

  _recordTrade(signal, result) {
    const filledA = result.filledA || 0;
    const filledB = result.filledB || 0;
    const matched = filledA > 0 && filledB > 0 ? Math.min(filledA, filledB)
      : (filledA > 0 || filledB > 0) ? Math.max(filledA, filledB) : 0;

    const trade = {
      id: `trade-${Date.now()}`,
      signalId: signal.id,
      asset: signal.asset,
      leg: signal.leg,
      timestamp: new Date().toISOString(),
      legA: { platform: signal.legA.platform, side: signal.legA.side, price: signal.legA.price, fee: signal.legA.fee },
      legB: { platform: signal.legB.platform, side: signal.legB.side, price: signal.legB.price, fee: signal.legB.fee },
      units: matched > 0 ? matched : (signal.sizing?.units || 0),
      totalCost: signal.totalCost,
      expectedRoi: signal.roiPct,
      ...result,
    };

    let pnl = 0;
    if (!result.dryRun && !result.blocked) {
      const matchedQty = Math.min(filledA, filledB);
      const fillA = result.fillA || signal.legA.price;
      const fillB = result.fillB || signal.legB.price;
      const feeA = signal.legA.connector.calcEntryFee(signal.legA.market, fillA);
      const feeB = signal.legB.connector.calcEntryFee(signal.legB.market, fillB);
      const worstFee = matchedQty > 0 ? feeA + feeB : 0;

      if (matchedQty > 0 && !result.hedged) {
        pnl += (1.0 - fillA - fillB - worstFee) * matchedQty;
      }
      if (result.hedged && filledA > filledB) {
        const excess = filledA - filledB;
        const sellEst = result.hedgeResult?.estimatedSellPrice || 0;
        const hedgeRevenue = result.hedgeResult?.takingAmount || (sellEst * excess);
        const buyCost = (signal.legA.connector.calcEntryFee(signal.legA.market, fillA) + fillA) * excess;
        pnl -= (buyCost - hedgeRevenue);
      }
      if (result.hedged && filledB > filledA) {
        const excess = filledB - filledA;
        const sellEst = result.hedgeResult?.estimatedSellPrice || 0;
        const hedgeRevenue = result.hedgeResult?.takingAmount || (sellEst * excess);
        const buyCost = (signal.legB.connector.calcEntryFee(signal.legB.market, fillB) + fillB) * excess;
        pnl -= (buyCost - hedgeRevenue);
      }
    }
    pnl = Math.round(pnl * 100) / 100;
    trade.pnl = pnl;

    if (this.capitalGuard && !result.dryRun && !result.blocked) {
      const realized = result.success ? 0 : pnl;
      this.capitalGuard.recordTrade({ pnl: realized, countTrade: true, source: result.success ? 'entry-fill' : 'hedge-loss' });
    }

    if (!result.skipped) {
      this.trades.push(trade);
      if (this.trades.length > this.maxTrades) this.trades = this.trades.slice(-this.maxTrades);
    }
    if (this.capitalGuard && !result.dryRun && !result.blocked) {
      this.capitalGuard.refreshBalances().catch(() => {});
    }

    const status = result.dryRun ? 'DRY-RUN' :
      result.blocked ? 'BLOCKED' :
      result.hedged ? 'HEDGED' :
      result.success ? 'FILLED' :
      result.partial ? 'PARTIAL' : 'FAILED';
    const aTag = filledA > 0 ? `${signal.legA.platform[0].toUpperCase()}:$${(result.fillA || signal.legA.price).toFixed(2)}/${filledA}`
      : (result.legAResult === 'failed' ? `${signal.legA.platform[0].toUpperCase()}:FAIL` : `${signal.legA.platform[0].toUpperCase()}:--`);
    const bTag = filledB > 0 ? `${signal.legB.platform[0].toUpperCase()}:$${(result.fillB || signal.legB.price).toFixed(2)}/${filledB}`
      : (result.legBResult === 'failed' ? `${signal.legB.platform[0].toUpperCase()}:FAIL` : `${signal.legB.platform[0].toUpperCase()}:--`);
    const ms = result.totalMs ? ` ${result.totalMs}ms` : '';
    const cumPnl = this.capitalGuard ? this.capitalGuard.getState().totalPnl : 0;
    const reasonTag = result.reason ? ` reason="${result.reason}"` : '';
    console.log(`[TRADE] ${status} ${signal.asset} ${signal.leg} x${trade.units} | ${aTag} ${bTag} |${ms} pnl=$${pnl.toFixed(2)} cumPnl=$${cumPnl.toFixed(2)}${reasonTag}`);
    return trade;
  }

  /* ============================================================ */
  /*  Early exit monitor (event-driven via WS ticks)               */
  /* ============================================================ */

  startEarlyExitMonitor() {
    if (this._earlyExitListening) return;
    this._tickHandler = () => {
      if (this._openPositions.length === 0) return;
      this._checkEarlyExits().catch(err => {
        console.error('[EarlyExit] tick error:', err.message);
        this._checkingExits = false;
      });
    };
    for (const p of this.platforms) p.connector.on('tick', this._tickHandler);
    this._earlyExitListening = true;
    console.log('[EarlyExit] monitor started — event-driven');
  }
  stopEarlyExitMonitor() {
    if (this._tickHandler) {
      for (const p of this.platforms) p.connector.removeListener('tick', this._tickHandler);
    }
    this._tickHandler = null;
    this._earlyExitListening = false;
  }

  async _checkEarlyExits() {
    if (this._openPositions.length === 0) return;
    if (this.tradeMode !== 'live') return;
    if (this._checkingExits) return;
    this._checkingExits = true;

    try {
      for (const pos of this._openPositions) {
        if (pos._exiting) continue;
        if (pos.readyAt && Date.now() < pos.readyAt) continue;

        if (pos.legA.closed && !pos.legB.closed) {
          pos._exiting = true; pos.state = 'exiting';
          this._emergencyHedge(pos.legB, pos.qty).then(hedge => this._finalizePartialExit(pos, hedge, 'A-closed-B-hedged'))
            .catch(err => { console.error('[EarlyExit] hedge error:', err.message); this._openPositions = this._openPositions.filter(p => p.id !== pos.id); });
          continue;
        }
        if (pos.legB.closed && !pos.legA.closed) {
          pos._exiting = true; pos.state = 'exiting';
          this._emergencyHedge(pos.legA, pos.qty).then(hedge => this._finalizePartialExit(pos, hedge, 'B-closed-A-hedged'))
            .catch(err => { console.error('[EarlyExit] hedge error:', err.message); this._openPositions = this._openPositions.filter(p => p.id !== pos.id); });
          continue;
        }

        await this._refreshLegBalance(pos.legA, pos);
        await this._refreshLegBalance(pos.legB, pos);

        const snapA = pos.legA.connector.getLiveSnapshotForSide(pos.legA.market, pos.legA.side);
        const snapB = pos.legB.connector.getLiveSnapshotForSide(pos.legB.market, pos.legB.side);
        const bidA = snapA?.bestBid || 0;
        const bidB = snapB?.bestBid || 0;
        if (bidA <= 0 && bidB <= 0) continue;

        if (bidA < 0.02 || bidB < 0.02) {
          if (!pos._lastLowBidLog || Date.now() - pos._lastLowBidLog >= 30_000) {
            pos._lastLowBidLog = Date.now();
            console.log(`[EarlyExit] SKIP ${pos.asset} ${pos.leg}: bidA=${bidA.toFixed(3)} bidB=${bidB.toFixed(3)} — deferring to settlement`);
          }
          continue;
        }

        const qtyA = pos.legA.tokenBalance ?? pos.qty;
        const qtyB = pos.legB.tokenBalance ?? pos.qty;
        const fillA = pos.legA.connector.simulateSellFill(pos.legA.market, pos.legA.side, qtyA);
        const fillB = pos.legB.connector.simulateSellFill(pos.legB.market, pos.legB.side, qtyB);

        const netCloseValue = fillA.netRevenue + fillB.netRevenue;
        const totalCost = (pos.legA.entryPrice + pos.legA.entryFee + pos.legB.entryPrice + pos.legB.entryFee) * pos.qty;
        const netProfit = netCloseValue - totalCost;
        if (pos.earlyExitPct == null) continue;

        const guaranteedProfit = Math.max(0, pos.qty * 1.0 - totalCost);
        const minExitProfit = Math.max(
          guaranteedProfit * (pos.earlyExitPct / 100),
          guaranteedProfit * 0.50,
          0.05,
        );

        const now = Date.now();
        if (!pos._lastStatusLog || now - pos._lastStatusLog >= 30_000) {
          pos._lastStatusLog = now;
          console.log(
            `[EarlyExit] STATUS ${pos.asset} ${pos.leg}: ` +
            `A:bid=${bidA.toFixed(3)} avg=${fillA.avgFillPrice.toFixed(3)} (${fillA.qtyFillable}/${qtyA}) ` +
            `B:bid=${bidB.toFixed(3)} avg=${fillB.avgFillPrice.toFixed(3)} (${fillB.qtyFillable}/${qtyB}) | ` +
            `net=$${netCloseValue.toFixed(2)} cost=$${totalCost.toFixed(2)} ` +
            `netProfit=$${netProfit.toFixed(3)} need=$${minExitProfit.toFixed(3)}`,
          );
        }

        const takeProfitReached = pos.takeProfitTarget > 0 && netProfit >= pos.takeProfitTarget;
        if (netProfit < minExitProfit && !takeProfitReached) continue;

        pos._exiting = true; pos.state = 'exiting';
        console.log(
          `[EarlyExit] OPPORTUNITY ${pos.asset} ${pos.leg}: ` +
          `A:net=$${fillA.netRevenue.toFixed(2)} + B:net=$${fillB.netRevenue.toFixed(2)} = $${netCloseValue.toFixed(2)} ` +
          `(cost=$${totalCost.toFixed(2)}) netProfit=$${netProfit.toFixed(3)}`,
        );
        this._executeEarlyExit(pos).catch(err => {
          console.error('[EarlyExit] error:', err.message);
          pos._exiting = false; pos.state = 'open';
        });
      }
    } finally {
      this._checkingExits = false;
    }
  }

  async _refreshLegBalance(legState, pos) {
    if (legState.closed) return;
    if (legState.connector.platform === 'kalshi') return;
    const now = Date.now();
    if (legState.tokenBalance == null) {
      try {
        const bal = await legState.connector.getSideTokenBalance(legState.market, legState.side);
        if (bal != null && bal > 0) {
          legState.tokenBalance = Math.floor(bal * 100) / 100;
          legState.balanceRefreshAt = now + 60_000;
          console.log(`[EarlyExit] ${pos.asset} ${legState.platform} balance settled: ${legState.tokenBalance}`);
        } else if (now - pos.entryTime > 30_000) {
          const raw = legState.takingAmount || pos.qty;
          legState.tokenBalance = Math.floor(raw * 100) / 100;
          legState.balanceRefreshAt = now + 60_000;
          console.warn(`[EarlyExit] ${pos.asset} ${legState.platform} balance API empty after 30s — using estimate ${legState.tokenBalance}`);
        }
      } catch (_) {}
    } else if (legState.balanceRefreshAt && now >= legState.balanceRefreshAt && !legState.balanceRefreshing) {
      legState.balanceRefreshing = true;
      legState.connector.getSideTokenBalance(legState.market, legState.side).then(bal => {
        if (bal != null && bal > 0) legState.tokenBalance = Math.floor(bal * 100) / 100;
      }).catch(() => {}).finally(() => {
        legState.balanceRefreshAt = Date.now() + 60_000;
        legState.balanceRefreshing = false;
      });
    }
  }

  async _executeEarlyExit(pos) {
    const qtyA = pos.legA.connector.platform === 'kalshi' ? pos.qty : Math.floor((pos.legA.tokenBalance || 0) * 10) / 10;
    const qtyB = pos.legB.connector.platform === 'kalshi' ? pos.qty : Math.floor((pos.legB.tokenBalance || 0) * 10) / 10;

    const snapA = pos.legA.connector.getLiveSnapshotForSide(pos.legA.market, pos.legA.side);
    const snapB = pos.legB.connector.getLiveSnapshotForSide(pos.legB.market, pos.legB.side);
    const bidA = snapA?.bestBid || 0;
    const bidB = snapB?.bestBid || 0;
    const skipA = bidA <= 0 || qtyA <= 0;
    const skipB = bidB <= 0 || qtyB <= 0;

    const floorA = pos.legA.connector.floorPrice ?? 0.001;
    const floorB = pos.legB.connector.floorPrice ?? 0.001;

    console.log(
      `[EarlyExit] EXEC ${pos.asset} ${pos.leg} — ` +
      (skipA ? 'A:SKIP' : `A:SELL ${qtyA}x@${floorA}`) + ' ' +
      (skipB ? 'B:SKIP' : `B:SELL ${qtyB}x@${floorB}`),
    );
    const t0 = Date.now();
    const promises = [];
    if (!skipA) promises.push(pos.legA.connector.placeSellOrder(pos.legA.market, pos.legA.side, floorA, qtyA));
    if (!skipB) promises.push(pos.legB.connector.placeSellOrder(pos.legB.market, pos.legB.side, floorB, qtyB));

    const settled = await Promise.allSettled(promises);
    const totalMs = Date.now() - t0;

    let pIdx = 0;
    let outcomeA = { soldQty: 0, takingAmount: 0, orderId: null };
    let outcomeB = { soldQty: 0, takingAmount: 0, orderId: null };
    if (!skipA) {
      const r = settled[pIdx]?.status === 'fulfilled' ? settled[pIdx].value : null;
      pIdx++;
      outcomeA = pos.legA.connector.extractSellOutcome(r);
    }
    if (!skipB) {
      const r = settled[pIdx]?.status === 'fulfilled' ? settled[pIdx].value : null;
      pIdx++;
      outcomeB = pos.legB.connector.extractSellOutcome(r);
    }

    const _legEstRevenue = (leg, outcome, bid) => {
      if (outcome.takingAmount > 0) return outcome.takingAmount;
      if (outcome.soldQty > 0) {
        const fee = leg.connector.calcExitFee(leg.market, bid) * outcome.soldQty;
        return bid * outcome.soldQty - fee;
      }
      return 0;
    };
    const estA = _legEstRevenue(pos.legA, outcomeA, bidA);
    const estB = _legEstRevenue(pos.legB, outcomeB, bidB);
    const totalRevenue = estA + estB;
    const totalCost = (pos.legA.entryPrice + pos.legA.entryFee + pos.legB.entryPrice + pos.legB.entryFee) * pos.qty;
    const pnl = Math.round((totalRevenue - totalCost) * 100) / 100;

    const isSuccess = (skipA ? true : outcomeA.soldQty > 0) && (skipB ? true : outcomeB.soldQty > 0);
    const fillBStr = (outcomeB.takingAmount > 0 && outcomeB.soldQty > 0) ? (outcomeB.takingAmount / outcomeB.soldQty).toFixed(3) : bidB.toFixed(3);
    const fillAStr = (outcomeA.takingAmount > 0 && outcomeA.soldQty > 0) ? (outcomeA.takingAmount / outcomeA.soldQty).toFixed(3) : bidA.toFixed(3);
    console.log(
      `[EarlyExit] ${isSuccess ? 'SUCCESS' : 'PARTIAL'} ${pos.asset} ${pos.leg} ` +
      `| A:SOLD ${outcomeA.soldQty}@~${fillAStr} B:SOLD ${outcomeB.soldQty}@${fillBStr} | ${totalMs}ms pnl=~$${pnl.toFixed(2)}`,
    );

    if (isSuccess) {
      if (this.capitalGuard && pnl !== 0) {
        this.capitalGuard.recordTrade({ pnl, source: 'early-exit-est' });
        this.capitalGuard.refreshBalances().catch(() => {});
      }
      this._openPositions = this._openPositions.filter(p => p.id !== pos.id);

      const tr = this.trades.find(t => t.id === pos.id);
      if (tr) {
        tr.pnl = pnl; tr.pnlEst = true; tr.settled = true; tr.earlyExited = true;
        if (this.monitor) this.monitor.emit('trade', tr);
      }
      if (this.monitor) this.monitor.unlockAssetForRound(pos.asset);

      this._correctEarlyExitPnl(pos, outcomeA, outcomeB, pnl).catch(err =>
        console.debug('[EarlyExit] PnL correction error:', err.message));

      return { soldA: outcomeA.soldQty, soldB: outcomeB.soldQty, pnl, totalMs };
    }

    if (outcomeA.soldQty > 0 && outcomeB.soldQty === 0 && !skipB) {
      pos.legA.exitRevenue = estA;
      pos.legA.closed = true;
      pos.totalEntryCost = totalCost;
      pos._exiting = false; pos.state = 'open';
    } else if (outcomeB.soldQty > 0 && outcomeA.soldQty === 0 && !skipA) {
      pos.legB.exitRevenue = estB;
      pos.legB.closed = true;
      pos.totalEntryCost = totalCost;
      pos._exiting = false; pos.state = 'open';
    } else {
      pos._exiting = false; pos.state = 'open';
    }
    return { soldA: outcomeA.soldQty, soldB: outcomeB.soldQty, pnl, totalMs };
  }

  _finalizePartialExit(pos, hedge, label) {
    this._openPositions = this._openPositions.filter(p => p.id !== pos.id);
    const hedgeRevenue = hedge?.takingAmount || ((hedge?.estimatedSellPrice || 0) * (hedge?.hedgedQty || pos.qty));
    const closedRevenue = (pos.legA.closed ? pos.legA.exitRevenue : 0) + (pos.legB.closed ? pos.legB.exitRevenue : 0);
    const finalPnl = Math.round((hedgeRevenue + closedRevenue - (pos.totalEntryCost || 0)) * 100) / 100;
    console.log(`[EarlyExit] ${label}: hedge=$${hedgeRevenue.toFixed(3)} closed=$${closedRevenue.toFixed(3)} cost=$${(pos.totalEntryCost||0).toFixed(3)} pnl=$${finalPnl.toFixed(2)}`);
    if (this.capitalGuard) {
      this.capitalGuard.recordTrade({ pnl: finalPnl, source: label });
      this.capitalGuard.refreshBalances().catch(() => {});
    }
    const tr = this.trades.find(t => t.id === pos.id);
    if (tr) {
      tr.pnl = finalPnl; tr.settled = true; tr.earlyExited = true;
      if (this.monitor) this.monitor.emit('trade', tr);
    }
  }

  /**
   * Async fill-price correction. Kalshi doesn't return avg fill price in the
   * order response; fetch /portfolio/fills to compute it and adjust PnL.
   */
  async _correctEarlyExitPnl(pos, outcomeA, outcomeB, pnlEst) {
    const corrections = [];
    for (const [leg, outcome] of [[pos.legA, outcomeA], [pos.legB, outcomeB]]) {
      if (!outcome.orderId) continue;
      if (leg.connector.platform !== 'kalshi') continue;
      try {
        const fills = await leg.connector.getOrderFills(outcome.orderId);
        if (!fills?.length) continue;
        let totalRev = 0, totalFilled = 0;
        for (const f of fills) {
          let raw;
          if (leg.side === 'no') {
            raw = f.no_price_dollars != null ? parseFloat(f.no_price_dollars)
              : f.yes_price_dollars != null ? 1 - parseFloat(f.yes_price_dollars)
              : (f.price ?? 0);
          } else {
            raw = parseFloat(f.yes_price_dollars ?? f.no_price_dollars ?? f.price ?? 0);
          }
          const cnt = parseFloat(f.count_fp ?? f.count ?? 0);
          totalRev += raw * cnt;
          totalFilled += cnt;
        }
        if (totalFilled === 0) continue;
        const avgFill = totalRev / totalFilled;
        const fee = leg.connector.calcExitFee(leg.market, avgFill);
        corrections.push({ leg: leg.platform, side: leg.side, soldQty: outcome.soldQty, avgFill, fee });
      } catch (_) {}
    }
    if (!corrections.length) return;

    let realRev = 0;
    realRev += corrections.reduce((s, c) => s + (c.avgFill - c.fee) * c.soldQty, 0);
    if (outcomeA.takingAmount > 0 && !corrections.find(c => c.leg === pos.legA.platform)) realRev += outcomeA.takingAmount;
    if (outcomeB.takingAmount > 0 && !corrections.find(c => c.leg === pos.legB.platform)) realRev += outcomeB.takingAmount;

    const totalCost = (pos.legA.entryPrice + pos.legA.entryFee + pos.legB.entryPrice + pos.legB.entryFee) * pos.qty;
    const pnlReal = Math.round((realRev - totalCost) * 100) / 100;
    const delta = Math.round((pnlReal - pnlEst) * 100) / 100;
    if (delta === 0) return;

    console.log(`[EarlyExit] PnL correction ${pos.asset} ${pos.leg}: $${pnlEst.toFixed(2)}→$${pnlReal.toFixed(2)} delta=$${delta.toFixed(2)}`);
    if (this.capitalGuard) this.capitalGuard.recordTrade({ pnl: delta, source: 'early-exit-correction' });
    const tr = this.trades.find(t => t.id === pos.id);
    if (tr) {
      tr.pnl = pnlReal; tr.pnlEst = false;
      if (this.monitor) this.monitor.emit('trade', tr);
    }
  }

  /* ============================================================ */
  /*  Round rotation cleanup + settlement confirmation             */
  /* ============================================================ */

  clearPositionsForAsset(asset) {
    const toRemove = this._openPositions.filter(p => p.asset === asset);
    for (const pos of toRemove) {
      if (pos._exiting) {
        console.log(`[Settlement] ${pos.asset} ${pos.leg}: skipping — exit in progress`);
        continue;
      }
      pos.state = 'pendingSettlement';
      this._confirmSettlement(pos);
    }
    const before = this._openPositions.length;
    this._openPositions = this._openPositions.filter(p => p.asset !== asset);
    if (before !== this._openPositions.length) {
      console.log(`[EarlyExit] cleared ${before - this._openPositions.length} expired position(s) for ${asset}`);
    }
  }

  async _confirmSettlement(pos) {
    const POLL_MS = 3000;
    const MAX = 10;
    const totalCost = (pos.legA.entryPrice + pos.legA.entryFee + pos.legB.entryPrice + pos.legB.entryFee) * pos.qty;

    const _record = (revenue, winner, source) => {
      const pnl = roundToCent(revenue - totalCost);
      console.log(`[Settlement] ${pos.asset} ${pos.leg} x${pos.qty}: winner=${winner} rev=$${revenue.toFixed(2)} cost=$${totalCost.toFixed(2)} pnl=$${pnl.toFixed(2)} (${source})`);
      if (this.capitalGuard) this.capitalGuard.recordTrade({ pnl, source: `settlement(${source})` });
      const tr = this.trades.find(t => t.id === pos.id);
      if (tr) {
        tr.pnl = roundToCent((tr.pnl || 0) + pnl);
        tr.settled = true; tr.settlementWinner = winner; tr.settlementSource = source;
        if (this.monitor) this.monitor.emit('trade', tr);
      }
    };

    const kLeg = pos.legA.connector.platform === 'kalshi' ? pos.legA
      : pos.legB.connector.platform === 'kalshi' ? pos.legB : null;
    if (kLeg && typeof kLeg.connector.getSettlements === 'function') {
      for (let attempt = 1; attempt <= MAX; attempt++) {
        await new Promise(r => setTimeout(r, POLL_MS));
        try {
          const settlements = await kLeg.connector.getSettlements(kLeg.market.ticker);
          const s = settlements.find(r => r.market_ticker === kLeg.market.ticker);
          if (!s) continue;
          const revCents = s.revenue != null ? s.revenue
            : (kLeg.side === 'yes' ? (s.yes_count || 0) : (s.no_count || 0)) * 100;
          const kRev = revCents / 100;
          const kalshiWon = kRev >= pos.qty * 0.90;
          const otherLeg = kLeg === pos.legA ? pos.legB : pos.legA;
          const winner = kalshiWon ? `${kLeg.platform}-${kLeg.side}` : `${otherLeg.platform}-${otherLeg.side}`;
          const otherTokens = otherLeg.tokenBalance || otherLeg.takingAmount || pos.qty;
          const totalRev = kalshiWon ? kRev : otherTokens * 1.0;
          _record(totalRev, winner, `kalshi-confirmed(attempt=${attempt})`);
          return;
        } catch (_) {}
      }
    }

    // Fallback: WS bid estimation
    const snapA = pos.legA.connector.getLiveSnapshotForSide(pos.legA.market, pos.legA.side);
    const snapB = pos.legB.connector.getLiveSnapshotForSide(pos.legB.market, pos.legB.side);
    const bidA = snapA?.bestBid || 0;
    const bidB = snapB?.bestBid || 0;
    let winner, revenue;
    if (bidA <= 0 && bidB <= 0) {
      winner = 'unknown'; revenue = pos.qty * 1.0;
    } else if (bidA >= bidB) {
      winner = `${pos.legA.platform}-${pos.legA.side}`;
      revenue = (pos.legA.tokenBalance || pos.legA.takingAmount || pos.qty) * 1.0;
    } else {
      winner = `${pos.legB.platform}-${pos.legB.side}`;
      revenue = (pos.legB.tokenBalance || pos.legB.takingAmount || pos.qty) * 1.0;
    }
    _record(revenue, winner, 'ws-estimate');
  }
}

module.exports = Dispatcher;
