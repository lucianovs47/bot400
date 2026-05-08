/**
 * Decision Engine — multi-platform arbitrage detector.
 *
 * Generic over N platforms. For each pair of platforms (X, Y), evaluates two
 * combos per pair of markets:
 *   X.yes + Y.no → if X.yes_price + Y.no_price < $1, arb exists
 *   X.no  + Y.yes → if X.no_price  + Y.yes_price < $1, arb exists
 *
 * For 3 platforms: 3 pairs × 2 combos = up to 6 candidate signals per asset
 * per scan tick. The Monitor picks the best (highest ROI) and dispatches it.
 *
 * Fee math is delegated to each connector via connector.calcEntryFee(market,
 * price). The engine doesn't know whether a platform uses parabolic or CTF
 * fees — each connector implements its own formula.
 *
 * Signal output:
 *   {
 *     id, asset, leg, timestamp,
 *     legA: { platform, connector, market, side, price, fee },
 *     legB: { platform, connector, market, side, price, fee },
 *     totalCost, grossSpread, estimatedFee, netProfit, roiPct,
 *     sizing, execute, minRoiPct, entrySize, earlyExitPct,
 *   }
 */

const config = require('../config');

class DecisionEngine {
  constructor(capitalGuard) {
    this.capitalGuard = capitalGuard;
    this.minRoiPct = config.fees.minRoiPct;
    this.maxStrikeDiff = 5.0; // max $ difference between platform strikes (default)
    this.signals = [];
    this.maxSignals = 100;
  }

  configure({ minRoiPct, maxStrikeDiff }) {
    if (minRoiPct != null) this.minRoiPct = minRoiPct;
    if (maxStrikeDiff != null) this.maxStrikeDiff = maxStrikeDiff;
  }

  _logThrottled(key, msg) {
    if (!this._logTimers) this._logTimers = {};
    const now = Date.now();
    if (this._logTimers[key] && now - this._logTimers[key] < 30_000) return;
    this._logTimers[key] = now;
    if (Object.keys(this._logTimers).length > 200) {
      for (const k of Object.keys(this._logTimers)) {
        if (now - this._logTimers[k] > 60_000) delete this._logTimers[k];
      }
    }
    console.log(msg);
  }

  /**
   * Tradability filter applied to every market regardless of platform.
   *   - prices populated (yesAsk and noAsk both > 0 = WS book ready)
   *   - prices not at extremes (0.10 < ask < 0.90 → fades phantom signals
   *     at expiry edges, where one side trades at $0.99)
   *   - market not closed/expired
   */
  _isTradable(market) {
    if (!market) return false;
    const tag = `${market.platform || '?'}/${market.asset || '?'}/${market.conditionId || market.ticker || '?'}`;
    if (market.closed) {
      this._logThrottled(`${tag}-closed`, `[Decision] _isTradable reject ${tag}: closed=true`);
      return false;
    }
    if (market.active === false) {
      this._logThrottled(`${tag}-inactive`, `[Decision] _isTradable reject ${tag}: active=false`);
      return false;
    }
    const yA = market.yesAsk || 0;
    const nA = market.noAsk || 0;
    if (yA <= 0 || nA <= 0) {
      this._logThrottled(`${tag}-noprice`, `[Decision] _isTradable reject ${tag}: yesAsk=${yA} noAsk=${nA} (book not ready)`);
      return false;
    }
    if (yA <= 0.10 || yA >= 0.90 || nA <= 0.10 || nA >= 0.90) {
      this._logThrottled(`${tag}-extreme`, `[Decision] _isTradable reject ${tag}: yesAsk=${yA.toFixed(3)} noAsk=${nA.toFixed(3)} outside (0.10, 0.90)`);
      return false;
    }
    return true;
  }

  /**
   * Per-asset filter for a platform's markets. Returns the list narrowed to
   * the current 15-min window. If the platform exposes more than one market
   * per asset (Kalshi has many strikes, predict.fun usually 1), they all
   * stay in the candidate pool — the strike filter handles cross-platform
   * matching downstream.
   */
  _filterMarkets(markets) {
    if (!Array.isArray(markets)) return [];
    const now = Date.now();
    return markets.filter((m) => {
      if (!m) return false;
      if (m.endDate) {
        const endStr = String(m.endDate).length <= 10 ? `${m.endDate}T23:59:59Z` : m.endDate;
        const endMs = new Date(endStr).getTime();
        if (endMs < now) {
          const tag = `${m.platform || '?'}/${m.asset || '?'}/${m.conditionId || m.ticker || '?'}`;
          this._logThrottled(`${tag}-expired`,
            `[Decision] _filterMarkets reject ${tag}: endDate=${m.endDate} expired (${Math.round((now - endMs)/1000)}s ago)`);
          return false;
        }
      }
      return this._isTradable(m);
    });
  }

  /**
   * Scan all platform pairs for an asset.
   * @param {string} assetSymbol
   * @param {Array<{name, connector, markets}>} platforms
   * @param {object} overrides per-asset user filters: { minRoiPct, maxStrikeDiff, entrySize, earlyExitPct }
   * @returns {Array} signals
   */
  scan(assetSymbol, platforms, overrides = {}) {
    const savedMinRoi = this.minRoiPct;
    const savedMaxStrike = this.maxStrikeDiff;
    const savedEntrySize = this.capitalGuard?.entrySize ?? null;
    if (overrides.minRoiPct != null) this.minRoiPct = overrides.minRoiPct;
    if (overrides.maxStrikeDiff != null) this.maxStrikeDiff = overrides.maxStrikeDiff;
    if (overrides.entrySize != null && this.capitalGuard) this.capitalGuard.entrySize = overrides.entrySize;

    const opportunities = [];

    if (!Array.isArray(platforms) || platforms.length < 2) {
      this.minRoiPct = savedMinRoi;
      this.maxStrikeDiff = savedMaxStrike;
      if (savedEntrySize != null && this.capitalGuard) this.capitalGuard.entrySize = savedEntrySize;
      return opportunities;
    }

    // Pre-filter each platform's markets to tradable ones in the current window.
    const prepared = platforms.map((p) => ({
      name: p.name,
      connector: p.connector,
      markets: this._filterMarkets(p.markets),
    })).filter((p) => p.markets.length > 0 && p.connector);

    if (prepared.length < 2) {
      this.minRoiPct = savedMinRoi;
      this.maxStrikeDiff = savedMaxStrike;
      if (savedEntrySize != null && this.capitalGuard) this.capitalGuard.entrySize = savedEntrySize;
      return opportunities;
    }

    // Distinct platform letter — Polymarket and Predict.fun both start with 'P',
    // which collided in legName / log throttle keys (silenced one platform).
    const platformLetter = (name) => {
      if (name === 'predictfun') return 'F';
      return name[0].toUpperCase();
    };

    // Iterate every unordered pair (X, Y) with X.index < Y.index.
    for (let i = 0; i < prepared.length; i++) {
      for (let j = i + 1; j < prepared.length; j++) {
        const A = prepared[i];
        const B = prepared[j];
        const aL = platformLetter(A.name);
        const bL = platformLetter(B.name);
        for (const mA of A.markets) {
          for (const mB of B.markets) {
            // Combo 1: A.yes + B.no
            const combo1 = this._evaluateCombo({
              asset: assetSymbol,
              legName: `${aL}.yes+${bL}.no`,
              legA: { platform: A.name, connector: A.connector, market: mA, side: 'yes' },
              legB: { platform: B.name, connector: B.connector, market: mB, side: 'no' },
              earlyExitPct: overrides.earlyExitPct ?? null,
            });
            if (combo1) opportunities.push(combo1);

            // Combo 2: A.no + B.yes
            const combo2 = this._evaluateCombo({
              asset: assetSymbol,
              legName: `${aL}.no+${bL}.yes`,
              legA: { platform: A.name, connector: A.connector, market: mA, side: 'no' },
              legB: { platform: B.name, connector: B.connector, market: mB, side: 'yes' },
              earlyExitPct: overrides.earlyExitPct ?? null,
            });
            if (combo2) opportunities.push(combo2);
          }
        }
      }
    }

    this.minRoiPct = savedMinRoi;
    this.maxStrikeDiff = savedMaxStrike;
    if (savedEntrySize != null && this.capitalGuard) this.capitalGuard.entrySize = savedEntrySize;
    return opportunities;
  }

  /**
   * Evaluate one combo (legA + legB). Both legs must produce a tradable
   * outcome combination (legA.side + legB.side = full payout coverage).
   * Returns a signal object or null.
   */
  _evaluateCombo({ asset, legName, legA, legB, earlyExitPct }) {
    const priceA = this._buyPrice(legA);
    const priceB = this._buyPrice(legB);
    if (!(priceA > 0) || !(priceB > 0)) {
      this._logThrottled(`${asset}-${legName}-noprice`,
        `[Decision] ${asset} ${legName}: missing price — A=${priceA} B=${priceB}`);
      return null;
    }

    const tokenA = legA.connector.getSideToken(legA.market, legA.side);
    const tokenB = legB.connector.getSideToken(legB.market, legB.side);
    if (!tokenA || !tokenB) {
      this._logThrottled(`${asset}-${legName}-notoken`,
        `[Decision] ${asset} ${legName}: missing tokenId — A(${legA.platform})=${tokenA ? 'ok' : 'MISSING'} B(${legB.platform})=${tokenB ? 'ok' : 'MISSING'}`);
      return null;
    }

    // Strike consistency check between platforms in the pair.
    const strikeA = legA.market.strike;
    const strikeB = legB.market.strike;
    if (strikeA == null || strikeB == null) {
      this._logThrottled(`${asset}-${legName}-nostrike`,
        `[Decision] ${asset} ${legName}: strike missing — A=${strikeA} B=${strikeB}`);
      return null;
    }
    // Larger tolerance when one platform uses an independent oracle snapshot
    // (predict.fun observed $30-50 drift vs Kalshi/RTDS in production).
    const usesIndependentOracle = legA.platform === 'predictfun' || legB.platform === 'predictfun';
    const effectiveMaxStrike = usesIndependentOracle ? 500 : this.maxStrikeDiff;
    const strikeGap = Math.abs(strikeA - strikeB);
    if (strikeGap > effectiveMaxStrike) {
      this._logThrottled(`${asset}-${legName}-strikegap`,
        `[Decision] ${asset} ${legName}: strike gap $${strikeGap.toFixed(0)} > max $${effectiveMaxStrike} (A=$${strikeA} B=$${strikeB})`);
      return null;
    }

    const totalCostRaw = priceA + priceB;
    if (totalCostRaw >= 1.0) {
      this._logThrottled(`${asset}-${legName}-noarb`,
        `[Decision] ${asset} ${legName}: no arb — A@${priceA.toFixed(3)} + B@${priceB.toFixed(3)} = $${totalCostRaw.toFixed(3)}`);
      return null;
    }

    const grossSpread = 1.0 - totalCostRaw;
    const feeA = legA.connector.calcEntryFee(legA.market, priceA);
    const feeB = legB.connector.calcEntryFee(legB.market, priceB);
    const worstFee = feeA + feeB;
    const netProfit = grossSpread - worstFee;
    if (netProfit <= 0) {
      this._logThrottled(`${asset}-${legName}-feeskill`,
        `[Decision] ${asset} ${legName}: fees eat spread — A@${priceA.toFixed(3)} + B@${priceB.toFixed(3)} = $${totalCostRaw.toFixed(3)} fee=$${worstFee.toFixed(3)}`);
      return null;
    }

    const roiPct = (netProfit / totalCostRaw) * 100;
    if (roiPct < this.minRoiPct) {
      this._logThrottled(`${asset}-${legName}-lowroi`,
        `[Decision] ${asset} ${legName}: roi=${roiPct.toFixed(2)}% < min=${this.minRoiPct}% — total $${totalCostRaw.toFixed(3)}`);
      return null;
    }

    const sizing = this.capitalGuard ? this.capitalGuard.size(totalCostRaw) : null;

    const signal = {
      id: `${asset}-${legName}-${Date.now()}`,
      asset,
      leg: legName,
      timestamp: new Date().toISOString(),

      legA: {
        platform: legA.platform,
        connector: legA.connector,
        market: legA.market,
        side: legA.side,
        price: Math.round(priceA * 1000) / 1000,
        fee: Math.round(feeA * 10000) / 10000,
      },
      legB: {
        platform: legB.platform,
        connector: legB.connector,
        market: legB.market,
        side: legB.side,
        price: Math.round(priceB * 1000) / 1000,
        fee: Math.round(feeB * 10000) / 10000,
      },

      totalCost: Math.round(totalCostRaw * 1000) / 1000,
      grossSpread: Math.round(grossSpread * 1000) / 1000,
      estimatedFee: Math.round(worstFee * 1000) / 1000,
      netProfit: Math.round(netProfit * 1000) / 1000,
      roiPct: Math.round(roiPct * 100) / 100,

      sizing,
      execute: sizing !== null,
      minRoiPct: this.minRoiPct,
      entrySize: this.capitalGuard?.entrySize ?? null,
      earlyExitPct,
    };

    // Store a serializable copy — connector refs would blow the stack in JSON.stringify
    const storedSignal = {
      ...signal,
      legA: { ...signal.legA, connector: undefined },
      legB: { ...signal.legB, connector: undefined },
    };
    this.signals.push(storedSignal);
    if (this.signals.length > this.maxSignals) {
      this.signals = this.signals.slice(-this.maxSignals);
    }

    const action = signal.execute ? 'EXECUTE' : 'WATCH';
    this._logThrottled(`${asset}-${legName}-${action}`,
      `[Decision] ${action} ${asset} ${legName}: ` +
      `${legA.platform} ${legA.side}@${priceA.toFixed(3)} + ${legB.platform} ${legB.side}@${priceB.toFixed(3)} = ` +
      `${totalCostRaw.toFixed(3)} | ROI: ${roiPct.toFixed(2)}% | Net: $${netProfit.toFixed(3)}` +
      (sizing ? ` | Size: ${sizing.units} ($${sizing.betSize})` : ''),
    );

    return signal;
  }

  /**
   * Buy price for a leg. Reads market.yesAsk / market.noAsk (already populated
   * from each platform's WS book by the connector).
   */
  _buyPrice(leg) {
    const m = leg.market;
    if (!m) return 0;
    return leg.side === 'yes' || leg.side === 'up' ? (m.yesAsk || 0) : (m.noAsk || 0);
  }

  getSignals() { return this.signals; }
  getActiveSignals() { return this.signals.filter((s) => s.execute); }
}

module.exports = DecisionEngine;
