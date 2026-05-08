/**
 * Capital Guard — multi-platform balance + sizing + PnL tracking.
 *
 * Maintains a cached balance per platform (refreshed in background every 60s
 * + after every trade) and gates new trades when any active platform falls
 * below its user-configured stop threshold.
 *
 * Each platform connector exposes one of:
 *   - `getBalance()` — async, returns the platform balance (e.g. Kalshi USD,
 *     Predict USDT). When this returns a number, it's used directly.
 *   - `getBalanceAddress()` — returns an EVM address. When the connector
 *     uses Polygon collateral (Polymarket pUSD), CapitalGuard fetches it
 *     via the cached Polygon RPC providers.
 *
 * Settings (per-user):
 *   - entrySize           exact $ per trade
 *   - stopThresholds      { [platformName]: number }
 */

const { ethers } = require('ethers');

// Polygon RPCs — used only for Polymarket pUSD balance fetch.
const POLYGON_RPCS = [
  'https://polygon-rpc.com',
  'https://polygon.llamarpc.com',
  'https://polygon-bor-rpc.publicnode.com',
  'https://polygon.gateway.tenderly.co',
  'https://rpc.ankr.com/polygon',
];
const PUSD_ADDRESS = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB';
const PUSD_ABI = ['function balanceOf(address) view returns (uint256)'];

class CapitalGuard {
  /**
   * @param {Array<{name, connector}>} platforms
   */
  constructor(platforms) {
    this.platforms = Array.isArray(platforms) ? platforms.filter(p => p && p.connector) : [];
    this.entrySize = 10;
    this.stopThresholds = {}; // platformName → number; 0 means no stop

    this._balances = {};       // platformName → number | null
    this._lastFetch = 0;

    this._refreshTimer = null;
    this._refreshIntervalMs = 60_000;

    this._polygonProviders = POLYGON_RPCS.map(rpcUrl =>
      new ethers.JsonRpcProvider(rpcUrl, 137, { staticNetwork: true, batchMaxCount: 1 }),
    );

    this.totalPnl = 0;
    this.tradeCount = 0;
  }

  setPlatforms(platforms) {
    this.platforms = Array.isArray(platforms) ? platforms.filter(p => p && p.connector) : [];
    this.refreshBalances().catch(() => {});
  }

  configure({ entrySize, stopThresholds }) {
    if (entrySize != null) this.entrySize = entrySize;
    if (stopThresholds && typeof stopThresholds === 'object') {
      Object.assign(this.stopThresholds, stopThresholds);
    }
  }

  start() {
    this.refreshBalances().catch((e) => console.warn('[CapitalGuard] initial refresh failed:', e.message));
    if (this._refreshTimer) clearInterval(this._refreshTimer);
    this._refreshTimer = setInterval(() => this.refreshBalances().catch(() => {}), this._refreshIntervalMs);
    if (this._refreshTimer.unref) this._refreshTimer.unref();
    console.log(`[CapitalGuard] background refresh started (every ${this._refreshIntervalMs/1000}s)`);
  }

  stop() {
    if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
  }

  /**
   * Synchronous read — never blocks the dispatcher critical path.
   * Returns { ok, reason?, balances }.
   */
  canTrade() {
    const balances = this.getCachedBalances();
    for (const p of this.platforms) {
      const stop = this.stopThresholds[p.name] || 0;
      const bal = balances[p.name];
      if (stop > 0 && bal !== null && bal !== undefined && bal < stop) {
        return {
          ok: false,
          reason: `${p.name} balance ($${bal.toFixed(2)}) below stop ($${stop})`,
          balances,
        };
      }
    }
    return { ok: true, balances };
  }

  size(totalCostPerUnit) {
    if (totalCostPerUnit <= 0 || this.entrySize <= 0) return null;
    const units = Math.floor(this.entrySize / totalCostPerUnit * 100) / 100;
    if (units <= 0) return null;
    const betSize = Math.round(units * totalCostPerUnit * 100) / 100;
    return { units, betSize };
  }

  recordTrade(trade) {
    const before = this.totalPnl;
    this.totalPnl += trade.pnl || 0;
    this.totalPnl = Math.round(this.totalPnl * 100) / 100;
    if (trade.countTrade) this.tradeCount++;
    const pnl = trade.pnl || 0;
    if (pnl !== 0) {
      console.debug(
        `[CapitalGuard] PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} `
        + `(${trade.source || 'unknown'}) → totalPnl: $${before.toFixed(2)} → $${this.totalPnl.toFixed(2)}`,
      );
    }
  }

  /** Returns cached balances as a flat map { platformName: number|null }. */
  getCachedBalances() {
    const snap = {};
    for (const p of this.platforms) snap[p.name] = this._balances[p.name] ?? null;
    return snap;
  }

  /** Per-platform direct read. */
  getBalance(platformName) {
    return this._balances[platformName] ?? null;
  }

  async refreshBalances() {
    const results = await Promise.allSettled(
      this.platforms.map(p => this._fetchPlatformBalance(p)),
    );
    this.platforms.forEach((p, idx) => {
      const r = results[idx];
      if (r.status === 'fulfilled') this._balances[p.name] = r.value;
    });
    this._lastFetch = Date.now();
    return this.getCachedBalances();
  }

  async _fetchPlatformBalance(p) {
    if (!p?.connector) return null;
    try {
      // Prefer connector.getBalance() when it returns a number.
      if (typeof p.connector.getBalance === 'function') {
        const bal = await p.connector.getBalance();
        if (typeof bal === 'number') return bal;
      }
      // Fallback for Polymarket: pUSD on Polygon via shared RPC pool.
      if (typeof p.connector.getBalanceAddress === 'function') {
        const addr = p.connector.getBalanceAddress();
        if (addr) return this._fetchPusdBalance(addr);
      }
      return null;
    } catch (err) {
      console.debug(`[CapitalGuard] ${p.name} balance error:`, err.message);
      return null;
    }
  }

  async _fetchPusdBalance(address) {
    if (!address) return null;
    const TIMEOUT_MS = 2500;
    const attempts = this._polygonProviders.map((provider, idx) => new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timeout RPC#${idx}`)), TIMEOUT_MS);
      (async () => {
        try {
          const usdc = new ethers.Contract(PUSD_ADDRESS, PUSD_ABI, provider);
          const raw = await usdc.balanceOf(address);
          clearTimeout(timer);
          resolve(parseFloat(ethers.formatUnits(raw, 6)));
        } catch (err) {
          clearTimeout(timer);
          reject(err);
        }
      })();
    }));
    try { return await Promise.any(attempts); }
    catch (err) {
      console.debug('[CapitalGuard] all Polygon RPCs failed:', err.message);
      return null;
    }
  }

  /**
   * Per-user balance fetch using the user's keys (read by /balances endpoint).
   * Each platform connector with `getBalanceForKeys(keys)` is called; otherwise
   * we fall back to the cached value.
   */
  async getBalancesForUser(keys) {
    const out = {};
    await Promise.all(this.platforms.map(async (p) => {
      try {
        if (typeof p.connector.getBalanceForKeys === 'function') {
          out[p.name] = await p.connector.getBalanceForKeys(keys);
        } else if (p.name === 'kalshi' && keys?.kalshiApiKey && keys?.kalshiPrivateKeyPem) {
          // Kalshi supports per-user signed REST.
          try {
            const data = await p.connector._getWithKeys(
              '/trade-api/v2/portfolio/balance',
              keys.kalshiApiKey,
              keys.kalshiPrivateKeyPem,
            );
            const cents = data.balance ?? data.available_balance ?? 0;
            out[p.name] = cents / 100;
          } catch (_) { out[p.name] = null; }
        } else if (p.name === 'polymarket' && keys?.polyFunderAddress) {
          out[p.name] = await this._fetchPusdBalance(keys.polyFunderAddress);
        } else {
          out[p.name] = this._balances[p.name] ?? null;
        }
      } catch (_) {
        out[p.name] = null;
      }
    }));
    return out;
  }

  getState() {
    return {
      entrySize: this.entrySize,
      stopThresholds: { ...this.stopThresholds },
      balances: this.getCachedBalances(),
      totalPnl: this.totalPnl,
      tradeCount: this.tradeCount,
    };
  }
}

module.exports = CapitalGuard;
