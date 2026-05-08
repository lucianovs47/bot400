/**
 * Central configuration — loads .env and exports typed settings.
 *
 * API keys are NOT stored here — each user configures their own
 * keys in the dashboard (encrypted in data/users.json).
 */

require('dotenv').config();

module.exports = {
  port: parseInt(process.env.PORT, 10) || 3001,
  env: process.env.NODE_ENV || 'development',

  kalshi: {
    baseUrl: process.env.KALSHI_BASE_URL || 'https://api.elections.kalshi.com',
    wsUrl: process.env.KALSHI_WS_URL || 'wss://api.elections.kalshi.com/trade-api/ws/v2',
  },

  polymarket: {
    baseUrl: process.env.POLY_BASE_URL || 'https://clob.polymarket.com',
  },

  predictfun: {
    baseUrl: process.env.PREDICTFUN_BASE_URL || 'https://api.predict.fun',
    wsUrl: process.env.PREDICTFUN_WS_URL || 'wss://ws.predict.fun/ws',
  },

  tradeMode: process.env.TRADE_MODE || 'dry-run',

  dashboard: {
    secret: process.env.DASHBOARD_SECRET || '',
  },

  fees: {
    kalshi:    parseFloat(process.env.KALSHI_FEE_PCT || '0.07'),
    polymarket: parseFloat(process.env.POLY_FEE_PCT || '0.04'),
    // predict.fun: fee is per-market (feeRateBps from API) — no global default needed.
    minRoiPct: parseFloat(process.env.MIN_ROI_PCT || '1.0'),
  },

  // Per-platform execution tuning — edit these values directly to calibrate.
  // Not read from .env: calibration is done by editing this file, same as before.
  execution: {
    // Depth buffer factor: contracts of extra depth required beyond target size.
    // Poly FAK ~350ms, predict.fun polls fills ~400ms — start identical, tune after real fills.
    polyDepthFactor:        1.5,
    predictfunDepthFactor:  1.5,
    // Haircut scale for WS book staleness in _simulatePolyFillRevenue.
    // haircut = min(0.30, (1-avgBid) × scale). Tune per-platform once fill data is available.
    polyHaircutScale:       0.60,
    predictfunHaircutScale: 0.60,
  },

  // Telegram is per-user (configured in dashboard settings, not .env)
};
