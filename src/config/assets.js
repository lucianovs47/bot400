/**
 * Supported assets and their default strike-filter settings.
 * Each asset can be individually enabled/disabled per user
 * without interrupting the global monitoring loop.
 *
 * Platform-specific search hints let connectors filter relevant markets.
 *   - kalshi.seriesTicker: used to filter Kalshi series (e.g. "KXBTC15M")
 *   - polymarket.slugPrefix: slug prefix for 15-min event discovery
 *     Slug format: "{prefix}-{unixTimestamp}" where timestamp = floor(now/900)*900
 */

const ASSETS = {
  BTC: {
    symbol: 'BTC',
    name: 'Bitcoin',
    enabled: true,
    strikeFilter: {
      minSpreadPct: 2.0,
      maxSlippagePct: 0.5,
    },
    kalshi:      { seriesTicker: 'KXBTC15M' },
    polymarket:  { slugPrefix: 'btc-updown-15m' },
    predictfun:  { marketSlug: 'btc-updown-15m' },
  },
  ETH: {
    symbol: 'ETH',
    name: 'Ethereum',
    enabled: true,
    strikeFilter: {
      minSpreadPct: 2.0,
      maxSlippagePct: 0.5,
    },
    kalshi:      { seriesTicker: 'KXETH15M' },
    polymarket:  { slugPrefix: 'eth-updown-15m' },
  },
  SOL: {
    symbol: 'SOL',
    name: 'Solana',
    enabled: false,
    strikeFilter: {
      minSpreadPct: 2.5,
      maxSlippagePct: 0.8,
    },
    kalshi:      { seriesTicker: 'KXSOL15M' },
    polymarket:  { slugPrefix: 'sol-updown-15m' },
  },
  XRP: {
    symbol: 'XRP',
    name: 'XRP',
    enabled: false,
    strikeFilter: {
      minSpreadPct: 2.5,
      maxSlippagePct: 0.8,
    },
    kalshi:      { seriesTicker: 'KXXRP15M' },
    polymarket:  { slugPrefix: 'xrp-updown-15m' },
  },
};

module.exports = { ASSETS };
