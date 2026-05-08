/**
 * SARBCCODE — Multi-platform entry point.
 * Boot order: connectors → capital guard → decision engine → dispatcher → monitor → server.
 */

const { startServer } = require('./api/server');
const KalshiConnector = require('./connectors/kalshi');
const PolymarketConnector = require('./connectors/polymarket');
const PredictFunConnector = require('./connectors/predictfun');
const CapitalGuard = require('./engine/capitalGuard');
const DecisionEngine = require('./engine/decisionEngine');
const Dispatcher = require('./execution/dispatcher');
const Monitor = require('./engine/monitor');
const RtdsClient = require('./services/rtdsClient');
const AutoRedeemer = require('./services/autoRedeemer');
const TelegramNotifier = require('./services/telegram');
const userStore = require('./store/users');
const config = require('./config');

function startEventLoopLagMonitor() {
  setInterval(() => {
    const start = Date.now();
    setImmediate(() => {
      const lag = Date.now() - start;
      if (lag > 100) {
        const mem = process.memoryUsage();
        const heapUsedMB = Math.round(mem.heapUsed / 1024 / 1024);
        const heapTotalMB = Math.round(mem.heapTotal / 1024 / 1024);
        const rssMB = Math.round(mem.rss / 1024 / 1024);
        const externalMB = Math.round(mem.external / 1024 / 1024);
        console.warn(`[EventLoop] lag: ${lag}ms | heap: ${heapUsedMB}/${heapTotalMB}MB rss: ${rssMB}MB ext: ${externalMB}MB`);
      }
    });
  }, 1_000).unref();
}

async function main() {
  // Non-blocking stdout/stderr so console.log doesn't stall the event loop on PM2.
  try {
    if (process.stdout._handle?.setBlocking) process.stdout._handle.setBlocking(false);
    if (process.stderr._handle?.setBlocking) process.stderr._handle.setBlocking(false);
  } catch (_) {}

  console.log('========================================');
  console.log(' SARBCCODE — Multi-Platform Arbitrage');
  console.log('========================================');
  startEventLoopLagMonitor();

  // 1. Init all connectors (always — user picks which to enable later)
  const kalshi = new KalshiConnector();
  const polymarket = new PolymarketConnector();
  const predictfun = new PredictFunConnector();
  const allConnectors = { kalshi, polymarket, predictfun };

  const users = userStore.listUsers();
  const settings = users.length > 0 ? userStore.getSettings(users[0]) : null;

  // 2. Load user keys + apply per-platform fee overrides
  if (users.length > 0) {
    const keys = userStore.getKeys(users[0]);
    if (keys?.kalshiApiKey && keys?.kalshiPrivateKeyPem) {
      kalshi.apiKey = keys.kalshiApiKey;
      kalshi.privateKey = keys.kalshiPrivateKeyPem;
      console.log(`[Boot] loaded Kalshi keys (user=${users[0]})`);
    }
    if (keys?.polyApiKey) {
      polymarket.loadUserKeys(keys);
      console.log(`[Boot] loaded Polymarket keys (user=${users[0]})`);
    }
    if (keys?.predictApiKey) {
      predictfun.loadUserKeys(keys);
      console.log(`[Boot] loaded Predict.fun keys (user=${users[0]})`);
    }
  }

  if (settings?.feeRates) {
    if (typeof settings.feeRates.kalshi === 'number') kalshi.setFeeRate(settings.feeRates.kalshi);
    if (typeof settings.feeRates.polymarket === 'number') polymarket.setFeeRate(settings.feeRates.polymarket);
    if (typeof settings.feeRates.predictfun === 'number') predictfun.setFeeRate(settings.feeRates.predictfun);
  }

  // 3. Connect each platform that has credentials
  await kalshi.connect();
  await polymarket.connect();
  if (polymarket.wallet && polymarket.connected) {
    console.log('[Boot] deriving Polymarket API keys...');
    await polymarket.deriveApiKeys();
  }
  if (predictfun.apiKey) {
    await predictfun.connect();
    if (predictfun.connected && predictfun.wallet) {
      console.log('[Boot] predict.fun: ensuring USDT approvals...');
      predictfun.ensureUsdtApproval().catch(err =>
        console.warn('[Boot] predict.fun USDT approval error:', err.message)
      );
    }
  }

  // 4. Build active platforms list (from settings; only include connected ones)
  const desired = settings?.activePlatforms?.length ? settings.activePlatforms : ['kalshi', 'polymarket'];
  const platforms = desired
    .map(name => ({ name, connector: allConnectors[name] }))
    .filter(p => p.connector && p.connector.connected);
  console.log(`[Boot] active platforms: ${platforms.map(p => p.name).join(', ') || '(none)'}`);

  if (platforms.length === 0) {
    console.warn('[Boot] no platforms connected — server will start in idle mode');
  }

  // 5. Capital guard
  const capitalGuard = new CapitalGuard(platforms);
  if (settings) {
    capitalGuard.configure({
      entrySize: settings.entrySize,
      stopThresholds: settings.stopThresholds,
    });
  }
  capitalGuard.start();

  // 6. Decision engine
  const decision = new DecisionEngine(capitalGuard);

  // 7. Telegram
  const telegram = new TelegramNotifier();

  // 8. Dispatcher
  const dispatcher = new Dispatcher(capitalGuard, platforms);
  if (settings?.tradeMode) dispatcher.tradeMode = settings.tradeMode;
  dispatcher.useWsLivecheck = userStore.getSystemUseWsLivecheck();
  console.log(`[Dispatcher] mode: ${dispatcher.tradeMode} | wsLivecheck: ${dispatcher.useWsLivecheck}`);
  dispatcher.startEarlyExitMonitor();

  // 9. RTDS — must connect before Monitor so strikes are ready
  const rtdsClient = new RtdsClient();
  rtdsClient.connect();
  console.log('[Boot] waiting for RTDS prices...');
  await new Promise((resolve) => {
    let resolved = false;
    const done = () => { if (!resolved) { resolved = true; resolve(); } };
    const check = setInterval(() => {
      const symbols = ['BTC', 'ETH', 'SOL', 'XRP'];
      const ready = symbols.filter((s) => rtdsClient.getLatestPrice(s) !== null);
      if (ready.length === symbols.length) { clearInterval(check); done(); }
    }, 500);
    setTimeout(() => { clearInterval(check); done(); }, 15_000);
  });
  const allPrices = ['BTC', 'ETH', 'SOL', 'XRP'].map((s) => {
    const p = rtdsClient.getLatestPrice(s);
    return p ? `${s}=$${p.price.toFixed(2)}` : `${s}=waiting`;
  });
  console.log(`[Boot] RTDS prices: ${allPrices.join(', ')}`);

  // 10. Monitor — multi-platform
  const monitor = new Monitor(platforms, decision, dispatcher, rtdsClient);
  dispatcher.monitor = monitor;
  if (platforms.length > 0) await monitor.start();

  monitor.on('trade', (trade) => telegram.notifyTrade(trade));

  // 11. Auto-redeemer (Polymarket-specific for now — predict.fun would need its own)
  const autoRedeemer = new AutoRedeemer();
  if (polymarket.connected) autoRedeemer.start(polymarket);

  // 12. API / dashboard server
  startServer({
    monitor, decision, capitalGuard, dispatcher, autoRedeemer,
    connectors: allConnectors,
    platforms,
    rebuildPlatforms: () => {
      // Caller (server) invokes this after settings change to rebuild platforms.
      const fresh = userStore.getSettings(users[0]);
      const desired = fresh?.activePlatforms?.length ? fresh.activePlatforms : ['kalshi', 'polymarket'];
      const next = desired
        .map(name => ({ name, connector: allConnectors[name] }))
        .filter(p => p.connector && p.connector.connected);
      console.log(`[Boot] rebuilding platforms: ${next.map(p => p.name).join(', ')}`);
      capitalGuard.setPlatforms(next);
      dispatcher.setPlatforms(next);
      // Monitor: stop and restart with new platforms.
      monitor.stop();
      monitor.platforms = next;
      monitor.start().catch(err => console.error('[Boot] monitor restart:', err.message));
      return next;
    },
  });

  // Initial balance fetch for first user (best effort)
  if (users.length > 0) {
    const keys = userStore.getKeys(users[0]);
    capitalGuard.getBalancesForUser(keys).then((b) => {
      const lines = Object.entries(b).map(([k, v]) => `${k}: ${v != null ? '$' + v.toFixed(2) : 'N/A'}`);
      console.log(`[CapitalGuard] balances — ${lines.join(', ')}`);
    }).catch(() => {});
  }
}

main().catch((err) => {
  console.error('[FATAL]', err);
  process.exit(1);
});
