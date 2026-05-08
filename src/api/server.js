/**
 * Express + Socket.io server — multi-platform aware.
 *
 * The server is given a context object: { monitor, decision, capitalGuard,
 * dispatcher, autoRedeemer, connectors, platforms, rebuildPlatforms }. When
 * settings change (active platforms, fee rates, stop thresholds), the server
 * applies them to the running engine without restarting the process.
 */

const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const config = require('../config');
const { signToken, requireAuth } = require('../auth/middleware');
const userStore = require('../store/users');
const { checkAndApprove } = require('../services/usdcApproval');

function createServer(ctx) {
  const { monitor, decision, capitalGuard, dispatcher, autoRedeemer, connectors = {}, rebuildPlatforms } = ctx;
  const app = express();
  const server = http.createServer(app);
  const io = new Server(server, { cors: { origin: '*' } });
  app.use(express.json());

  /* ---------- Static files ---------- */
  app.use(express.static(path.join(__dirname, '..', 'public')));

  /* ---------- Public ---------- */
  app.get('/health', (_req, res) => res.json({ status: 'ok', uptime: process.uptime() }));

  app.get('/prices', requireAuth, (_req, res) => {
    if (!monitor) return res.status(503).json({ error: 'monitor not ready' });
    res.json(monitor.getSnapshot());
  });

  app.get('/opportunities', requireAuth, (_req, res) => {
    if (!decision) return res.status(503).json({ error: 'decision engine not ready' });
    res.json({ signals: decision.getSignals(), active: decision.getActiveSignals() });
  });

  app.get('/trades', requireAuth, (_req, res) => {
    if (!dispatcher) return res.status(503).json({ error: 'dispatcher not ready' });
    res.json({
      state: dispatcher.getState(),
      trades: dispatcher.getTrades(),
      hedgeLog: dispatcher.getHedgeLog(),
      openPositions: dispatcher.getOpenPositions(),
    });
  });

  app.get('/balances', requireAuth, async (req, res) => {
    if (!capitalGuard) return res.json({});
    try {
      const keys = userStore.getKeys(req.user.username);
      const balances = await capitalGuard.getBalancesForUser(keys);
      const capState = capitalGuard.getState();
      res.json({
        ...balances,
        totalPnl: capState.totalPnl,
        tradeCount: capState.tradeCount,
      });
    } catch (_err) {
      res.json({});
    }
  });

  /* ---------- Auth ---------- */
  app.post('/api/register', async (req, res) => {
    try {
      const { username, password, inviteCode } = req.body;
      if (!username || !password || password.length < 6) {
        return res.status(400).json({ error: 'Username and password (min 6 chars) required' });
      }
      if (!inviteCode) return res.status(400).json({ error: 'Invite code required' });
      const user = await userStore.createUser(username.toLowerCase().trim(), password, inviteCode);
      const token = signToken(user.username);
      res.json({ token, username: user.username, isAdmin: userStore.isAdmin(user.username) });
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  app.post('/api/login', async (req, res) => {
    try {
      const { username, password } = req.body;
      const user = await userStore.authenticate(username?.toLowerCase().trim(), password);
      if (!user) return res.status(401).json({ error: 'Invalid credentials' });
      const token = signToken(user.username);
      res.json({ token, username: user.username, isAdmin: userStore.isAdmin(user.username) });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  /* ---------- API Keys ---------- */
  app.get('/api/keys/status', requireAuth, (req, res) => {
    res.json(userStore.hasKeys(req.user.username));
  });

  app.post('/api/keys', requireAuth, async (req, res) => {
    try {
      userStore.saveKeys(req.user.username, req.body);
      const keys = userStore.getKeys(req.user.username);

      if (connectors.kalshi && keys.kalshiApiKey && keys.kalshiPrivateKeyPem) {
        connectors.kalshi.loadUserKeys(keys).then(() => {
          if (monitor && connectors.kalshi.connected) {
            console.log('[Server] Kalshi keys loaded — re-discovering markets');
            monitor._discoverMarkets().catch(() => {});
          }
        }).catch(() => {});
      }
      if (connectors.polymarket && keys.polyApiKey) connectors.polymarket.loadUserKeys(keys);
      if (connectors.predictfun && keys.predictApiKey) connectors.predictfun.loadUserKeys(keys);

      let approval = null;
      if (req.body.polyPrivateKey && req.body.polyFunderAddress) {
        try { approval = await checkAndApprove(req.body.polyPrivateKey, req.body.polyFunderAddress); }
        catch (err) { approval = { error: err.message }; }
      }
      res.json({ ok: true, status: userStore.hasKeys(req.user.username), approval });
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  /* ---------- Settings ---------- */
  app.get('/api/settings', requireAuth, (req, res) => {
    res.json(userStore.getSettings(req.user.username) || {});
  });

  app.put('/api/settings', requireAuth, (req, res) => {
    try {
      const before = userStore.getSettings(req.user.username) || {};
      const saved = userStore.saveSettings(req.user.username, req.body);
      const norm = userStore.getSettings(req.user.username);

      if (capitalGuard) {
        capitalGuard.configure({
          entrySize: norm.entrySize,
          stopThresholds: norm.stopThresholds,
        });
      }
      if (decision) {
        decision.configure({
          minRoiPct: req.body.minRoiPct,
          maxStrikeDiff: req.body.maxStrikeDiff,
        });
      }
      if (dispatcher && norm.tradeMode) {
        dispatcher.tradeMode = norm.tradeMode;
        console.log(`[Dispatcher] trade mode: ${norm.tradeMode}`);
      }

      if (norm.feeRates) {
        if (typeof norm.feeRates.kalshi === 'number') connectors.kalshi?.setFeeRate(norm.feeRates.kalshi);
        if (typeof norm.feeRates.polymarket === 'number') connectors.polymarket?.setFeeRate(norm.feeRates.polymarket);
        if (typeof norm.feeRates.predictfun === 'number') connectors.predictfun?.setFeeRate(norm.feeRates.predictfun);
      }

      const beforeList = (before.activePlatforms || []).join(',');
      const afterList = (norm.activePlatforms || []).join(',');
      if (beforeList !== afterList && typeof rebuildPlatforms === 'function') {
        rebuildPlatforms();
      }

      res.json(saved);
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  /* ---------- Filters ---------- */
  app.get('/api/filters', requireAuth, (req, res) => {
    res.json(userStore.getFilters(req.user.username) || {});
  });
  app.put('/api/filters', requireAuth, (req, res) => {
    try { res.json(userStore.saveFilters(req.user.username, req.body)); }
    catch (err) { res.status(400).json({ error: err.message }); }
  });

  /* ---------- Platforms ---------- */
  app.get('/api/platforms', requireAuth, (_req, res) => {
    const list = Object.entries(connectors).map(([name, c]) => ({
      name,
      connected: !!c?.connected,
      hasKeys: !!(c?.apiKey || c?.privateKey),
    }));
    res.json({ all: list, active: (ctx.platforms || []).map(p => p.name) });
  });

  /* ---------- WS Live Check (admin) ---------- */
  app.get('/api/system/livecheck', requireAuth, (req, res) => {
    if (!userStore.isAdmin(req.user.username)) return res.status(403).json({ error: 'Admin only' });
    res.json({ enabled: userStore.getSystemUseWsLivecheck() });
  });

  app.put('/api/system/livecheck', requireAuth, (req, res) => {
    if (!userStore.isAdmin(req.user.username)) return res.status(403).json({ error: 'Admin only' });
    try {
      const enabled = !!req.body.enabled;
      userStore.saveSystemUseWsLivecheck(enabled);
      if (dispatcher) {
        dispatcher.useWsLivecheck = enabled;
        console.log(`[Dispatcher] useWsLivecheck: ${enabled}`);
      }
      res.json({ ok: true, enabled });
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  app.get('/api/system/ws-max-age', requireAuth, (_req, res) => {
    res.json({ maxWsAgeForLive: dispatcher ? dispatcher.maxWsAgeForLive : 300 });
  });

  app.put('/api/system/ws-max-age', requireAuth, (req, res) => {
    if (!userStore.isAdmin(req.user.username)) return res.status(403).json({ error: 'Admin only' });
    try {
      const ms = parseInt(req.body.maxWsAgeForLive, 10);
      if (isNaN(ms) || ms < 0) return res.status(400).json({ error: 'maxWsAgeForLive must be >= 0' });
      if (dispatcher) {
        dispatcher.maxWsAgeForLive = ms;
        console.log(`[Dispatcher] maxWsAgeForLive: ${ms}ms`);
      }
      res.json({ ok: true, maxWsAgeForLive: ms });
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  /* ---------- Auto-redeem log ---------- */
  app.get('/api/redeems', requireAuth, (_req, res) => {
    if (!autoRedeemer) return res.json([]);
    res.json(autoRedeemer.getLog());
  });

  /* ---------- Socket.io ---------- */
  io.on('connection', (socket) => {
    console.log('[WS] client connected:', socket.id);
    const onPrices = (data) => socket.emit('prices', data);
    const onOpps = (data) => socket.emit('opportunities', data);
    const onTrade = (data) => socket.emit('trade', data);

    if (monitor) {
      monitor.on('prices', onPrices);
      monitor.on('opportunities', onOpps);
      monitor.on('trade', onTrade);
    }
    socket.on('disconnect', () => {
      console.log('[WS] client disconnected:', socket.id);
      if (monitor) {
        monitor.removeListener('prices', onPrices);
        monitor.removeListener('opportunities', onOpps);
        monitor.removeListener('trade', onTrade);
      }
    });
  });

  return { app, server, io };
}

function startServer(ctx) {
  const { server, io } = createServer(ctx);
  server.listen(config.port, () => {
    console.log(`[Server] listening on :${config.port}`);
  });
  return { server, io };
}

module.exports = { createServer, startServer };
