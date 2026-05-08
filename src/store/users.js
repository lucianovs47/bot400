/**
 * User store — file-based storage with encrypted API keys.
 *
 * Each user has:
 *   - login credentials (bcrypt-hashed password)
 *   - encrypted platform API keys (AES-256-GCM)
 *   - per-asset filter settings
 *   - trade settings
 *
 * Invite codes: required for registration, single-use, generated via CLI.
 * Data stored at data/users.json + data/invites.json (gitignored).
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const config = require('../config');

const DATA_DIR = path.join(__dirname, '..', '..', 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const INVITES_FILE = path.join(DATA_DIR, 'invites.json');
const SYSTEM_FILE = path.join(DATA_DIR, 'system.json');
const ALGO = 'aes-256-gcm';

function _getEncKey() {
  const secret = config.dashboard.secret || 'sarbccode-default-key-change-me';
  return crypto.scryptSync(secret, 'sarbccode-salt', 32);
}

function _encrypt(text) {
  if (!text) return '';
  const key = _getEncKey();
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(ALGO, key, iv);
  let enc = cipher.update(text, 'utf8', 'hex');
  enc += cipher.final('hex');
  const tag = cipher.getAuthTag().toString('hex');
  return `${iv.toString('hex')}:${tag}:${enc}`;
}

function _decrypt(data) {
  if (!data || !data.includes(':')) return '';
  const [ivHex, tagHex, enc] = data.split(':');
  const key = _getEncKey();
  const decipher = crypto.createDecipheriv(ALGO, key, Buffer.from(ivHex, 'hex'));
  decipher.setAuthTag(Buffer.from(tagHex, 'hex'));
  let dec = decipher.update(enc, 'hex', 'utf8');
  dec += decipher.final('utf8');
  return dec;
}

function _ensureDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function _load() {
  if (!fs.existsSync(USERS_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8')); }
  catch { return {}; }
}

function _save(users) {
  _ensureDir();
  fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2));
}

function _loadInvites() {
  if (!fs.existsSync(INVITES_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(INVITES_FILE, 'utf8')); }
  catch { return {}; }
}

function _saveInvites(invites) {
  _ensureDir();
  fs.writeFileSync(INVITES_FILE, JSON.stringify(invites, null, 2));
}

/* ---------------------------------------------------------- */
/*  Invite Codes                                               */
/* ---------------------------------------------------------- */

/**
 * Generate an invite code.
 * @param {number|null} expiresInDays - null = lifetime, number = expires in N days
 * @returns {{ code: string, expiresAt: string|null }}
 */
function createInvite(expiresInDays = null) {
  const invites = _loadInvites();
  const code = crypto.randomBytes(8).toString('hex').toUpperCase(); // 16-char code

  const expiresAt = expiresInDays
    ? new Date(Date.now() + expiresInDays * 86400000).toISOString()
    : null;

  invites[code] = {
    createdAt: new Date().toISOString(),
    expiresAt,
    used: false,
    usedBy: null,
  };

  _saveInvites(invites);
  return { code, expiresAt };
}

/**
 * Validate and consume an invite code.
 * Returns true if valid, throws if invalid/expired/used.
 */
function useInvite(code) {
  if (!code) throw new Error('Invite code required');

  const invites = _loadInvites();
  const invite = invites[code.toUpperCase()];

  if (!invite) throw new Error('Invalid invite code');
  if (invite.used) throw new Error('Invite code already used');
  if (invite.expiresAt && new Date(invite.expiresAt) < new Date()) {
    throw new Error('Invite code expired');
  }

  return true; // valid, will be consumed in createUser
}

function _consumeInvite(code, username) {
  const invites = _loadInvites();
  const key = code.toUpperCase();
  if (invites[key]) {
    invites[key].used = true;
    invites[key].usedBy = username;
    invites[key].usedAt = new Date().toISOString();
    _saveInvites(invites);
  }
}

/**
 * List all invite codes (for admin CLI).
 */
function listInvites() {
  return _loadInvites();
}

/* ---------------------------------------------------------- */
/*  Users                                                      */
/* ---------------------------------------------------------- */

// Default per-asset filter
const DEFAULT_ASSET_FILTER = {
  enabled: true,
  entrySize: 10,           // exact $ per trade for this asset
  minRoiPct: 3.0,          // min ROI % for this asset
  maxStrikeDiff: 5.0,      // max strike price difference in $ between platforms
};

/**
 * Register a new user (requires valid invite code).
 */
async function createUser(username, password, inviteCode) {
  // Validate invite first
  useInvite(inviteCode);

  const users = _load();
  if (users[username]) throw new Error('User already exists');

  const hash = await bcrypt.hash(password, 10);
  users[username] = {
    passwordHash: hash,
    createdAt: new Date().toISOString(),
    inviteCode: inviteCode.toUpperCase(),
    keys: {
      kalshiApiKey: '',
      kalshiPrivateKeyPem: '',
      polyApiKey: '',
      polyApiSecret: '',
      polyPassphrase: '',
      polyPrivateKey: '',
      polyFunderAddress: '',
      predictApiKey: '',
      predictPrivateKey: '',
      predictAccountAddress: '',
    },
    settings: {
      tradeMode: 'dry-run',
      // Multi-platform: which platforms to enable + per-platform stops.
      // activePlatforms: subset of ['kalshi','polymarket','predictfun'].
      // stopThresholds: per-platform balance floor; 0 = no stop.
      activePlatforms: ['kalshi', 'polymarket'],
      stopThresholds: { kalshi: 0, polymarket: 0, predictfun: 0 },
      // Per-platform fee rates (override connector defaults).
      feeRates: { kalshi: 0.07, polymarket: 0.04, predictfun: 0.01 },
      // Per-platform depth buffer factors. Reserved book depth required
      // beyond target qty to absorb latency between order send and fill.
      // Kalshi IOC ~80ms (small buffer), Poly FAK ~370ms / Predict ~400ms (larger).
      // bufContracts = max(MIN_BUF, ceil(qty × factor)) where MIN_BUF=3 (kalshi) or 5 (others).
      depthFactors: { kalshi: 1.0, polymarket: 1.5, predictfun: 1.5 },
      entrySize: 10,
      // Round / cooldown timing — 0 disables each gate.
      entryCutoffSecs: 180,    // block new entries this close to round end
      retryCooldownSecs: 5,    // cooldown after a failed dispatch
      reentryCooldownSecs: 60, // cooldown after a successful early exit
      maxTradesPerRound: 2,
      maxHedgesPerRound: 5,
      hedgeCooldownSecs: 30,
      telegramEnabled: false,
      telegramBotToken: '',
      telegramChatId: '',
      // Legacy fields kept for migration (read by getSettings normaliser).
      kalshiStopAt: 0,
      polyStopAt: 0,
      activePlatform2: 'polymarket',
    },
    filters: {
      BTC: { ...DEFAULT_ASSET_FILTER },
      ETH: { ...DEFAULT_ASSET_FILTER },
      SOL: { ...DEFAULT_ASSET_FILTER },
      XRP: { ...DEFAULT_ASSET_FILTER },
    },
  };

  _save(users);
  _consumeInvite(inviteCode, username);

  return { username, createdAt: users[username].createdAt };
}

async function authenticate(username, password) {
  const users = _load();
  const user = users[username];
  if (!user) return null;

  const match = await bcrypt.compare(password, user.passwordHash);
  if (!match) return null;

  return { username, createdAt: user.createdAt };
}

function saveKeys(username, keys) {
  const users = _load();
  if (!users[username]) throw new Error('User not found');

  users[username].keys = {
    kalshiApiKey: _encrypt(keys.kalshiApiKey || ''),
    kalshiPrivateKeyPem: _encrypt(keys.kalshiPrivateKeyPem || ''),
    polyApiKey: _encrypt(keys.polyApiKey || ''),
    polyApiSecret: _encrypt(keys.polyApiSecret || ''),
    polyPassphrase: _encrypt(keys.polyPassphrase || ''),
    polyPrivateKey: _encrypt(keys.polyPrivateKey || ''),
    polyFunderAddress: keys.polyFunderAddress || '',
    predictApiKey: _encrypt(keys.predictApiKey || ''),
    predictPrivateKey: _encrypt(keys.predictPrivateKey || ''),
    predictAccountAddress: keys.predictAccountAddress || '', // Predict Account (smart wallet)
  };

  _save(users);
}

function getKeys(username) {
  const users = _load();
  const user = users[username];
  if (!user) return null;

  const k = user.keys || {};
  return {
    kalshiApiKey: _decrypt(k.kalshiApiKey),
    kalshiPrivateKeyPem: _decrypt(k.kalshiPrivateKeyPem),
    polyApiKey: _decrypt(k.polyApiKey),
    polyApiSecret: _decrypt(k.polyApiSecret),
    polyPassphrase: _decrypt(k.polyPassphrase),
    polyPrivateKey: _decrypt(k.polyPrivateKey),
    polyFunderAddress: k.polyFunderAddress || '',
    predictApiKey: _decrypt(k.predictApiKey),
    predictPrivateKey: _decrypt(k.predictPrivateKey),
    predictAccountAddress: k.predictAccountAddress || '',
  };
}

function hasKeys(username) {
  const users = _load();
  const user = users[username];
  if (!user) return { kalshi: false, polymarket: false };

  const k = user.keys || {};
  return {
    kalshi: !!(k.kalshiApiKey && k.kalshiPrivateKeyPem),
    polymarket: !!(k.polyApiKey && k.polyApiSecret && k.polyPassphrase),
  };
}

function saveSettings(username, settings) {
  const users = _load();
  if (!users[username]) throw new Error('User not found');
  users[username].settings = { ...users[username].settings, ...settings };
  _save(users);
  return users[username].settings;
}

/**
 * Normalize legacy settings into the multi-platform schema. Read-only —
 * the stored object is left intact, only the returned copy is patched.
 */
function _normalizeSettings(raw) {
  if (!raw) return null;
  const s = { ...raw };

  // Migrate activePlatforms from legacy `activePlatform2` (Kalshi was always implicit).
  if (!Array.isArray(s.activePlatforms)) {
    const legacy = s.activePlatform2 === 'predictfun' ? 'predictfun' : 'polymarket';
    s.activePlatforms = ['kalshi', legacy];
  }

  // Migrate stop thresholds from legacy fields. The legacy `polyStopAt`
  // applied to whichever platform2 was active at the time, so route it to
  // the platform implied by `activePlatform2` (default: polymarket).
  const legacyP2 = s.activePlatform2 === 'predictfun' ? 'predictfun' : 'polymarket';
  if (!s.stopThresholds || typeof s.stopThresholds !== 'object') {
    s.stopThresholds = { kalshi: s.kalshiStopAt || 0, polymarket: 0, predictfun: 0 };
    s.stopThresholds[legacyP2] = s.polyStopAt || 0;
  } else {
    if (s.stopThresholds.kalshi == null) s.stopThresholds.kalshi = s.kalshiStopAt || 0;
    if (s.stopThresholds.polymarket == null) s.stopThresholds.polymarket = legacyP2 === 'polymarket' ? (s.polyStopAt || 0) : 0;
    if (s.stopThresholds.predictfun == null) s.stopThresholds.predictfun = legacyP2 === 'predictfun' ? (s.polyStopAt || 0) : 0;
  }

  // Default fee rates (user can override; 0 means "use connector default").
  if (!s.feeRates || typeof s.feeRates !== 'object') {
    s.feeRates = { kalshi: 0.07, polymarket: 0.04, predictfun: 0.01 };
  }

  // Default depth buffer factors per platform.
  if (!s.depthFactors || typeof s.depthFactors !== 'object') {
    s.depthFactors = { kalshi: 1.0, polymarket: 1.5, predictfun: 1.5 };
  } else {
    if (s.depthFactors.kalshi == null)     s.depthFactors.kalshi = 1.0;
    if (s.depthFactors.polymarket == null) s.depthFactors.polymarket = 1.5;
    if (s.depthFactors.predictfun == null) s.depthFactors.predictfun = 1.5;
  }

  // Defaults for round/cooldown timings — explicit 0 disables a gate.
  if (s.entryCutoffSecs == null) s.entryCutoffSecs = 180;
  if (s.retryCooldownSecs == null) s.retryCooldownSecs = 5;
  if (s.reentryCooldownSecs == null) s.reentryCooldownSecs = 60;
  if (s.maxTradesPerRound == null) s.maxTradesPerRound = 2;
  if (s.maxHedgesPerRound == null) s.maxHedgesPerRound = 5;
  if (s.hedgeCooldownSecs == null) s.hedgeCooldownSecs = 30;
  if (s.entrySize == null) s.entrySize = 10;
  if (s.tradeMode == null) s.tradeMode = 'dry-run';
  return s;
}

function getSettings(username) {
  const users = _load();
  return _normalizeSettings(users[username]?.settings);
}

function saveFilters(username, filters) {
  const users = _load();
  if (!users[username]) throw new Error('User not found');
  users[username].filters = { ...users[username].filters, ...filters };
  _save(users);
  return users[username].filters;
}

function getFilters(username) {
  const users = _load();
  return users[username]?.filters || null;
}

function listUsers() {
  const users = _load();
  return Object.keys(users);
}

/**
 * Admin = first registered user.
 */
function isAdmin(username) {
  const users = _load();
  const firstUser = Object.keys(users)[0];
  return firstUser === username;
}

/* ---------------------------------------------------------- */
/*  System Config (global, admin-only)                         */
/* ---------------------------------------------------------- */

function _loadSystem() {
  if (!fs.existsSync(SYSTEM_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(SYSTEM_FILE, 'utf8')); }
  catch { return {}; }
}

function _saveSystem(data) {
  _ensureDir();
  fs.writeFileSync(SYSTEM_FILE, JSON.stringify(data, null, 2));
}

/**
 * Global WS live-check toggle. When true, the dispatcher reads top-of-book
 * from the in-memory WS cache (~20ms). When false, it falls back to REST
 * (~150-1800ms). Default true. Admin-only.
 */
function saveSystemUseWsLivecheck(enabled) {
  const sys = _loadSystem();
  sys.useWsLivecheck = !!enabled;
  _saveSystem(sys);
}

function getSystemUseWsLivecheck() {
  const sys = _loadSystem();
  // Default to true when never set, so new installs get the fast path.
  return sys.useWsLivecheck === undefined ? true : !!sys.useWsLivecheck;
}

module.exports = {
  createUser,
  authenticate,
  saveKeys,
  getKeys,
  hasKeys,
  listUsers,
  isAdmin,
  saveSettings,
  getSettings,
  saveFilters,
  getFilters,
  createInvite,
  useInvite,
  listInvites,
  saveSystemUseWsLivecheck,
  getSystemUseWsLivecheck,
};
