/**
 * Polymarket order signing utilities — CTF Exchange V2 (live April 28, 2026).
 *
 * Two layers of auth required:
 *   1. L2 HMAC headers — authenticate API requests (key/secret/passphrase)
 *   2. EIP-712 signature — sign each order with wallet private key
 *
 * V2 EIP-712 signed struct (ORDER_TYPES — 11 fields, no taker/nonce/expiration/feeRateBps):
 *   salt, maker, signer, tokenId, makerAmount, takerAmount, side,
 *   signatureType, timestamp, metadata, builder
 *
 * V2 wire payload still includes taker+expiration (not signed, used by CLOB for routing).
 *
 * V2 signatureType enum (changed from V1):
 *   0 = EOA            (unchanged)
 *   1 = POLY_PROXY     (was 2 in V1 — proxy wallet)
 *   2 = POLY_GNOSIS_SAFE (new)
 *   3 = POLY_1271      (new — smart contract wallets)
 *
 * Sources:
 *   clob-client-v2/src/order-utils/model/ctfExchangeV2TypedData.ts
 *   clob-client-v2/src/order-utils/model/signatureTypeV2.ts
 *   clob-client-v2/src/types/ordersV2.ts (orderToJsonV2)
 */

const crypto = require('crypto');
const { ethers } = require('ethers');

// CTF Exchange V2 contracts on Polygon (live April 28, 2026 ~12:00 UTC)
// Source: @polymarket/clob-client-v2 src/config.ts MATIC_CONTRACTS
const CTF_EXCHANGE_V2          = '0xE111180000d2663C0091e4f400237545B87B996B'; // standard markets
const NEG_RISK_EXCHANGE_V2     = '0xe2222d279d744050d28e00520010520000310F59'; // neg-risk markets (BTC/ETH crypto)

const BYTES32_ZERO = '0x' + '00'.repeat(32);

// EIP-712 domain is built per-order — verifyingContract depends on neg-risk flag.
// neg-risk markets (BTC/ETH crypto) use NEG_RISK_EXCHANGE_V2.
// Standard markets use CTF_EXCHANGE_V2.
// Wrong contract address = wrong domain hash = "invalid signature".
function _buildDomain(negRisk) {
  return {
    name: 'Polymarket CTF Exchange',
    version: '2',
    chainId: 137,
    verifyingContract: negRisk ? NEG_RISK_EXCHANGE_V2 : CTF_EXCHANGE_V2,
  };
}

// V2 EIP-712 signed struct — 11 fields (taker/nonce/expiration/feeRateBps removed)
const ORDER_TYPES = {
  Order: [
    { name: 'salt',          type: 'uint256' },
    { name: 'maker',         type: 'address' },
    { name: 'signer',        type: 'address' },
    { name: 'tokenId',       type: 'uint256' },
    { name: 'makerAmount',   type: 'uint256' },
    { name: 'takerAmount',   type: 'uint256' },
    { name: 'side',          type: 'uint8'   },
    { name: 'signatureType', type: 'uint8'   },
    { name: 'timestamp',     type: 'uint256' },
    { name: 'metadata',      type: 'bytes32' },
    { name: 'builder',       type: 'bytes32' },
  ],
};

const SIDE_BUY = 0;
const SIDE_SELL = 1;

// V2 signature types
const SIG_TYPE_EOA          = 0; // EOA wallet (unchanged)
const SIG_TYPE_POLY_PROXY   = 1; // Polymarket proxy wallet (was 2 in V1)
const SIG_TYPE_GNOSIS_SAFE  = 2; // Gnosis Safe (maker=proxy, signer=EOA owner)

const USDC_DECIMALS = 6;

/**
 * Create L2 HMAC authentication headers for CLOB API requests.
 */
function createL2Headers(apiKey, apiSecret, passphrase, signerAddress, method, path, body = '') {
  const timestamp = Math.floor(Date.now() / 1000).toString();
  let message = `${timestamp}${method}${path}`;
  if (body !== undefined && body !== '') message += body;

  const secretStd = apiSecret.replace(/-/g, '+').replace(/_/g, '/');
  const hmacKey = Buffer.from(secretStd, 'base64');
  const hmac = crypto.createHmac('sha256', hmacKey);
  hmac.update(message);

  const sigBase64 = hmac.digest('base64');
  const signature = sigBase64.replace(/\+/g, '-').replace(/\//g, '_');

  return {
    'POLY_ADDRESS':    signerAddress || '',
    'POLY_API_KEY':    apiKey,
    'POLY_SIGNATURE':  signature,
    'POLY_TIMESTAMP':  timestamp,
    'POLY_PASSPHRASE': passphrase,
    'Content-Type':    'application/json',
  };
}

/**
 * Build and sign a Polymarket V2 order using EIP-712.
 *
 * Wire payload shape for POST /order (V2) — verified against orderToJsonV2:
 *   {
 *     deferExec:  false,
 *     postOnly:   false,
 *     order: {
 *       salt:          number,   // parseInt() on CLOB side
 *       maker:         address,
 *       signer:        address,
 *       taker:         address,  // zero address (not signed, used for routing)
 *       tokenId:       string,
 *       makerAmount:   string,   // uint256 decimal string
 *       takerAmount:   string,
 *       side:          "BUY"|"SELL",
 *       signatureType: number,   // 0=EOA, 1=POLY_PROXY (changed from V1!)
 *       timestamp:     string,   // ms since epoch as decimal STRING
 *       expiration:    string,   // "0" (not signed, present in wire)
 *       metadata:      bytes32,
 *       builder:       bytes32,
 *       signature:     string,
 *     },
 *     owner:     string,         // API key UUID
 *     orderType: "GTC"|"FOK"|"FAK",
 *   }
 *
 * @param {ethers.Wallet} wallet
 * @param {object} params
 * @param {string} params.tokenId
 * @param {number} params.price
 * @param {number} params.size
 * @param {string} params.side - "BUY" or "SELL"
 * @param {string} params.funderAddress
 * @param {string} params.apiKey
 * @param {boolean} [params.negRisk=false] - true for neg-risk markets (BTC/ETH crypto)
 * @returns {Promise<object>}
 */
async function buildSignedOrder(wallet, { tokenId, price, size, side, funderAddress, apiKey, negRisk = false }) {
  const signerAddress = wallet.address;
  const makerAddress = funderAddress || signerAddress;
  const usesProxy = funderAddress && funderAddress.toLowerCase() !== signerAddress.toLowerCase();
  // V2: EOA=0 direto, Gnosis Safe=2 (maker=proxy, signer=EOA owner — confirmado funcionar em 2026-04-30)
  const sigType = usesProxy ? SIG_TYPE_GNOSIS_SAFE : SIG_TYPE_EOA;
  console.log(`[Signer] signer=${signerAddress} maker=${makerAddress} usesProxy=${usesProxy} sigType=${sigType}`);

  const sideNum = side === 'BUY' ? SIDE_BUY : SIDE_SELL;

  const rawSize = BigInt(Math.round(size * 1e6));

  let makerAmount, takerAmount;
  if (sideNum === SIDE_BUY) {
    makerAmount = BigInt(Math.round(price * size * 1e6));
    takerAmount = rawSize;
  } else {
    makerAmount = rawSize;
    takerAmount = BigInt(Math.round(price * size * 1e6));
  }

  const salt = Math.floor(Math.random() * 2 ** 52);
  // V2: timestamp (ms) replaces nonce for uniqueness — signed as uint256
  const timestamp = Date.now().toString();

  // ----- EIP-712 signing — 11 fields (no taker/nonce/expiration/feeRateBps) -----
  const orderForSigning = {
    salt:          salt.toString(),
    maker:         makerAddress,
    signer:        signerAddress,
    tokenId:       tokenId,
    makerAmount:   makerAmount.toString(),
    takerAmount:   takerAmount.toString(),
    side:          sideNum,
    signatureType: sigType,
    timestamp,
    metadata:      BYTES32_ZERO,
    builder:       BYTES32_ZERO,
  };

  const domain = _buildDomain(negRisk);
  console.log(`[Signer] EIP-712 domain: verifyingContract=${domain.verifyingContract} negRisk=${negRisk}`);
  const signature = await wallet.signTypedData(domain, ORDER_TYPES, orderForSigning);

  // ----- Wire payload (orderToJsonV2 format) -----
  // V2: taker is NOT in the wire payload (V2 buildOrder omits taker entirely —
  // sending it causes CLOB to fall back to V1 signature verification and fail).
  // expiration is still present (set to "0") as per V2 orderToJsonV2.
  return {
    deferExec: false,
    postOnly:  false,
    order: {
      salt:          salt,               // number (CLOB does parseInt)
      maker:         makerAddress,
      signer:        signerAddress,
      // taker: intentionally OMITTED — V2 does not include this field
      tokenId:       tokenId,
      makerAmount:   makerAmount.toString(),
      takerAmount:   takerAmount.toString(),
      side:          side === 'BUY' ? 'BUY' : 'SELL',
      signatureType: sigType,
      timestamp,                         // STRING (decimal ms)
      expiration:    '0',                // present in wire, not signed
      metadata:      BYTES32_ZERO,
      builder:       BYTES32_ZERO,
      signature,
    },
    owner:     apiKey || '',
    orderType: 'GTC',                    // overridden by caller (FAK for our bot)
  };
}

module.exports = {
  createL2Headers,
  buildSignedOrder,
  SIDE_BUY,
  SIDE_SELL,
  USDC_DECIMALS,
};
