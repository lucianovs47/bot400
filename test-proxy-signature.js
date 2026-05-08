/**
 * test-proxy-signature.js
 *
 * Testa se sigType=2 (POLY_GNOSIS_SAFE) é aceito pelo CLOB da Polymarket V2.
 *
 * Coloca uma ordem FAK de $0.01 a um preço impossível (0.01) — não vai
 * preencher. O objetivo é ver se a ASSINATURA passa (HTTP 200) ou se retorna
 * "invalid signature" (HTTP 400 / erro de sig).
 *
 * Resultados possíveis:
 *   ✅  "not filled" / "fully filled" / vazio  → sigType=2 aceito, podemos voltar ao proxy
 *   ❌  "invalid signature"                    → V2 não suporta Gnosis Safe desse jeito
 *   ❌  "not enough balance"                   → sig OK, mas proxy não tem pUSD aprovado
 *
 * Usage: node test-proxy-signature.js
 */

const axios = require('axios');
const crypto = require('crypto');
const { ethers } = require('ethers');
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const { ClobClient } = require('@polymarket/clob-client');
const users = require('./src/store/users');

// ─── constantes V2 ───────────────────────────────────────────────────────────
const CLOB_URL              = 'https://clob.polymarket.com';
const GAMMA_URL             = 'https://gamma-api.polymarket.com';
const CTF_EXCHANGE_V2       = '0xE111180000d2663C0091e4f400237545B87B996B';
const NEG_RISK_EXCHANGE_V2  = '0xe2222d279d744050d28e00520010520000310F59';
const BYTES32_ZERO          = '0x' + '00'.repeat(32);

const SIG_TYPE_EOA          = 0;
const SIG_TYPE_GNOSIS_SAFE  = 2;

// EIP-712 V2 order struct (11 fields — sem taker/nonce/expiration/feeRateBps)
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

// ─── helpers ─────────────────────────────────────────────────────────────────

function createL2Headers(apiKey, apiSecret, passphrase, signerAddress, method, path, body = '') {
  const timestamp = Math.floor(Date.now() / 1000).toString();
  let message = `${timestamp}${method}${path}`;
  if (body) message += body;

  const secretStd = apiSecret.replace(/-/g, '+').replace(/_/g, '/');
  const hmacKey = Buffer.from(secretStd, 'base64');
  const sig = crypto.createHmac('sha256', hmacKey).update(message).digest('base64')
    .replace(/\+/g, '-').replace(/\//g, '_');

  return {
    'POLY_ADDRESS':    signerAddress,
    'POLY_API_KEY':    apiKey,
    'POLY_SIGNATURE':  sig,
    'POLY_TIMESTAMP':  timestamp,
    'POLY_PASSPHRASE': passphrase,
    'Content-Type':    'application/json',
  };
}

async function buildOrder(wallet, { makerAddress, sigType, tokenId, price, size, negRisk }) {
  const signerAddress = wallet.address;
  const sideNum = 0; // BUY

  const makerAmount = BigInt(Math.round(price * size * 1e6));
  const takerAmount = BigInt(Math.round(size * 1e6));
  const salt = Math.floor(Math.random() * 2 ** 52);
  const timestamp = Date.now().toString();

  const orderForSigning = {
    salt:          salt.toString(),
    maker:         makerAddress,
    signer:        signerAddress,
    tokenId,
    makerAmount:   makerAmount.toString(),
    takerAmount:   takerAmount.toString(),
    side:          sideNum,
    signatureType: sigType,
    timestamp,
    metadata:      BYTES32_ZERO,
    builder:       BYTES32_ZERO,
  };

  const domain = {
    name: 'Polymarket CTF Exchange',
    version: '2',
    chainId: 137,
    verifyingContract: negRisk ? NEG_RISK_EXCHANGE_V2 : CTF_EXCHANGE_V2,
  };

  const signature = await wallet.signTypedData(domain, ORDER_TYPES, orderForSigning);

  return {
    deferExec: false,
    postOnly:  false,
    order: {
      salt,
      maker:         makerAddress,
      signer:        signerAddress,
      tokenId,
      makerAmount:   makerAmount.toString(),
      takerAmount:   takerAmount.toString(),
      side:          'BUY',
      signatureType: sigType,
      timestamp,
      expiration:    '0',
      metadata:      BYTES32_ZERO,
      builder:       BYTES32_ZERO,
      signature,
    },
    owner:     '',  // será substituído abaixo com apiKey
    orderType: 'FAK',
  };
}

function extractFirstTokenId(raw) {
  if (!raw) return null;
  // Gamma retorna clobTokenIds como string JSON: '["id1","id2"]'
  if (typeof raw === 'string') {
    try { raw = JSON.parse(raw); } catch (_) {}
  }
  if (Array.isArray(raw)) return raw[0] || null;
  if (typeof raw === 'string') return raw.trim() || null;
  return null;
}

async function findTestTokenId() {
  // Tenta BTC 15-min primeiro, senão pega qualquer mercado ativo com liquidez
  const searches = [
    { slug_prefix: 'will-btc-', limit: 10 },
    { slug_prefix: 'will-eth-', limit: 10 },
    { active: true, limit: 20 },  // fallback genérico
  ];

  for (const params of searches) {
    try {
      const resp = await axios.get(`${GAMMA_URL}/markets`, {
        params: { ...params, closed: false },
        timeout: 10_000,
      });

      const markets = Array.isArray(resp.data) ? resp.data : (resp.data?.markets || []);
      for (const m of markets) {
        const tokenId = extractFirstTokenId(m.clobTokenIds || m.tokens);
        if (tokenId && tokenId.length > 10) {
          console.log(`Mercado: ${m.question || m.slug}`);
          return { tokenId, negRisk: m.negRisk ?? false };
        }
      }
    } catch (e) {
      console.warn(`Gamma API (${JSON.stringify(params)}) falhou:`, e.message);
    }
  }
  return null;
}

async function testMode(label, wallet, { apiKey, apiSecret, passphrase, makerAddress, sigType, tokenId, negRisk }) {
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`TESTANDO: ${label}`);
  console.log(`  maker:  ${makerAddress}`);
  console.log(`  signer: ${wallet.address}`);
  console.log(`  sigType: ${sigType}`);
  console.log(`  negRisk: ${negRisk}`);

  const order = await buildOrder(wallet, {
    makerAddress,
    sigType,
    tokenId,
    price: 0.01,   // preço impossível — não vai preencher
    size: 1,       // 1 contrato ($0.01 máximo de risco)
    negRisk,
  });
  order.owner = apiKey;

  const path = '/order';
  const bodyStr = JSON.stringify(order);
  const headers = createL2Headers(apiKey, apiSecret, passphrase, wallet.address, 'POST', path, bodyStr);

  try {
    const resp = await axios({
      method: 'POST',
      url: `${CLOB_URL}${path}`,
      data: order,
      headers,
      timeout: 15_000,
      proxy: false,
    });

    const data = resp.data;
    if (data?.error) {
      const err = typeof data.error === 'string' ? data.error : JSON.stringify(data.error);
      if (err.toLowerCase().includes('invalid signature') || err.toLowerCase().includes('invalid sig')) {
        console.log(`  ❌ ASSINATURA REJEITADA: ${err}`);
        return 'sig_invalid';
      }
      if (err.toLowerCase().includes('balance') || err.toLowerCase().includes('allowance')) {
        console.log(`  ⚠️  SIG OK — mas sem balance/allowance: ${err}`);
        return 'balance_error';
      }
      console.log(`  ⚠️  SIG OK — outro erro CLOB: ${err}`);
      return 'other_error';
    }

    console.log(`  ✅ ORDEM ACEITA (HTTP 200): ${JSON.stringify(data).slice(0, 200)}`);
    return 'accepted';

  } catch (e) {
    const status = e.response?.status;
    const body = e.response?.data ? JSON.stringify(e.response.data).slice(0, 300) : e.message;
    if (body.toLowerCase().includes('invalid signature') || body.toLowerCase().includes('invalid sig')) {
      console.log(`  ❌ ASSINATURA REJEITADA (HTTP ${status}): ${body}`);
      return 'sig_invalid';
    }
    if (body.toLowerCase().includes('balance') || body.toLowerCase().includes('allowance')) {
      console.log(`  ⚠️  SIG OK — sem balance/allowance (HTTP ${status}): ${body}`);
      return 'balance_error';
    }
    console.log(`  ❓ ERRO HTTP ${status}: ${body}`);
    return 'http_error';
  }
}

// ─── main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('=== TESTE DE ASSINATURA POLYMARKET V2 ===\n');
  console.log('Objetivo: verificar se sigType=2 (POLY_GNOSIS_SAFE) é aceito.');
  console.log('Preço da ordem: $0.01 → não vai preencher, só testa a assinatura.\n');

  const userIds = users.listUsers();
  if (!userIds.length) { console.error('Sem usuários'); process.exit(1); }
  const keys = users.getKeys(userIds[0]);

  const wallet = new ethers.Wallet(keys.polyPrivateKey);
  const eoaAddress   = wallet.address;
  const proxyAddress = keys.polyFunderAddress;

  console.log(`EOA (signer): ${eoaAddress}`);
  console.log(`Proxy (Gnosis Safe): ${proxyAddress}`);

  if (!proxyAddress) {
    console.error('polyFunderAddress não configurado!');
    process.exit(1);
  }

  if (proxyAddress.toLowerCase() === eoaAddress.toLowerCase()) {
    console.error('polyFunderAddress == EOA — proxy não está configurado.');
    process.exit(1);
  }

  // Re-deriva chaves de API via ClobClient — as chaves do user store podem
  // estar desatualizadas se o bot re-derivou em memória sem salvar no store.
  console.log('\nDerivando chaves de API...');
  const pk = keys.polyPrivateKey.startsWith('0x') ? keys.polyPrivateKey : `0x${keys.polyPrivateKey}`;
  const account = privateKeyToAccount(pk);
  const viemWallet = createWalletClient({ account, chain: polygon, transport: http() });
  const clobClient = new ClobClient(CLOB_URL, 137, viemWallet);
  let apiKey, apiSecret, passphrase;
  try {
    const creds = await clobClient.deriveApiKey();
    if (!creds?.key) throw new Error('deriveApiKey retornou vazio');
    apiKey     = creds.key;
    apiSecret  = creds.secret;
    passphrase = creds.passphrase;
    console.log(`apiKey derivado: ${apiKey.slice(0, 8)}...`);
  } catch (e) {
    console.warn(`deriveApiKey falhou (${e.message}) — usando chaves do store`);
    apiKey     = keys.polyApiKey;
    apiSecret  = keys.polyApiSecret;
    passphrase = keys.polyPassphrase;
    console.log(`apiKey do store: ${apiKey?.slice(0, 8)}...`);
  }

  // Busca tokenId de mercado ativo
  console.log('\nBuscando mercado ativo...');
  const market = await findTestTokenId();
  if (!market) {
    console.error('Não encontrou mercado ativo para teste.');
    process.exit(1);
  }
  console.log(`tokenId: ${market.tokenId.slice(0, 16)}... negRisk=${market.negRisk}`);

  const baseParams = {
    apiKey,
    apiSecret,
    passphrase,
    tokenId:    market.tokenId,
    negRisk:    market.negRisk,
  };

  // Teste 1: EOA (controle — deve funcionar)
  const r0 = await testMode('sigType=0 EOA (controle)', wallet, {
    ...baseParams,
    makerAddress: eoaAddress,
    sigType: SIG_TYPE_EOA,
  });

  // Teste 2: POLY_GNOSIS_SAFE com proxy como maker
  const r2 = await testMode('sigType=2 GNOSIS_SAFE (proxy como maker)', wallet, {
    ...baseParams,
    makerAddress: proxyAddress,
    sigType: SIG_TYPE_GNOSIS_SAFE,
  });

  // Resumo
  console.log('\n' + '═'.repeat(60));
  console.log('RESULTADO:');
  console.log(`  sigType=0 EOA:          ${r0}`);
  console.log(`  sigType=2 Gnosis Safe:  ${r2}`);
  console.log('');

  if (r2 === 'sig_invalid') {
    console.log('❌ Gnosis Safe NÃO funciona — V2 não suporta este modo.');
    console.log('   Opções: manter EOA ou investigar sigType=1 com registerProxy on-chain.');
  } else if (r2 === 'balance_error') {
    console.log('✅ Gnosis Safe FUNCIONA — sig aceita, mas proxy não tem pUSD aprovado.');
    console.log('   Para usar o proxy: aprovar pUSD no proxy para o CTF Exchange V2.');
    console.log('   (pUSD já foi movido para EOA — precisa mover de volta ou redepositar)');
  } else if (r2 === 'accepted' || r2 === 'other_error') {
    console.log('✅ Gnosis Safe FUNCIONA — podemos voltar ao modo proxy!');
  } else {
    console.log('⚠️  Resultado inconclusivo — verificar logs acima.');
  }

  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
