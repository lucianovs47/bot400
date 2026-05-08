/**
 * setup-poly-v2-proxy.js
 *
 * Diagnoses and fixes the proxy-operator relationship on Polymarket CTF Exchange V2.
 *
 * Problem: Existing proxy wallets (created in V1) are NOT automatically registered
 * as operators on the new V2 CTF Exchange. This causes HTTP 400 "invalid signature"
 * when placing orders with sigType=1 (POLY_PROXY).
 *
 * This script:
 *   1. Checks on-chain operator status (EOA authorized for proxy on V2)
 *   2. Checks pUSD balance (proxy and EOA)
 *   3. If NOT registered, calls CTF_EXCHANGE_V2.registerProxy(proxyAddress)
 *   4. Reports what the bot should use (EOA mode vs proxy mode)
 *
 * After running this, if proxy registration succeeds, revert polymarket.js to
 * use funderAddress: this.funderAddress (proxy mode) in _postOrder.
 *
 * Usage: node setup-poly-v2-proxy.js
 */

const { ethers } = require('ethers');
const users = require('./src/store/users');

// Multiple RPCs — same list as capitalGuard.js (ordered by reliability)
const POLYGON_RPCS = [
  'https://polygon-rpc.com',
  'https://polygon-bor-rpc.publicnode.com',
  'https://polygon.gateway.tenderly.co',
  'https://rpc.ankr.com/polygon',
  'https://polygon.llamarpc.com',
];

const CTF_EXCHANGE_V2   = '0xE111180000d2663C0091e4f400237545B87B996B';
const NEG_RISK_V2       = '0xe2222d279d744050d28e00520010520000310F59';
const PUSD_ADDRESS      = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB';
const USDC_E_ADDRESS    = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174';

// CTF Exchange V2 ABI — operator functions
// Source: Polymarket clob-client-v2 / CTFExchange V2 contract
const EXCHANGE_ABI = [
  // V1 + V2: Check if signer is registered operator for proxy
  'function operators(address proxy, address operator) view returns (bool)',
  // V1 + V2: Register caller's EOA as operator for a given proxy wallet
  'function registerProxy(address proxy) external',
  // Domain separator (sanity check)
  'function DOMAIN_SEPARATOR() view returns (bytes32)',
];

const ERC20_ABI = [
  'function balanceOf(address account) view returns (uint256)',
  'function decimals() view returns (uint8)',
  'function allowance(address owner, address spender) view returns (uint256)',
  'function approve(address spender, uint256 amount) returns (bool)',
];

async function main() {
  console.log('=== POLYMARKET V2 PROXY SETUP ===\n');

  const userIds = users.listUsers();
  if (userIds.length === 0) { console.error('No users in store'); process.exit(1); }
  const keys = users.getKeys(userIds[0]);

  if (!keys.polyPrivateKey) { console.error('Missing polyPrivateKey'); process.exit(1); }

  // Try multiple RPCs until one works
  let provider = null;
  for (const rpc of POLYGON_RPCS) {
    try {
      const p = new ethers.JsonRpcProvider(rpc);
      await p.getBlockNumber();  // sanity check
      provider = p;
      console.log(`RPC: ${rpc}\n`);
      break;
    } catch (e) {
      console.log(`RPC failed (${rpc}): ${e.message.slice(0, 60)}`);
    }
  }
  if (!provider) {
    console.error('All Polygon RPCs unreachable — cannot check on-chain state');
    console.error('Try running: curl -s https://polygon-rpc.com -d \'{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}\' -H "Content-Type: application/json"');
    process.exit(1);
  }

  const wallet = new ethers.Wallet(keys.polyPrivateKey, provider);
  const eoa = wallet.address;
  const proxy = keys.polyFunderAddress || '';

  console.log(`EOA   (signer): ${eoa}`);
  console.log(`Proxy (funder): ${proxy || '(none)'}`);
  console.log('');

  // ── 1. pUSD balances ──────────────────────────────────────────────────────
  console.log('=== pUSD BALANCES (on-chain) ===');
  const pusd = new ethers.Contract(PUSD_ADDRESS, ERC20_ABI, provider);
  const usdce = new ethers.Contract(USDC_E_ADDRESS, ERC20_ABI, provider);

  const eoaPusd  = await pusd.balanceOf(eoa);
  const eoaUsdce = await usdce.balanceOf(eoa);
  console.log(`EOA   pUSD:   ${ethers.formatUnits(eoaPusd,  6)}`);
  console.log(`EOA   USDC.e: ${ethers.formatUnits(eoaUsdce, 6)}`);

  if (proxy) {
    const proxyPusd  = await pusd.balanceOf(proxy);
    const proxyUsdce = await usdce.balanceOf(proxy);
    console.log(`Proxy pUSD:   ${ethers.formatUnits(proxyPusd,  6)}`);
    console.log(`Proxy USDC.e: ${ethers.formatUnits(proxyUsdce, 6)}`);
  }
  console.log('');

  // ── 2. pUSD allowance (proxy → V2) ────────────────────────────────────────
  console.log('=== pUSD ALLOWANCES (on-chain) ===');
  const eoaAllowV2  = await pusd.allowance(eoa, CTF_EXCHANGE_V2);
  console.log(`EOA → V2 Exchange:   ${ethers.formatUnits(eoaAllowV2, 6)}`);
  if (proxy) {
    const proxyAllowV2 = await pusd.allowance(proxy, CTF_EXCHANGE_V2);
    console.log(`Proxy → V2 Exchange: ${ethers.formatUnits(proxyAllowV2, 6)}`);
  }
  console.log('');

  if (!proxy || proxy.toLowerCase() === eoa.toLowerCase()) {
    console.log('No separate proxy address configured — EOA mode is correct. Nothing to register.');
    console.log('Ensure EOA has pUSD balance and allowance for V2 exchange.');
    process.exit(0);
  }

  // ── 3. Operator status on CTF Exchange V2 ─────────────────────────────────
  console.log('=== OPERATOR STATUS (V2 CTF Exchange) ===');
  const exchange = new ethers.Contract(CTF_EXCHANGE_V2, EXCHANGE_ABI, wallet);

  let isOperator = false;
  try {
    isOperator = await exchange.operators(proxy, eoa);
    console.log(`isOperator(proxy, eoa) = ${isOperator}`);
  } catch (e) {
    console.log(`operators() call failed: ${e.message.slice(0, 100)}`);
    console.log('Contract may use a different function name — checking alternatives...');

    // Try alternative: some versions have operators(address signer, address proxy) reversed
    try {
      isOperator = await exchange.operators(eoa, proxy);
      console.log(`operators(eoa, proxy) = ${isOperator}`);
    } catch (e2) {
      console.log(`Alternative also failed: ${e2.message.slice(0, 100)}`);
    }
  }

  // Also check domain separator to confirm we have the right contract
  try {
    const domain = await exchange.DOMAIN_SEPARATOR();
    console.log(`DOMAIN_SEPARATOR: ${domain.slice(0, 20)}...`);
  } catch (e) {
    console.log(`DOMAIN_SEPARATOR unavailable: ${e.message.slice(0, 60)}`);
  }
  console.log('');

  // ── 4. Register if needed ─────────────────────────────────────────────────
  if (isOperator) {
    console.log('✅ EOA is ALREADY registered as operator for proxy on V2!');
    console.log('   → Proxy mode should work. Check if signer.js sigType issue was the problem.');
    console.log('   → In polymarket.js _postOrder, change back to: funderAddress: this.funderAddress');
  } else {
    console.log('❌ EOA NOT registered as operator for proxy on V2.');
    console.log('   → Calling CTF_EXCHANGE_V2.registerProxy(proxy)...');

    try {
      const gasEst = await exchange.registerProxy.estimateGas(proxy);
      console.log(`   Gas estimate: ${gasEst}`);

      const tx = await exchange.registerProxy(proxy, {
        gasLimit: gasEst * 2n,
      });
      console.log(`   TX submitted: ${tx.hash}`);
      console.log('   Waiting for confirmation...');
      const receipt = await tx.wait();
      console.log(`   ✅ Confirmed in block ${receipt.blockNumber}`);
      console.log('');
      console.log('Operator registration DONE. Now update polymarket.js:');
      console.log('   In _postOrder: funderAddress: this.funderAddress  (back to proxy mode)');
      console.log('   In getTokenBalance: signature_type: 1  (proxy, not EOA)');
    } catch (e) {
      console.log(`   ❌ registerProxy FAILED: ${e.message}`);
      console.log('');
      console.log('Fallback options:');
      console.log('  A) Deposit pUSD from EOA wallet directly on Polymarket UI');
      console.log('     → Creates CLOB balance under EOA → current EOA mode works');
      console.log('  B) Withdraw all from Polymarket → transfer proxy→EOA → re-deposit from EOA');
    }
  }

  console.log('');
  console.log('=== SUMMARY ===');
  console.log(`EOA pUSD on-chain:   ${ethers.formatUnits(eoaPusd, 6)}`);
  console.log(`Proxy registered V2: ${isOperator}`);
  if (isOperator) {
    console.log('→ RECOMMENDED: Revert to proxy mode (maker=proxy, sigType=1)');
  } else {
    console.log('→ RECOMMENDED: Keep EOA mode + deposit pUSD from MetaMask to Polymarket');
  }

  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
