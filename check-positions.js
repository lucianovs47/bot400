/**
 * check-positions.js
 *
 * Verifies bot trades by checking on-chain conditional token balances
 * AND querying Polymarket data-api for both EOA and proxy positions.
 *
 * Use this to confirm where your positions are after a trade.
 *
 * Usage: node check-positions.js
 */

const axios = require('axios');
const { ethers } = require('ethers');
const users = require('./src/store/users');

const POLYGON_RPCS = [
  'https://polygon-rpc.com',
  'https://polygon-bor-rpc.publicnode.com',
  'https://polygon.gateway.tenderly.co',
  'https://rpc.ankr.com/polygon',
  'https://polygon.llamarpc.com',
];

const PUSD_ADDRESS = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB';
const DATA_API     = 'https://data-api.polymarket.com';

const PUSD_ABI = [
  'function balanceOf(address) view returns (uint256)',
];

async function getProvider() {
  for (const rpc of POLYGON_RPCS) {
    try {
      const p = new ethers.JsonRpcProvider(rpc, 137, { staticNetwork: true, batchMaxCount: 1 });
      await p.getBlockNumber();
      return p;
    } catch (e) { /* next */ }
  }
  throw new Error('all RPCs failed');
}

async function fetchPositions(addr) {
  try {
    const url = `${DATA_API}/positions?user=${addr}&sizeThreshold=.01`;
    const resp = await axios.get(url, { timeout: 15000 });
    return Array.isArray(resp.data) ? resp.data : [];
  } catch (e) {
    if (e.response?.status === 404) return [];
    return [];
  }
}

async function main() {
  console.log('=== POSITION VERIFICATION ===\n');

  const userIds = users.listUsers();
  if (!userIds.length) { console.error('No users'); process.exit(1); }
  const keys = users.getKeys(userIds[0]);

  const eoa   = new ethers.Wallet(keys.polyPrivateKey).address;
  const proxy = keys.polyFunderAddress;

  console.log(`EOA:   ${eoa}`);
  console.log(`Proxy: ${proxy}\n`);

  const provider = await getProvider();
  const pusd = new ethers.Contract(PUSD_ADDRESS, PUSD_ABI, provider);

  // pUSD balances
  const eoaPusd   = await pusd.balanceOf(eoa);
  const proxyPusd = proxy ? await pusd.balanceOf(proxy) : 0n;
  const eoaMatic  = await provider.getBalance(eoa);

  console.log('=== ON-CHAIN BALANCES ===');
  console.log(`EOA pUSD:   ${ethers.formatUnits(eoaPusd, 6)}`);
  console.log(`EOA MATIC:  ${ethers.formatEther(eoaMatic)}`);
  if (proxy) console.log(`Proxy pUSD: ${ethers.formatUnits(proxyPusd, 6)}`);
  console.log('');

  // Positions from data-api
  console.log('=== POSITIONS (Polymarket data-api) ===\n');

  const eoaPositions = await fetchPositions(eoa);
  console.log(`EOA positions: ${eoaPositions.length}`);
  for (const p of eoaPositions) {
    const size  = parseFloat(p.size ?? p.shares ?? 0);
    const price = parseFloat(p.curPrice ?? p.currentPrice ?? p.price ?? 0);
    const title = (p.title || p.eventSlug || p.condition_id || '').slice(0, 60);
    console.log(`  • ${title}`);
    console.log(`    size=${size.toFixed(4)} price=${price.toFixed(4)} value=$${(size * price).toFixed(2)}`);
  }
  console.log('');

  if (proxy) {
    const proxyPositions = await fetchPositions(proxy);
    console.log(`Proxy positions: ${proxyPositions.length}`);
    for (const p of proxyPositions) {
      const size  = parseFloat(p.size ?? p.shares ?? 0);
      const price = parseFloat(p.curPrice ?? p.currentPrice ?? p.price ?? 0);
      const title = (p.title || p.eventSlug || p.condition_id || '').slice(0, 60);
      console.log(`  • ${title}`);
      console.log(`    size=${size.toFixed(4)} price=${price.toFixed(4)} value=$${(size * price).toFixed(2)}`);
    }
    console.log('');
  }

  // Summary
  console.log('=== SUMMARY ===');
  const totalEoaValue = eoaPositions.reduce((s, p) => {
    const size  = parseFloat(p.size ?? p.shares ?? 0);
    const price = parseFloat(p.curPrice ?? p.currentPrice ?? p.price ?? 0);
    return s + (size * price);
  }, 0);

  console.log(`EOA pUSD on-chain:        ${ethers.formatUnits(eoaPusd, 6)}`);
  console.log(`EOA position value:       $${totalEoaValue.toFixed(2)}`);
  console.log(`Total EOA capital:        $${(parseFloat(ethers.formatUnits(eoaPusd, 6)) + totalEoaValue).toFixed(2)}`);
  console.log('');
  console.log('Polymarket profile pages to check manually:');
  console.log(`  EOA:   https://polymarket.com/profile/${eoa}`);
  if (proxy) console.log(`  Proxy: https://polymarket.com/profile/${proxy}`);

  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
