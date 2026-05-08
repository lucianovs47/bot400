/**
 * approve-eoa-for-v2.js
 *
 * Approves CTF Exchange V2 to spend pUSD + conditional tokens from the EOA.
 * Run this AFTER move-pusd-via-relayer.js puts pUSD in the EOA.
 *
 * Requires: ~0.05 MATIC in EOA for gas (Polygon).
 * Get MATIC: send from another wallet, or buy on any exchange → withdraw to Polygon.
 *
 * Usage: node approve-eoa-for-v2.js
 */

const { ethers } = require('ethers');
const users = require('./src/store/users');

const POLYGON_RPCS = [
  'https://polygon-rpc.com',
  'https://polygon-bor-rpc.publicnode.com',
  'https://polygon.gateway.tenderly.co',
  'https://rpc.ankr.com/polygon',
  'https://polygon.llamarpc.com',
];

const PUSD_ADDRESS    = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB';
const CTF_EXCHANGE_V2 = '0xE111180000d2663C0091e4f400237545B87B996B';
const CTF_CONTRACT    = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045';

const ERC20_ABI = [
  'function balanceOf(address) view returns (uint256)',
  'function allowance(address owner, address spender) view returns (uint256)',
  'function approve(address spender, uint256 amount) returns (bool)',
];

const CTF_ABI = [
  'function isApprovedForAll(address owner, address operator) view returns (bool)',
  'function setApprovalForAll(address operator, bool approved)',
];

async function main() {
  console.log('=== APPROVE EOA FOR V2 CTF EXCHANGE ===\n');

  const userIds = users.listUsers();
  if (!userIds.length) { console.error('No users'); process.exit(1); }
  const keys = users.getKeys(userIds[0]);
  if (!keys.polyPrivateKey) { console.error('Missing polyPrivateKey'); process.exit(1); }

  let provider = null;
  for (const rpc of POLYGON_RPCS) {
    try {
      const p = new ethers.JsonRpcProvider(rpc);
      await p.getBlockNumber();
      provider = p;
      console.log(`RPC: ${rpc}`);
      break;
    } catch (e) {
      console.log(`RPC failed (${rpc.split('/')[2]}): ${e.message.slice(0, 50)}`);
    }
  }
  if (!provider) { console.error('All RPCs failed'); process.exit(1); }

  const wallet = new ethers.Wallet(keys.polyPrivateKey, provider);
  const eoa = wallet.address;
  console.log(`EOA: ${eoa}\n`);

  // Check MATIC balance
  const maticBalance = await provider.getBalance(eoa);
  console.log(`MATIC balance: ${ethers.formatEther(maticBalance)}`);
  if (maticBalance < ethers.parseEther('0.01')) {
    console.error('\n❌ Not enough MATIC for gas (need ~0.05 MATIC)');
    console.error('Options:');
    console.error('  1. Send MATIC from another wallet to: ' + eoa);
    console.error('  2. Buy MATIC on any exchange → withdraw to Polygon network → send to above address');
    console.error('  3. Use a Polygon MATIC faucet (for small amounts)');
    process.exit(1);
  }

  const pusd = new ethers.Contract(PUSD_ADDRESS, ERC20_ABI, wallet);
  const ctf  = new ethers.Contract(CTF_CONTRACT, CTF_ABI, wallet);

  const eoaPusd      = await pusd.balanceOf(eoa);
  const currentAllow = await pusd.allowance(eoa, CTF_EXCHANGE_V2);
  const ctfApproved  = await ctf.isApprovedForAll(eoa, CTF_EXCHANGE_V2);

  console.log(`EOA pUSD:           ${ethers.formatUnits(eoaPusd, 6)}`);
  console.log(`pUSD → V2 allow:    ${ethers.formatUnits(currentAllow, 6)}`);
  console.log(`CTF approved:       ${ctfApproved}\n`);

  if (eoaPusd === 0n) {
    console.error('❌ EOA has 0 pUSD — run move-pusd-via-relayer.js first');
    process.exit(1);
  }

  // Step 1: pUSD approve
  const threshold = ethers.parseUnits('1000', 6); // 1000 pUSD threshold
  if (currentAllow >= threshold) {
    console.log('pUSD already approved for V2 ✅');
  } else {
    console.log('Approving pUSD for CTF Exchange V2...');
    try {
      const tx = await pusd.approve(CTF_EXCHANGE_V2, ethers.MaxUint256);
      console.log(`TX: ${tx.hash}`);
      await tx.wait();
      console.log('pUSD approved ✅');
    } catch (e) {
      console.error(`approve failed: ${e.message}`);
      process.exit(1);
    }
  }

  // Step 2: CTF setApprovalForAll
  if (ctfApproved) {
    console.log('CTF already approved ✅');
  } else {
    console.log('Approving CTF conditional tokens for V2...');
    try {
      const tx = await ctf.setApprovalForAll(CTF_EXCHANGE_V2, true);
      console.log(`TX: ${tx.hash}`);
      await tx.wait();
      console.log('CTF approved ✅');
    } catch (e) {
      console.error(`setApprovalForAll failed: ${e.message}`);
      process.exit(1);
    }
  }

  console.log('\n=== DONE ===');
  console.log(`EOA pUSD: ${ethers.formatUnits(await pusd.balanceOf(eoa), 6)}`);
  console.log('✅ EOA fully approved — restart bot: pm2 restart sarbccode');

  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
