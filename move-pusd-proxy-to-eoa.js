/**
 * move-pusd-proxy-to-eoa.js
 *
 * Moves pUSD from the V1 Polymarket proxy wallet to the EOA wallet,
 * then approves the CTF Exchange V2 to spend pUSD from the EOA.
 *
 * Why: V2 CTF Exchange does not support V1 proxy wallets.
 *      Bot uses EOA mode (sigType=0, maker=signer=EOA).
 *      EOA needs pUSD on-chain to fund orders.
 *
 * What this does:
 *   1. Read pUSD balance of proxy
 *   2. Call proxy.execute(pUSD, 0, transfer(eoa, balance))
 *      → pUSD moves from proxy to EOA
 *   3. EOA approves V2 CTF Exchange for MaxUint256 pUSD
 *   4. (Optional) EOA approves CTF conditional token contract
 *
 * Usage: node move-pusd-proxy-to-eoa.js
 *   Add --dry-run to simulate without sending transactions.
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
const CTF_CONTRACT    = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'; // Gnosis CTF

const ERC20_ABI = [
  'function balanceOf(address) view returns (uint256)',
  'function allowance(address owner, address spender) view returns (uint256)',
  'function approve(address spender, uint256 amount) returns (bool)',
  'function transfer(address to, uint256 amount) returns (bool)',
];

const CTF_ABI = [
  'function isApprovedForAll(address owner, address operator) view returns (bool)',
  'function setApprovalForAll(address operator, bool approved)',
];

// Polymarket proxy wallet ABI — owner can call execute() to run arbitrary txs
const PROXY_ABI = [
  'function execute(address destination, uint256 value, bytes data) external returns (bool)',
  // Some proxy versions use this signature:
  'function execute(address destination, uint256 value, bytes data) external returns (bytes)',
  'function owner() view returns (address)',
];

const DRY_RUN = process.argv.includes('--dry-run');

async function main() {
  console.log(`=== MOVE pUSD PROXY → EOA ${DRY_RUN ? '(DRY RUN)' : ''} ===\n`);

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
  const eoa   = wallet.address;
  const proxy = keys.polyFunderAddress;

  if (!proxy || proxy.toLowerCase() === eoa.toLowerCase()) {
    console.error('No proxy address configured — nothing to move');
    process.exit(1);
  }

  console.log(`EOA:   ${eoa}`);
  console.log(`Proxy: ${proxy}\n`);

  // ── Check proxy ownership ──────────────────────────────────────────────────
  try {
    const proxyContract = new ethers.Contract(proxy, PROXY_ABI, provider);
    const owner = await proxyContract.owner();
    if (owner.toLowerCase() !== eoa.toLowerCase()) {
      console.error(`⚠️  Proxy owner is ${owner}, not EOA ${eoa}`);
      console.error('Cannot execute from this EOA');
      process.exit(1);
    }
    console.log(`Proxy owner: ${owner} ✅`);
  } catch (e) {
    console.log(`owner() call failed: ${e.message.slice(0, 80)}`);
    console.log('Continuing anyway — owner() may not be exposed...\n');
  }

  // ── Read balances ──────────────────────────────────────────────────────────
  const pusd = new ethers.Contract(PUSD_ADDRESS, ERC20_ABI, wallet);
  const proxyBalance = await pusd.balanceOf(proxy);
  const eoaBalance   = await pusd.balanceOf(eoa);

  console.log(`Proxy pUSD: ${ethers.formatUnits(proxyBalance, 6)}`);
  console.log(`EOA   pUSD: ${ethers.formatUnits(eoaBalance,   6)}\n`);

  if (proxyBalance === 0n) {
    console.log('Proxy has 0 pUSD — nothing to move.');
  } else {
    // ── Step 1: proxy.execute → pUSD.transfer(eoa, balance) ────────────────
    console.log(`Step 1: Moving ${ethers.formatUnits(proxyBalance, 6)} pUSD from proxy to EOA`);

    const transferData = pusd.interface.encodeFunctionData('transfer', [eoa, proxyBalance]);
    const proxyWithSigner = new ethers.Contract(proxy, PROXY_ABI, wallet);

    if (DRY_RUN) {
      console.log(`[DRY RUN] proxy.execute(${PUSD_ADDRESS}, 0, transfer(${eoa}, ${proxyBalance}))`);
    } else {
      try {
        const gasEst = await proxyWithSigner.execute.estimateGas(PUSD_ADDRESS, 0n, transferData);
        console.log(`Gas estimate: ${gasEst}`);

        const tx = await proxyWithSigner.execute(PUSD_ADDRESS, 0n, transferData, {
          gasLimit: gasEst * 2n,
        });
        console.log(`TX: ${tx.hash}`);
        const receipt = await tx.wait();
        console.log(`Confirmed block ${receipt.blockNumber} ✅`);

        const newEoaBal = await pusd.balanceOf(eoa);
        console.log(`EOA pUSD after: ${ethers.formatUnits(newEoaBal, 6)}\n`);
      } catch (e) {
        console.error(`execute() failed: ${e.message}`);
        console.error('');
        console.error('Manual alternative:');
        console.error(`  In your MetaMask, go to Polygon network`);
        console.error(`  Send pUSD from ${proxy}`);
        console.error(`  to ${eoa}`);
        console.error(`  Amount: ${ethers.formatUnits(proxyBalance, 6)} pUSD`);
        process.exit(1);
      }
    }
  }

  // ── Step 2: EOA approves V2 Exchange ──────────────────────────────────────
  console.log('Step 2: EOA → V2 CTF Exchange pUSD allowance');
  const currentAllow = await pusd.allowance(eoa, CTF_EXCHANGE_V2);
  const threshold = ethers.parseUnits('1000000', 6);

  if (currentAllow >= threshold) {
    console.log(`Already approved (${ethers.formatUnits(currentAllow, 6)}) ✅`);
  } else {
    console.log(`Current allowance: ${ethers.formatUnits(currentAllow, 6)} — approving MaxUint256...`);
    if (DRY_RUN) {
      console.log('[DRY RUN] pUSD.approve(CTF_EXCHANGE_V2, MaxUint256)');
    } else {
      try {
        const tx = await pusd.approve(CTF_EXCHANGE_V2, ethers.MaxUint256);
        console.log(`TX: ${tx.hash}`);
        await tx.wait();
        console.log('pUSD approved for V2 ✅\n');
      } catch (e) {
        console.error(`approve() failed: ${e.message}`);
        process.exit(1);
      }
    }
  }

  // ── Step 3: EOA approves CTF conditional tokens ───────────────────────────
  console.log('Step 3: EOA → CTF conditional token approval');
  const ctf = new ethers.Contract(CTF_CONTRACT, CTF_ABI, wallet);
  const ctfApproved = await ctf.isApprovedForAll(eoa, CTF_EXCHANGE_V2);

  if (ctfApproved) {
    console.log('CTF already approved ✅');
  } else {
    console.log('Approving CTF for V2...');
    if (DRY_RUN) {
      console.log('[DRY RUN] ctf.setApprovalForAll(CTF_EXCHANGE_V2, true)');
    } else {
      try {
        const tx = await ctf.setApprovalForAll(CTF_EXCHANGE_V2, true);
        console.log(`TX: ${tx.hash}`);
        await tx.wait();
        console.log('CTF approved ✅');
      } catch (e) {
        console.error(`setApprovalForAll() failed: ${e.message}`);
      }
    }
  }

  // ── Final summary ──────────────────────────────────────────────────────────
  const finalEoaPusd = await pusd.balanceOf(eoa);
  const finalAllow   = await pusd.allowance(eoa, CTF_EXCHANGE_V2);
  const finalCtf     = await ctf.isApprovedForAll(eoa, CTF_EXCHANGE_V2);

  console.log('\n=== FINAL STATE ===');
  console.log(`EOA pUSD on-chain:        ${ethers.formatUnits(finalEoaPusd, 6)}`);
  console.log(`EOA → V2 allowance:       ${ethers.formatUnits(finalAllow, 6)}`);
  console.log(`EOA CTF approved:         ${finalCtf}`);

  if (finalEoaPusd > 0n && finalAllow >= finalEoaPusd && finalCtf) {
    console.log('\n✅ EOA fully funded and approved — bot EOA mode should work!');
    console.log('Run: pm2 restart sarbccode');
  } else {
    console.log('\n⚠️  Setup incomplete — check items above');
    if (finalEoaPusd === 0n) {
      console.log('   EOA has 0 pUSD — transfer from proxy failed or proxy was empty');
    }
    if (!finalCtf) {
      console.log('   CTF not approved — setApprovalForAll failed');
    }
  }

  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
