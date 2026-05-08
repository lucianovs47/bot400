/**
 * move-pusd-via-relayer.js
 *
 * Uses the Polymarket Relayer (same as AutoRedeemer) to transfer pUSD
 * from the proxy (Gnosis Safe) to the EOA wallet — GASLESS.
 *
 * After this, the EOA has pUSD on-chain and the bot's EOA mode works.
 * The EOA will still need a tiny bit of MATIC (~0.05) to run approve-eoa-for-v2.js.
 *
 * Usage: node move-pusd-via-relayer.js
 */

const { createWalletClient, http, encodeFunctionData } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { polygon } = require('viem/chains');
const { ethers } = require('ethers');
const users = require('./src/store/users');

const POLYGON_RPCS = [
  'https://polygon-rpc.com',
  'https://polygon-bor-rpc.publicnode.com',
  'https://polygon.gateway.tenderly.co',
  'https://rpc.ankr.com/polygon',
  'https://polygon.llamarpc.com',
];

const RELAYER_URL  = 'https://relayer-v2.polymarket.com/';
const PUSD_ADDRESS = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB';

const PUSD_ABI = [
  { name: 'balanceOf', type: 'function', inputs: [{ name: 'account', type: 'address' }], outputs: [{ type: 'uint256' }], stateMutability: 'view' },
  { name: 'transfer',  type: 'function', inputs: [{ name: 'to', type: 'address' }, { name: 'amount', type: 'uint256' }], outputs: [{ type: 'bool' }] },
];

const ERC20_ETHERS_ABI = [
  'function balanceOf(address) view returns (uint256)',
];

async function main() {
  console.log('=== MOVE pUSD PROXY → EOA (via Polymarket Relayer) ===\n');

  const userIds = users.listUsers();
  if (!userIds.length) { console.error('No users'); process.exit(1); }
  const keys = users.getKeys(userIds[0]);
  if (!keys.polyPrivateKey) { console.error('Missing polyPrivateKey'); process.exit(1); }

  const eoa   = new ethers.Wallet(keys.polyPrivateKey).address;
  const proxy = keys.polyFunderAddress;

  console.log(`EOA:   ${eoa}`);
  console.log(`Proxy: ${proxy}\n`);

  if (!proxy || proxy.toLowerCase() === eoa.toLowerCase()) {
    console.error('No proxy address — nothing to move');
    process.exit(1);
  }

  // Connect to Polygon RPC for balance reads
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

  // Check proxy pUSD balance
  const pusd = new ethers.Contract(PUSD_ADDRESS, ERC20_ETHERS_ABI, provider);
  const proxyBalance = await pusd.balanceOf(proxy);
  const eoaBalance   = await pusd.balanceOf(eoa);

  console.log(`\nProxy pUSD: ${ethers.formatUnits(proxyBalance, 6)}`);
  console.log(`EOA   pUSD: ${ethers.formatUnits(eoaBalance, 6)}\n`);

  if (proxyBalance === 0n) {
    console.log('Proxy has 0 pUSD — nothing to transfer.');
    process.exit(0);
  }

  // Load BuilderConfig (ESM module — same as autoRedeemer)
  let BuilderConfig;
  try {
    const mod = await import('@polymarket/builder-signing-sdk');
    BuilderConfig = mod.BuilderConfig;
    console.log('BuilderConfig loaded ✅');
  } catch (e) {
    console.error(`Failed to load builder-signing-sdk: ${e.message}`);
    console.error('Make sure @polymarket/builder-signing-sdk is installed (dependency of builder-relayer-client)');
    process.exit(1);
  }

  // Get Builder credentials from user store
  const builderCreds = {
    key: keys.polyApiKey,
    secret: keys.polyApiSecret,
    passphrase: keys.polyPassphrase,
  };

  if (!builderCreds.key || !builderCreds.secret || !builderCreds.passphrase) {
    console.error('Missing Builder credentials (polyApiKey/polyApiSecret/polyPassphrase) in user store');
    process.exit(1);
  }
  console.log(`Builder key: ${builderCreds.key.slice(0, 8)}...`);

  // Build viem wallet (for signing)
  const pk = keys.polyPrivateKey.startsWith('0x') ? keys.polyPrivateKey : `0x${keys.polyPrivateKey}`;
  const account = privateKeyToAccount(pk);
  const viemWallet = createWalletClient({ account, chain: polygon, transport: http() });

  // Create RelayClient (SAFE type — exactly as autoRedeemer does)
  const { RelayClient, RelayerTxType } = require('@polymarket/builder-relayer-client');
  const builderConfig = new BuilderConfig({ localBuilderCreds: builderCreds });
  const client = new RelayClient(RELAYER_URL, 137, viemWallet, builderConfig, RelayerTxType.SAFE);

  // Encode pUSD.transfer(eoa, proxyBalance) calldata
  const transferCalldata = encodeFunctionData({
    abi: PUSD_ABI,
    functionName: 'transfer',
    args: [eoa, proxyBalance],
  });

  const txns = [{ to: PUSD_ADDRESS, data: transferCalldata, value: '0' }];

  console.log(`\nSending ${ethers.formatUnits(proxyBalance, 6)} pUSD from proxy to EOA via relayer...`);

  try {
    const txResponse = await client.execute(txns, 'Transfer pUSD to EOA');
    console.log('Relayer accepted tx ✅');
    console.log('Waiting for on-chain confirmation...');

    const confirmed = await Promise.race([
      txResponse.wait(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout 60s')), 60_000)),
    ]);

    const txHash = confirmed?.transactionHash || txResponse?.transactionHash || 'submitted';
    console.log(`Confirmed ✅  tx: ${txHash}`);

    // Verify new balance
    const newEoaBal = await pusd.balanceOf(eoa);
    console.log(`\nEOA pUSD after: ${ethers.formatUnits(newEoaBal, 6)}`);

    if (newEoaBal > 0n) {
      console.log('\n✅ pUSD moved to EOA successfully!');
      console.log('\nNext step: approve CTF Exchange V2 to spend pUSD from EOA.');
      console.log('This requires a small amount of MATIC (~0.05 MATIC) for gas.');
      console.log('Run: node approve-eoa-for-v2.js');
    }

  } catch (e) {
    console.error(`Relayer failed: ${e.message}`);
    console.error('');
    console.error('Possible reasons:');
    console.error('  - Builder credentials are expired/invalid (polyApiKey in user store)');
    console.error('  - Relayer rejected the pUSD.transfer call (only allows CTF operations)');
    console.error('');
    console.error('Alternative: use the Polymarket UI to withdraw pUSD to MetaMask,');
    console.error('then re-deposit from MetaMask on Polymarket V2.');
    process.exit(1);
  }

  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
