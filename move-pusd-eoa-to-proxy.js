/**
 * move-pusd-eoa-to-proxy.js
 *
 * Move pUSD do EOA de volta para o proxy (Gnosis Safe).
 * Necessário após ter migrado temporariamente para modo EOA.
 *
 * Requer: MATIC no EOA para gas (~0.001 MATIC — simples ERC20 transfer).
 * Verifique o saldo MATIC do EOA antes de rodar.
 *
 * Usage: node move-pusd-eoa-to-proxy.js
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

const PUSD_ADDRESS = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB';
const PUSD_ABI = [
  'function balanceOf(address) view returns (uint256)',
  'function transfer(address to, uint256 amount) returns (bool)',
];

async function main() {
  console.log('=== MOVE pUSD EOA → PROXY ===\n');

  const userIds = users.listUsers();
  if (!userIds.length) { console.error('Sem usuários'); process.exit(1); }
  const keys = users.getKeys(userIds[0]);
  if (!keys.polyPrivateKey) { console.error('Falta polyPrivateKey'); process.exit(1); }

  const wallet = new ethers.Wallet(keys.polyPrivateKey);
  const eoa    = wallet.address;
  const proxy  = keys.polyFunderAddress;

  console.log(`EOA:   ${eoa}`);
  console.log(`Proxy: ${proxy}\n`);

  if (!proxy) { console.error('polyFunderAddress não configurado'); process.exit(1); }
  if (proxy.toLowerCase() === eoa.toLowerCase()) {
    console.error('EOA == Proxy — nada para mover');
    process.exit(1);
  }

  // Conecta RPC
  let provider = null;
  for (const rpc of POLYGON_RPCS) {
    try {
      const p = new ethers.JsonRpcProvider(rpc);
      await p.getBlockNumber();
      provider = p;
      console.log(`RPC: ${rpc}`);
      break;
    } catch (e) {
      console.log(`RPC falhou (${rpc.split('/')[2]}): ${e.message.slice(0, 50)}`);
    }
  }
  if (!provider) { console.error('Todos os RPCs falharam'); process.exit(1); }

  const signer = wallet.connect(provider);
  const pusd   = new ethers.Contract(PUSD_ADDRESS, PUSD_ABI, signer);

  const eoaBalance  = await pusd.balanceOf(eoa);
  const maticBal    = await provider.getBalance(eoa);

  console.log(`\nEOA pUSD:   ${ethers.formatUnits(eoaBalance, 6)}`);
  console.log(`EOA MATIC:  ${ethers.formatEther(maticBal)}`);

  if (eoaBalance === 0n) {
    console.log('\nEOA tem 0 pUSD — nada para mover.');
    process.exit(0);
  }

  if (maticBal < ethers.parseEther('0.001')) {
    console.error('\n❌ MATIC insuficiente para gas (precisa de ~0.001 MATIC).');
    console.error(`   EOA: ${eoa}`);
    console.error('   Envie MATIC para o EOA antes de continuar.');
    process.exit(1);
  }

  console.log(`\nTransferindo ${ethers.formatUnits(eoaBalance, 6)} pUSD → proxy...`);
  try {
    const tx = await pusd.transfer(proxy, eoaBalance);
    console.log(`TX: ${tx.hash}`);
    const receipt = await tx.wait();
    console.log(`Confirmado ✅  bloco: ${receipt.blockNumber}`);

    const eoaAfter  = await pusd.balanceOf(eoa);
    const proxyAfter = await pusd.balanceOf(proxy);
    console.log(`\nEOA pUSD depois:   ${ethers.formatUnits(eoaAfter, 6)}`);
    console.log(`Proxy pUSD depois: ${ethers.formatUnits(proxyAfter, 6)}`);
    console.log('\n✅ pUSD de volta no proxy — pode reiniciar o bot: pm2 restart sarbccode');
  } catch (e) {
    console.error(`transfer falhou: ${e.message}`);
    process.exit(1);
  }

  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
