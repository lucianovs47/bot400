/**
 * pUSD/USDC Approval — ensures the Polymarket CTF Exchange V2 contracts are
 * approved to spend pUSD from the user's wallet.
 *
 * After April 28, 2026 upgrade:
 *   - Collateral is pUSD (0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB)
 *   - Exchange is CTF Exchange V2 (0xE111180000d2663C0091e4f400237545B87B996B)
 *
 * For proxy wallets, Polymarket auto-migrates approvals during upgrade.
 * For EOA wallets, we approve pUSD for the V2 exchange.
 */

const { ethers } = require('ethers');

// Multiple RPCs — same list as capitalGuard.js
const POLYGON_RPCS = [
  'https://polygon-rpc.com',
  'https://polygon-bor-rpc.publicnode.com',
  'https://polygon.gateway.tenderly.co',
  'https://rpc.ankr.com/polygon',
  'https://polygon.llamarpc.com',
];

// V2 contracts on Polygon (live April 28, 2026)
const PUSD_ADDRESS       = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB'; // pUSD (replaces USDC.e)
const CTF_EXCHANGE_V2    = '0xE111180000d2663C0091e4f400237545B87B996B'; // CTF Exchange V2
const CTF_CONTRACT       = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'; // Gnosis CTF (unchanged)

// V1 legacy (kept for reference — no longer approved)
const USDC_ADDRESS        = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174';
const NEG_RISK_CTF_EXCHANGE = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E';
const NEG_RISK_ADAPTER    = '0xC5d563A36AE78145C45a50134d48A1215220f80a';

const USDC_ABI = [
  'function allowance(address owner, address spender) view returns (uint256)',
  'function approve(address spender, uint256 amount) returns (bool)',
];

const CTF_ABI = [
  'function isApprovedForAll(address owner, address operator) view returns (bool)',
  'function setApprovalForAll(address operator, bool approved)',
];

const MAX_UINT256 = ethers.MaxUint256;

/**
 * Check and approve USDC + CTF spending for Polymarket trading.
 *
 * @param {string} privateKey - Signer wallet private key
 * @param {string} funderAddress - Proxy wallet address (if different from signer)
 * @returns {{ usdc: boolean, ctf: boolean, isProxy: boolean }}
 */
async function checkAndApprove(privateKey, funderAddress) {
  let provider = null;
  for (const rpc of POLYGON_RPCS) {
    try {
      const p = new ethers.JsonRpcProvider(rpc);
      await p.getBlockNumber();
      provider = p;
      break;
    } catch (e) { /* try next */ }
  }
  if (!provider) throw new Error('All Polygon RPCs unreachable');

  const wallet = new ethers.Wallet(privateKey, provider);
  const signerAddr = wallet.address;
  const isProxy = funderAddress && funderAddress.toLowerCase() !== signerAddr.toLowerCase();

  // For proxy wallets, Polymarket handles approvals — we just check
  // For EOA wallets, we need to approve ourselves
  const checkAddr = isProxy ? funderAddress : signerAddr;

  const pusd = new ethers.Contract(PUSD_ADDRESS, USDC_ABI, wallet);

  const result = { usdc: false, ctf: false, isProxy };

  try {
    // Check pUSD allowance for CTF Exchange V2
    const allowance = await pusd.allowance(checkAddr, CTF_EXCHANGE_V2);
    const threshold = ethers.parseUnits('1000000', 6); // 1M pUSD threshold

    if (allowance < threshold) {
      if (isProxy) {
        console.log('[Approval] proxy wallet pUSD allowance low — Polymarket auto-migrates proxy approvals');
        result.usdc = false;
      } else {
        console.log('[Approval] approving pUSD for CTF Exchange V2...');
        const tx = await pusd.approve(CTF_EXCHANGE_V2, MAX_UINT256);
        await tx.wait();
        console.log('[Approval] pUSD approved for V2 — tx:', tx.hash);
        result.usdc = true;
      }
    } else {
      console.log('[Approval] pUSD already approved for CTF Exchange V2');
      result.usdc = true;
    }

    // Check CTF approval (conditional tokens) for CTF Exchange V2
    const ctf = new ethers.Contract(CTF_CONTRACT, CTF_ABI, wallet);
    const isApproved = await ctf.isApprovedForAll(checkAddr, CTF_EXCHANGE_V2);

    if (!isApproved) {
      if (isProxy) {
        console.log('[Approval] proxy wallet CTF not approved for V2 — Polymarket should handle this');
        result.ctf = false;
      } else {
        console.log('[Approval] approving CTF for CTF Exchange V2...');
        const tx = await ctf.setApprovalForAll(CTF_EXCHANGE_V2, true);
        await tx.wait();
        console.log('[Approval] CTF approved for V2 — tx:', tx.hash);
        result.ctf = true;
      }
    } else {
      console.log('[Approval] CTF already approved for CTF Exchange V2');
      result.ctf = true;
    }
  } catch (err) {
    console.error('[USDC Approval] error:', err.message);
  }

  return result;
}

module.exports = { checkAndApprove };
