#!/usr/bin/env node
/**
 * Hedge analysis report — parses PM2 logs and summarizes hedge events.
 *
 * Usage:
 *   pm2 logs sarbccode --nostream --lines 100000 | node scripts/hedge-report.js
 *
 * Save logs first (recommended for big windows):
 *   pm2 logs sarbccode --nostream --lines 200000 > /tmp/bot-logs.txt
 *   node scripts/hedge-report.js < /tmp/bot-logs.txt
 */

const readline = require('readline');

const rl = readline.createInterface({ input: process.stdin, terminal: false });
const lines = [];
rl.on('line', l => lines.push(l));
rl.on('close', () => analyze(lines));

function parseTs(line) {
  const m = line.match(/(\d{4}-\d{2}-\d{2}) (\d{2}):(\d{2}):(\d{2})/);
  if (!m) return null;
  return {
    date: m[1],
    time: `${m[2]}:${m[3]}:${m[4]}`,
    hour: parseInt(m[2]),
    minute: parseInt(m[3]),
    roundSecond: (parseInt(m[3]) % 15) * 60 + parseInt(m[4]),
  };
}

/** Extract {asset, leg} from any line containing e.g. "ETH LegB". */
function extractAssetLeg(line) {
  const m = line.match(/\b(BTC|ETH|SOL|XRP)\s+Leg([AB])\b/);
  return m ? { asset: m[1], leg: m[2] } : null;
}

function pct(n, total) {
  return total ? (n / total * 100).toFixed(0) : '0';
}

function bar(n) {
  return '█'.repeat(Math.min(n, 40));
}

function analyze(lines) {
  const hedges = [];

  // Most recent context observed from [TRADE] or [EarlyExit] lines as we
  // scan forward.  Updated in the outer loop so that lines BEFORE the hedge
  // contribute to it (early-exit hedges have context right before them).
  let lastContext    = null;
  let lastContextIdx = -1;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // ── Update context from [TRADE] and [EarlyExit] lines ──
    if (line.includes('[TRADE]') || line.includes('[EarlyExit]')) {
      const ctx = extractAssetLeg(line);
      if (ctx) {
        const costM = line.match(/cost=\$([\d.]+)/);
        const roiM  = line.match(/roi=([\d.]+)%/);
        lastContext = {
          ...ctx,
          cost: costM ? parseFloat(costM[1]) : lastContext?.cost,
          roi:  roiM  ? parseFloat(roiM[1])  : lastContext?.roi,
          ts:   parseTs(line),
        };
        lastContextIdx = i;
      }
    }

    // ── Detect hedge start ──
    let hedgeType = null;
    if (line.includes('[HEDGE] Selling Polymarket')) hedgeType = 'POLY_FAILED';
    if (line.includes('[HEDGE] Selling Kalshi'))     hedgeType = 'KALSHI_FAILED';
    if (!hedgeType) continue;

    // ── Determine if this hedge was triggered by an early exit ──
    // Early exit hedges are always preceded (within 5 lines) by a
    // "[EarlyExit] *-only remainder" log from _executeEarlyExit.
    let fromEarlyExit = false;
    for (let k = Math.max(0, i - 5); k < i; k++) {
      if (lines[k].includes('[EarlyExit] Poly-only remainder') ||
          lines[k].includes('[EarlyExit] Kalshi-only remainder')) {
        fromEarlyExit = true;
        break;
      }
    }

    const ts = parseTs(line);
    const hedge = {
      type         : hedgeType,
      ts,
      asset        : lastContext?.asset || '?',
      leg          : lastContext?.leg   || '?',
      entryCost    : lastContext?.cost  || null,
      entryRoi     : lastContext?.roi   || null,
      errors       : [],
      bookTops     : [],
      rounds       : 0,
      success      : false,
      pnl          : null,
      fromEarlyExit,
    };

    // ── Inner loop: collect rounds / errors / book tops / outcome / pnl ──
    // We do NOT break on SUCCESS/FAILED here so that the loop runs to the
    // final summary line ("[HEDGE] SUCCESS — N attempt(s)").  That way we
    // keep the correct end-offset for the post-loop [TRADE] scan below.
    let innerEndIdx = i;
    for (let j = i + 1; j < Math.min(i + 150, lines.length); j++) {
      const l = lines[j];
      innerEndIdx = j;

      // Book state
      const bookM = l.match(/\[HEDGE\] (?:Poly|Kalshi) book top[^:]*:\s*\$([\d.]+)\s+x([\d.]+)\s+\((\d+) levels?\)/);
      if (bookM) {
        hedge.bookTops.push({
          price  : parseFloat(bookM[1]),
          size   : parseFloat(bookM[2]),
          levels : parseInt(bookM[3]),
        });
      }

      // Error classification
      if      (l.includes('no orders found to match'))                    hedge.errors.push('no_match');
      else if (l.includes('invalid amounts'))                             hedge.errors.push('invalid_amounts');
      else if (l.includes('balance reject'))                              hedge.errors.push('balance');
      else if (l.includes('not filled') || l.includes('no liquidity'))   hedge.errors.push('no_liquidity');
      else if (l.includes('FAK failed') || l.includes('FAK rejected') || l.includes('FAK FAILED')) {
        const known = ['no_match', 'invalid_amounts', 'balance', 'no_liquidity'];
        if (!hedge.errors.some(e => known.includes(e))) hedge.errors.push('fak_other');
      }

      // Round counter
      if (l.includes('[HEDGE] Poly FAK SELL:') || l.includes('[HEDGE] Kalshi IOC SELL:')) {
        hedge.rounds++;
      }

      // PnL from partial exit settled log
      const pnlM = l.match(/partial exit settled:.*pnl=\$(-?[\d.]+)/);
      if (pnlM) hedge.pnl = parseFloat(pnlM[1]);

      // Outcome
      if (l.includes('[HEDGE] SUCCESS — closed all') || l.includes('[HEDGE] DUST EXIT')) hedge.success = true;
      if (l.includes('[HEDGE] CRITICAL')) hedge.success = false;

      // The final summary line — hedge object is complete, stop inner scan
      if (l.match(/\[HEDGE\] (?:SUCCESS|FAILED) — \d+ attempt/)) break;

      // A new hedge starting — stop (we overread)
      if (l.includes('[HEDGE] Selling Polymarket') || l.includes('[HEDGE] Selling Kalshi')) {
        innerEndIdx = j - 1;
        break;
      }
    }

    // ── Post-loop: for ENTRY hedges, [TRADE] HEDGED appears AFTER the ──
    // hedge block.  Look ahead from innerEndIdx to capture the real
    // asset + leg (overrides the stale lastContext from the prior trade).
    // For early-exit hedges there is no new [TRADE] — skip.
    if (!fromEarlyExit) {
      for (let k = innerEndIdx + 1; k < Math.min(innerEndIdx + 25, lines.length); k++) {
        if (lines[k].includes('[TRADE]')) {
          const ctx = extractAssetLeg(lines[k]);
          if (ctx) {
            hedge.asset = ctx.asset;
            hedge.leg   = ctx.leg;
            const costM = lines[k].match(/cost=\$([\d.]+)/);
            const roiM  = lines[k].match(/roi=([\d.]+)%/);
            if (costM) hedge.entryCost = parseFloat(costM[1]);
            if (roiM)  hedge.entryRoi  = parseFloat(roiM[1]);
          }
          break;
        }
        // Stop if another hedge starts before we find the [TRADE]
        if (lines[k].includes('[HEDGE] Selling')) break;
      }
    }

    hedges.push(hedge);
  }

  // ── Output ──
  const sep  = '═'.repeat(54);
  const sep2 = '─'.repeat(54);

  if (hedges.length === 0) {
    console.log('\nNenhum hedge encontrado nos logs.\n');
    return;
  }

  const dateRange = [
    hedges[0]?.ts?.date,
    hedges[hedges.length - 1]?.ts?.date,
  ].filter(Boolean).join(' → ');

  console.log(`\n${sep}`);
  console.log(`  HEDGE REPORT  |  ${hedges.length} eventos  |  ${dateRange}`);
  console.log(`${sep}\n`);

  // ── Causa ──
  const polyFailed   = hedges.filter(h => h.type === 'POLY_FAILED');
  const kalshiFailed = hedges.filter(h => h.type === 'KALSHI_FAILED');
  console.log('CAUSA DO HEDGE:');
  console.log(`  Poly falhou   (Kalshi preencheu) : ${String(polyFailed.length).padStart(3)}  ${pct(polyFailed.length, hedges.length)}%`);
  console.log(`  Kalshi falhou (Poly preencheu)   : ${String(kalshiFailed.length).padStart(3)}  ${pct(kalshiFailed.length, hedges.length)}%`);
  console.log(`  De early exit parcial            : ${String(hedges.filter(h => h.fromEarlyExit).length).padStart(3)}`);

  // ── Por ativo ──
  const byAsset = {};
  hedges.forEach(h => { byAsset[h.asset] = (byAsset[h.asset] || 0) + 1; });
  console.log('\nPOR ATIVO:');
  Object.entries(byAsset).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => {
    console.log(`  ${k.padEnd(4)}: ${String(v).padStart(3)}  ${pct(v, hedges.length)}%  ${bar(v)}`);
  });

  // ── Por leg ──
  const byLeg = {};
  hedges.forEach(h => { byLeg[h.leg] = (byLeg[h.leg] || 0) + 1; });
  console.log('\nPOR LEG:');
  Object.entries(byLeg).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => {
    console.log(`  Leg${k}: ${String(v).padStart(3)}  ${pct(v, hedges.length)}%  ${bar(v)}`);
  });

  // ── Tipo de erro ──
  const byError = {};
  hedges.forEach(h => {
    const key = h.errors.length ? [...new Set(h.errors)].join('+') : 'sem_erro_capturado';
    byError[key] = (byError[key] || 0) + 1;
  });
  console.log('\nTIPO DE ERRO:');
  Object.entries(byError).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => {
    console.log(`  ${k.padEnd(26)}: ${String(v).padStart(3)}  ${pct(v, hedges.length)}%`);
  });

  // ── Por hora (UTC) ──
  const byHour = {};
  hedges.forEach(h => { if (h.ts?.hour != null) byHour[h.ts.hour] = (byHour[h.ts.hour] || 0) + 1; });
  if (Object.keys(byHour).length) {
    console.log('\nPOR HORA (UTC):');
    for (let h = 0; h < 24; h++) {
      const n = byHour[h] || 0;
      if (n > 0) console.log(`  ${String(h).padStart(2, '0')}h : ${String(n).padStart(3)}  ${bar(n)}`);
    }
  }

  // ── Por momento do round (0-900s) ──
  const byRoundBucket = { '0-3min': 0, '3-6min': 0, '6-9min': 0, '9-12min': 0, '12-15min': 0 };
  hedges.forEach(h => {
    const s = h.ts?.roundSecond;
    if (s == null) return;
    if (s < 180)      byRoundBucket['0-3min']++;
    else if (s < 360) byRoundBucket['3-6min']++;
    else if (s < 540) byRoundBucket['6-9min']++;
    else if (s < 720) byRoundBucket['9-12min']++;
    else              byRoundBucket['12-15min']++;
  });
  console.log('\nMOMENTO NO ROUND (quando o hedge disparou):');
  Object.entries(byRoundBucket).forEach(([k, v]) => {
    if (v > 0) console.log(`  ${k.padEnd(10)}: ${String(v).padStart(3)}  ${bar(v)}`);
  });

  // ── Book top Poly no início do hedge ──
  const polyTops = polyFailed.map(h => h.bookTops[0]?.price).filter(v => v != null);
  if (polyTops.length) {
    const buckets = [
      ['< $0.01',    v => v < 0.01],
      ['$0.01-0.05', v => v >= 0.01 && v < 0.05],
      ['$0.05-0.15', v => v >= 0.05 && v < 0.15],
      ['$0.15-0.30', v => v >= 0.15 && v < 0.30],
      ['> $0.30',    v => v >= 0.30],
    ];
    console.log(`\nBOOK TOP POLY no início do hedge (${polyTops.length} eventos):`);
    buckets.forEach(([label, fn]) => {
      const n = polyTops.filter(fn).length;
      if (n) console.log(`  ${label.padEnd(12)}: ${String(n).padStart(3)}  ${pct(n, polyTops.length)}%  ${bar(n)}`);
    });
    const avg = polyTops.reduce((s, v) => s + v, 0) / polyTops.length;
    console.log(`  média: $${avg.toFixed(4)}`);
  }

  // ── Rounds de tentativa ──
  const withRounds = hedges.filter(h => h.rounds > 0);
  if (withRounds.length) {
    const avg = withRounds.reduce((s, h) => s + h.rounds, 0) / withRounds.length;
    const byRound = {};
    withRounds.forEach(h => { byRound[h.rounds] = (byRound[h.rounds] || 0) + 1; });
    console.log(`\nROUNDS DE TENTATIVA (${withRounds.length}/${hedges.length} com dados, média: ${avg.toFixed(1)}):`);
    Object.keys(byRound).sort((a, b) => a - b).forEach(r => {
      console.log(`  ${r} round(s): ${String(byRound[r]).padStart(3)}  ${bar(byRound[r])}`);
    });
  } else {
    console.log('\nROUNDS DE TENTATIVA: nenhum capturado');
  }

  // ── P&L ──
  const withPnl = hedges.filter(h => h.pnl != null);
  if (withPnl.length) {
    const total = withPnl.reduce((s, h) => s + h.pnl, 0);
    const avg   = total / withPnl.length;
    const worst = Math.min(...withPnl.map(h => h.pnl));
    console.log(`\nP&L DOS HEDGES (${withPnl.length}/${hedges.length} com dados):`);
    console.log(`  Total  : $${total.toFixed(2)}`);
    console.log(`  Média  : $${avg.toFixed(2)} por hedge`);
    console.log(`  Pior   : $${worst.toFixed(2)}`);
  }

  // ── Detalhes individuais (últimos 30) ──
  console.log(`\n${sep2}`);
  console.log(`ÚLTIMOS ${Math.min(30, hedges.length)} HEDGES (mais recentes primeiro):`);
  console.log(`${sep2}`);
  hedges.slice(-30).reverse().forEach((h) => {
    const err    = h.errors.length ? [...new Set(h.errors)].join('+') : 'ok';
    const top    = h.bookTops[0] ? `book=$${h.bookTops[0].price.toFixed(3)}x${h.bookTops[0].levels}lv` : '';
    const pnlStr = h.pnl != null ? ` pnl=$${h.pnl.toFixed(2)}` : '';
    const roi    = h.entryRoi != null ? ` roi=${h.entryRoi.toFixed(1)}%` : '';
    const eeTag  = h.fromEarlyExit ? ' [EE]' : '';
    console.log(
      `  ${h.ts?.time || '??:??:??'} ${h.asset} Leg${h.leg} ` +
      `[${h.type === 'POLY_FAILED' ? 'POLY_FAIL' : 'KALS_FAIL'}]${eeTag} ` +
      `${err} ${top}${roi}${pnlStr} ` +
      `${h.success ? '✓' : '✗'} ${h.rounds}rnd`,
    );
  });

  console.log(`\n${sep}\n`);
}
