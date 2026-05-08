# SARBCCODE — Contexto para Claude Code

## Quem é o usuário

WLKR. Desenvolvendo um bot de arbitragem HFT cross-platform. Leigo em programação mas aprende rápido — explicar conceitos de forma detalhada e clara, nunca condescendente.

## Como se comportar

- Português BR, descontraído mas técnico
- Você é um dev senior contratado. Opiniões técnicas reais, sugestões proativas, desafie quando ele estiver errado
- NUNCA concorde só para agradar. Se algo não é possível, diga e explique por quê
- NUNCA presuma ou adivinhe. Sem certeza → "preciso verificar no código". Respostas baseadas em dados concretos, nunca "acho que talvez"
- SEMPRE ler o código antes de sugerir mudanças. Nunca resoluções precipitadas
- Prioridade: segurança do capital > correção > performance > legibilidade
- Pensar em todas as possibilidades e edge cases antes de implementar
- Trazer ideias de melhoria e otimização proativamente
- Exemplos com números reais do bot (preços, fees, quantidades)
- Objetivo e focado. Sem enrolação, sem disclaimers genéricos
- Sempre que uma alteração impactar informações deste CLAUDE.md (versão de API, fórmulas, endpoints, fluxos, bugs corrigidos, plano), fornecer o trecho atualizado. Formato: "ATUALIZAR CLAUDE.md: [o que mudou]" seguido do bloco

---

## Arquitetura Geral

### O que é
Bot de arbitragem HFT cross-platform entre Kalshi (exchange centralizada) e Polymarket (CLOB descentralizado, Polygon). Mercados binários de crypto 15 minutos (Up/Down). Ativos: BTC, ETH. Stack: Node.js, Express, Socket.io, PM2. VPS Linux.

### Lógica de arbitragem
Se custo combinado de lados opostos < $1.00 → comprar ambos garante lucro (um lado sempre paga $1.00).
- Leg A: Kalshi YES + Poly DOWN
- Leg B: Kalshi NO + Poly UP
- ROI = (1.00 - totalCost - worstCaseFee) / totalCost

### Fees
- Kalshi: taker fee parabólica → `0.07 × price × (1-price) × C`. Pico em P=0.5 (1.75¢/contrato). Mesma fórmula em BUY e SELL (taker IOC). NÃO é (1-price)×0.07 — esse era o bug corrigido em 67a6942
- Polymarket: fee deduzida em tokens no fill (não em USDC). Fórmula do CTF exchange contract:
  - BUY: `fee_tokens = feeRateBps/10000 × min(P, 1-P) / P × shares`
  - SELL: 0% taker fee confirmado (docs)
  - Empírico (commit 1899c8d): polyFee=0.04 calibrado contra fills reais → ~3.7% perda em P=0.48, ~2.1% em P=0.7
  - feeRateBps assinado=1000 mas fee real efetivo é menor (suspeita: bug #18 — deveria consultar /fee-rate por mercado)

### Boot (src/index.js)
connectors(Kalshi+Poly) → capitalGuard → decisionEngine → dispatcher → monitor(start) → autoRedeemer → server

### Rounds de 15 minutos
- Window: `Math.floor(Date.now()/1000/900)*900`
- Fim: window + 900
- Rotação: limpa posições, reseta locks, redescobre mercados, reseta hedge counters e strikeGuard

---

## Estrutura de Arquivos

```
src/
├── index.js                      # Boot sequence
├── config/
│   ├── index.js                  # .env config (fees, URLs, tradeMode)
│   └── assets.js                 # BTC/ETH/SOL/XRP com seriesTicker e slugPrefix
├── connectors/
│   ├── kalshi.js (1002 linhas)   # REST+WS, IOC orders, markets cache
│   └── polymarket.js (1101)      # REST+WS CLOB, FAK orders, EIP-712
├── engine/
│   ├── decisionEngine.js (318)   # Scan arb, ROI, signal generation
│   ├── capitalGuard.js (331)     # Balance, sizing, PnL tracking
│   └── monitor.js (650)          # Event-driven WS→scan→dispatch, rounds, hedge retry
├── execution/
│   └── dispatcher.js (1605)      # Core: execute, early exit, strike guard, hedge, PnL
├── services/
│   ├── autoRedeemer.js (415)     # Redeem tokens on-chain (Polygon)
│   ├── rtdsClient.js             # Preços real-time crypto
│   ├── telegram.js               # Notificações
│   └── usdcApproval.js           # USDC approval para Poly
├── api/
│   └── server.js (282)           # Dashboard Express+Socket.io
├── store/
│   └── users.js                  # User keys/settings
└── public/
    └── js/app.js                 # Dashboard frontend
```

---

## Connectors

### Kalshi (src/connectors/kalshi.js)
- API: v2 (`/trade-api/v2`) — versão atual
- WS: `wss://api.elections.kalshi.com/trade-api/ws/v2`
- Auth: API Key + Private Key PEM (RSA signing)
- Ordens: IOC (Immediate or Cancel), latência ~50-100ms
- Response: `{ order: { order_id, status, fill_count_fp, remaining_count_fp } }`
- `_actualFilled`: extraído de `fill_count_fp` (quantidade). NÃO retorna preço de fill
- Markets cache: `Map<ticker, { yesAsk, noAsk, yesBid, noBid }>`
- Matchable depth: BUY NO@55c → match YES orders >= 45c (complemento binário)

### Polymarket (src/connectors/polymarket.js)
- CLOB: v1 (endpoint `POST /order`) — EXISTE v2, migração pendente
- SDK: `@polymarket/clob-client@^5.8.1`
- WS: `wss://ws-subscriptions-clob.polymarket.com/ws/market` (v1)
- Gamma API: `https://gamma-api.polymarket.com` (discovery de mercados)
- Auth: EIP-712 signing (wallet) + L2 HMAC headers
- Ordens: FAK (Fill and Kill), latência ~370-460ms
- Fee rate: hardcoded 1000 bps — deveria consultar `/fee-rate` por mercado (bug #18)
- Response BUY: `{ takingAmount: "tokens", makingAmount: "USDC pago" }`
- Response SELL: `{ takingAmount: "USDC recebido", makingAmount: "tokens vendidos" }`
- takingAmount do SELL = USDC real. Usar para PnL, NUNCA estimar com bid*qty
- WS events: `book` (snapshot), `price_change` (tick)
- FAK sweep: varre book inteiro (price $0.01), fill medio pode diferir muito do best bid

---

## Execution (Dispatcher)

### Fluxo execute() (src/execution/dispatcher.js)
1. Gates: StrikeGuard → CapitalGuard → busy check
2. Fetch live books: WS fast-path ~20ms ou REST fallback
3. Validate live: recheck ROI com precos reais
4. Dynamic entry: qty baseado em depth (Poly multi-level ate 10c, buffer=5; Kalshi matchable)
5. Dynamic improvement: ROI buffer → melhora FAK price
6. Parallel: `Promise.allSettled([kalshi IOC, poly FAK])`
7. Outcomes: OK/OK → success → _openPositions, mismatch → hedge, FAIL → record

### Early Exit (_checkEarlyExits)
- Event-driven por WS tick
- Lê bids WS cache ambos lados
- Cost correto: `(pos.kalshiPrice + (1 - pos.kalshiPrice) * pos.kalshiFee) * pos.qty`
- NUNCA: `pos.kalshiPrice * (1 + pos.kalshiFee)` — ERRADO, já corrigido
- Poly revenue na decisão: book walk real via `_simulatePolyFillRevenue(tokenId, qty)` que percorre os 5 níveis de bid do WS somando price*size. Aplica haircut de 15% (× 0.85) sobre o resultado para compensar defasagem WS ~370ms (1899c8d). SELL fee = 0% (zero buffer artificial)
- Threshold: `netProfit > max($0.05, guaranteedSettlement × 0.70)` — alvo é capturar ~70% do payout garantido (2df2630)
- Saída paralela (Promise.allSettled)
- Balance Poly: refresh background a cada 60s. Drift > 1% gera warning

### PnL no exit (_executeEarlyExit)
- Poly revenue: `takingAmount` real do CLOB (NUNCA polyBid * polySold)
- Kalshi revenue: `kalshiBid * kalshiSold - kalshiBid * kalshiFee * kalshiSold`
- Log: `P:SELL qty@fillPrice(bid=wsBid) recv=$takingAmt`

### PnL na entrada (_recordTrade)
- Trades FILLED: registra pnl=0 no capitalGuard (real vem no exit)
- Trades HEDGED/FAILED: registra pnl real imediatamente

### Strike Guard
- Ultimos 10s do round: `kalshiBid + polyBid < $0.15` → exit emergencial
- `_strikeGuardBlocked = true` → bloqueia até rotação

### Emergency Hedge
- Se só uma perna preenche → vende imediatamente
- Tentativas multiplas com fallback de preço

---

## Engine

### Monitor (src/engine/monitor.js)
- Event-driven: WS ticks → `_runScan` → `decisionEngine.scan()` → dispatch melhor opp
- 1 trade por ativo por round (`_executedThisRound`)
- Retry: 5s cooldown após falha, 30s cooldown após hedge
- Hedge retry: até 5 por ativo/round, lock permanente no 5o
- Rotação: limpa tudo, redescobre, resubscreve WS

### DecisionEngine (src/engine/decisionEngine.js)
- `scan()`: avalia Leg A e Leg B para cada par de mercados
- Fee model (atualizado 1899c8d):
  - `kalshiEntryFee = K_price × (1-K_price) × 0.07` (parabólica, pago no BUY)
  - `polyTokenLoss = polyFee × min(P, 1-P) / P` (perda em tokens no settlement Poly)
  - `worstFee = kalshiEntryFee + polyTokenLoss` (additive — ambos aplicam quando Poly vence)
  - `netProfit = grossSpread - worstFee`
- ROI: `netProfit / totalCost × 100`
- Filtra: asks fora de 0.10-0.90, strikes divergentes > $5
- Signal propaga: sizing, kalshiFee, polyFee, minRoiPct ao dispatcher

### CapitalGuard (src/engine/capitalGuard.js)
- Background refresh: balances cada 60s (non-blocking)
- `canTrade()`: sync read cache, verifica stop thresholds
- `recordTrade({ pnl })`: soma totalPnl, incrementa tradeCount
- totalPnl: NÃO captura settlement (só early exit e hedge)

---

## Services

### AutoRedeemer (src/services/autoRedeemer.js)
- Background 24/7, intervalo 60s. Timeout 60s no wait(). Overlap guard

### RtdsClient (src/services/rtdsClient.js)
- WS preços real-time crypto. Usado para capturar strikes Polymarket

### Server (src/api/server.js)
- Express + Socket.io dashboard. Bugs pendentes: sem auth (#9), sem rate limit (#10)

---

## Regras de Desenvolvimento

1. **Branch**: `claude/debug-latency-logging`. NUNCA outro sem confirmação
2. **Rollback**: Tag `rollback-before-<nome>` ANTES de qualquer mudança
3. **Ler antes**: SEMPRE ler código antes de editar
4. **Explicar**: O que, como, por quê, impacto, riscos
5. **WS only**: Bids/asks para decisões sempre do WS cache, nunca REST
6. **Validar**: `node -c <arquivo>` após edição
7. **Diff**: Verificar diff completo antes de commit
8. **Commits**: Mensagens descritivas + link sessão
9. **Idioma**: Português BR
10. **Sem presunção**: Nunca presumir APIs — verificar código ou docs

---

## Plano de Execução

### Branch: `claude/debug-latency-logging`
### Ultimo commit: e2b8ae4
### Rollback: `git reset --hard rollback-before-pnl-fix`

### CRITICO
| # | O que | Status |
|---|-------|--------|
| 1 | Remover código proxy | ✅ |
| 2 | Entry size dinâmico | ✅ |
| 3 | FAK Poly partial fill + P&L hedge real | ✅ |
| 4 | Lock per-asset | ✅ |
| 5 | P&L com fees reais | ✅ |
| 6 | JsonRpcProvider leak | ✅ |
| 6.1 | Bug #3: Slippage buffer na decisão de early exit | ✅ |
| 6.2 | Fee Kalshi parabólica (era linear errada) | ✅ |
| 6.3 | Book walk real do Poly no early exit | ✅ |
| 6.4 | Refresh periódico _polyBalance | ✅ |
| 6.5 | Improvement revert + fee model + book walk haircut | ✅ |

### ALTO
| # | O que | Status |
|---|-------|--------|
| 7 | Estresse de longo uptime | ✅ |
| 8 | Early exit | ✅ monitorando |
| 9 | Socket.io sem auth + CORS * | pendente |
| 10 | Rate limiting ausente | pendente |
| 11 | USDC approval não atômico | pendente |
| 12 | DASHBOARD_SECRET migration | pendente |
| 13 | deriveApiKey loga credenciais | pendente |

### MEDIO
| # | O que | Status |
|---|-------|--------|
| 14 | Nearest-miss diagnostic log | pendente |
| 15 | [TRADE] units errado | pendente |
| 16 | First-user-only filters | pendente |
| 17 | O(n) scan no book update | pendente |
| 18 | Fee rate hardcoded 1000 bps | pendente |
| 19 | loadUserKeys recusa reload | pendente |
| 20 | Trade ID colisão | pendente |
| 21 | _logTimers cresce infinito | ✅ |
| 22 | DISCOVERY_INTERVAL_MS morta | pendente |
| 23 | ESM import() sem retry | pendente |
| 24 | "INVALID OPERATION" polui logs | pendente |

### BAIXO
| # | O que | Status |
|---|-------|--------|
| 25 | Dashboard SOL/XRP | pendente |
| 26 | Between-strikes tracking | pendente |

### EXTRAS concluídos
- ✅ Kalshi IOC partial fill
- ✅ Poly improvement revertido
- ✅ POLY_DEPTH_BUFFER ajustado
- ✅ Execução paralela Promise.allSettled
- ✅ Recheck pós-Kalshi com fees + improvement
- ✅ Notional upsize guard
- ✅ Fix PnL (takingAmount real + fee correta + double-count)
- ✅ Strike Guard
- ✅ Hedge retry 5x com cooldown 30s
- ✅ Auto Redeemer fixes

---

## Dados de Slippage (Bug #3 — referência para calibração)

| Poly bid | Fill real | Slippage | Decisão | PnL real |
|----------|-----------|----------|---------|----------|
| $0.96 | $0.96 | 0% | +$0.08 | +$0.18 |
| $0.66 | $0.73 | +10% | +$0.13 | +$0.94 |
| $0.30 | $0.276 | -8% | +$0.18 | +$0.04 |
| $0.20 | $0.120 | -40% | +$0.13 | -$0.47 |
| $0.61 | $0.50 | -18% | +$0.08 | -$0.25 |

Padrão: bid baixo = book raso = slippage negativo alto.

---

## Investigações Pendentes

1. Poly fee real (3-5%) > config (2%) — não afeta early exit (usa takingAmount) mas afeta settlement
2. cumPnl não captura PnL de settlement (trades sem early exit)
3. CLOB v1 → v2 migração pendente
4. Fee rate hardcoded 1000 bps (bug #18)
