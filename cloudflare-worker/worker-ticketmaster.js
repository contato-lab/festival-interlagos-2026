/**
 * Cloudflare Worker — Ticketmaster Aggregator
 * Festival Interlagos 2026 — Agencia Lime
 *
 * Substitui o ticketmaster-data.json estatico do GitHub Actions.
 * Chama getcrowder.com em tempo real, agrega por dia e retorna JSON
 * no mesmo formato.
 *
 * Cache interno de 30s.
 *
 * Logica de cancelamento: CANCELLATION/REFUND sao atribuidos a data
 * da ISSUANCE original (via purchase.id), nao a data do cancelamento.
 * Isso faz bater com o dashboard oficial do Ticketmaster.
 *
 * Timezone: a API getcrowder.com entrega timestamps em UTC, e o PAINEL
 * OFICIAL da Ticketmaster agrupa as vendas por dia UTC (não BRT). Pra
 * que nosso dashboard mostre o mesmo número que o painel oficial, também
 * fatiamos a data em UTC. Antes convertíamos pra BRT mas isso causava
 * divergência: vendas BRT 21:00-23:59 viravam dia anterior no nosso vs
 * dia seguinte no painel TM (ex: 21/05 21:04 BRT = 22/05 00:04 UTC).
 */

const API_BASE         = 'https://data.getcrowder.com';
const API_ENDPOINT     = '/activity/organizer';
const API_KEY          = '4f3a9648a77d9dbf29969726d71521d8fba8a01af91129a51ac2d8e80fc15991';
const CAMPAIGN_START_MS = 1774396800000; // 2026-03-25 00:00:00 UTC

// Converte um timestamp ISO (UTC) pra string YYYY-MM-DD em UTC.
// Mantém a convenção do painel oficial da Ticketmaster.
function toUtcDateStr(isoStr) {
  if (!isoStr) return '';
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return (isoStr || '').slice(0, 10);
  return d.toISOString().slice(0, 10);
}

const MOTO_SHOW_IDS = new Set([195330, 195736, 195737, 195738]);
const AUTO_SHOW_IDS = new Set([195331, 195739, 195740, 195741]);

const CACHE_TTL  = 300;       // 5min - cache normal (reduz cache MISS que demora ~25s)
const STALE_TTL  = 3600;      // 1h - fallback cache quando API falha
const MAX_RETRIES = 3;

// ─── FETCH com retry + backoff ───────────────────────────
async function fetchWithRetry(url, opts, label) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const r = await fetch(url, opts);
      if (!r.ok) throw new Error(`${label} HTTP ${r.status}`);
      return r;
    } catch (err) {
      lastErr = err;
      if (attempt < MAX_RETRIES) {
        await new Promise(res => setTimeout(res, 200 * Math.pow(3, attempt - 1)));
      }
    }
  }
  throw new Error(`${label} falhou apos ${MAX_RETRIES} tentativas: ${lastErr.message}`);
}

// ─── FETCH COM PAGINACAO ─────────────────────────────────
async function fetchAllMovements(startUpdate = CAMPAIGN_START_MS, startMovementId = 1) {
  const all = [];
  let lastUpdate     = startUpdate;
  let lastMovementId = startMovementId;
  let page           = 0;

  while (true) {
    page++;
    const url = `${API_BASE}${API_ENDPOINT}?lastUpdate=${lastUpdate}&lastMovementId=${lastMovementId}`;
    const r = await fetchWithRetry(url, {
      headers: {
        'apiKey':       API_KEY,
        'Content-Type': 'application/json',
      },
    }, `TM pag.${page}`);
    const d = await r.json();
    const movements = d.movements || [];
    const hasMore   = d.hasMore || false;
    all.push(...movements);
    if (!hasMore || !movements.length) break;
    lastUpdate     = d.lastUpdate     ?? lastUpdate;
    lastMovementId = d.lastMovementId ?? lastMovementId;
    if (page > 100) break; // safety
  }
  return all;
}

// ─── CLASSIFICACAO DO SHOW ───────────────────────────────
function classifyShow(tickets) {
  for (const t of tickets || []) {
    const id = t.show?.id;
    if (MOTO_SHOW_IDS.has(id)) return 'moto';
    if (AUTO_SHOW_IDS.has(id)) return 'auto';
  }
  return null;
}

// ─── AGREGACAO ───────────────────────────────────────────
function aggregate(movements, opts = {}) {
  // clamp=false e usado quando este resultado ainda vai ser SOMADO em cima
  // de um baseline (busca incremental): clampar em 0 aqui, no delta isolado,
  // pode zerar erroneamente uma reducao legitima (ex.: cancelamento de uma
  // venda emitida num lote anterior). O clamp final acontece so depois de
  // somar delta + baseline, no merge.
  const clamp = opts.clamp !== false;
  // Indexa ISSUANCES por purchase.id -> date_str original (em UTC)
  const issuanceDateByPurchase = {};
  for (const mv of movements) {
    if (mv.operation === 'ISSUANCE') {
      const pid = mv.purchase?.id;
      if (pid != null && !(pid in issuanceDateByPurchase)) {
        issuanceDateByPurchase[pid] = toUtcDateStr(mv.date);
      }
    }
  }

  const daily  = {};
  const totals = {
    moto_receita: 0, auto_receita: 0,
    moto_ingressos: 0, auto_ingressos: 0,
    moto_cortesias: 0, auto_cortesias: 0,  // rastreio separado
    orphan_refunds: 0,                      // diagnostico: estornos sem issuance no batch
  };
  let lastSaleMotoAt = null;
  let lastSaleAutoAt = null;

  for (const mv of movements) {
    const op      = mv.operation;
    const amount  = parseFloat(mv.amount || 0);
    const tc      = parseInt(mv.ticketCount || 0);
    const edition = classifyShow(mv.tickets);

    // ──────────────────────────────────────────────────────────────────
    // CORTESIAS / VOUCHERS: qualquer movimentação com amount=0 e tc!=0 é
    // cortesia. Inclui:
    //   - ISSUANCE de cortesia (tc>0): emissão pra imprensa, patrocinador, etc
    //   - REFUND/CANCELLATION de cortesia (tc<0): retirada de cortesia
    // Tratar simétrico evita o bug onde CANCELAMENTO DE CORTESIA era
    // subtraído de moto_ingressos (vendas), gerando contagem negativa
    // no dia do cancelamento (ex: 25/05/2026 mostrou -42 motos quando
    // 100 cortesias foram canceladas — antes elas nunca tinham entrado
    // em moto_ingressos, mas o cancelamento estava saindo de lá).
    // Mantemos cortesias em contagem separada pra rastreabilidade e pra
    // o TKT médio não ser descalibrado.
    // ──────────────────────────────────────────────────────────────────
    const isCortesia = (amount === 0 && tc !== 0);

    let dateStr;
    if ((op === 'CANCELLATION' || op === 'REFUND') && !isCortesia) {
      // Cancelamento de venda real — atribuir à data da ISSUANCE original
      // pra bater com o painel oficial da Ticketmaster.
      const pid = mv.purchase?.id;
      // Se a ISSUANCE original não está no batch (compra de campanha
      // anterior ou purchase.id null), IGNORAR o estorno pra não inflar
      // negativos no dia do cancelamento.
      if (pid == null || !(pid in issuanceDateByPurchase)) {
        totals.orphan_refunds++;
        continue;
      }
      dateStr = issuanceDateByPurchase[pid];
    } else {
      // ISSUANCE (venda ou cortesia) e cancelamento de cortesia: usa a
      // data do próprio movimento.
      dateStr = toUtcDateStr(mv.date);
    }

    if (!daily[dateStr]) {
      daily[dateStr] = {
        moto_receita: 0, auto_receita: 0,
        moto_ingressos: 0, auto_ingressos: 0,
        moto_cortesias: 0, auto_cortesias: 0,
      };
    }

    if (edition === 'moto') {
      if (isCortesia) {
        daily[dateStr].moto_cortesias += tc;
        totals.moto_cortesias         += tc;
      } else {
        daily[dateStr].moto_receita   += amount;
        daily[dateStr].moto_ingressos += tc;
        totals.moto_receita           += amount;
        totals.moto_ingressos         += tc;
        if (op === 'ISSUANCE' && tc > 0) {
          const mvDate = mv.date || '';
          if (!lastSaleMotoAt || mvDate > lastSaleMotoAt) lastSaleMotoAt = mvDate;
        }
      }
    } else if (edition === 'auto') {
      if (isCortesia) {
        daily[dateStr].auto_cortesias += tc;
        totals.auto_cortesias         += tc;
      } else {
        daily[dateStr].auto_receita   += amount;
        daily[dateStr].auto_ingressos += tc;
        totals.auto_receita           += amount;
        totals.auto_ingressos         += tc;
        if (op === 'ISSUANCE' && tc > 0) {
          const mvDate = mv.date || '';
          if (!lastSaleAutoAt || mvDate > lastSaleAutoAt) lastSaleAutoAt = mvDate;
        }
      }
    }
  }

  totals.moto_receita    = Math.round(totals.moto_receita * 100) / 100;
  totals.auto_receita    = Math.round(totals.auto_receita * 100) / 100;
  if (clamp) {
    totals.moto_ingressos  = Math.max(0, totals.moto_ingressos);
    totals.auto_ingressos  = Math.max(0, totals.auto_ingressos);
    totals.moto_cortesias  = Math.max(0, totals.moto_cortesias);
    totals.auto_cortesias  = Math.max(0, totals.auto_cortesias);
  }
  totals.total_receita   = Math.round((totals.moto_receita + totals.auto_receita) * 100) / 100;
  totals.total_ingressos = totals.moto_ingressos + totals.auto_ingressos;

  const clampFn = v => clamp ? Math.max(0, v) : v;
  const dailyList = Object.keys(daily)
    .sort()
    .map(dateStr => ({
      date:             dateStr,
      moto_receita:     clampFn(Math.round(daily[dateStr].moto_receita * 100) / 100),
      auto_receita:     clampFn(Math.round(daily[dateStr].auto_receita * 100) / 100),
      // Defensivo: nunca expor contagem negativa (sinal de bug). Se aparecer
      // negativo, é melhor mostrar 0 do que confundir o usuario com -42.
      // (Desligado quando clamp=false, ver comentario no topo da funcao.)
      moto_ingressos:   clampFn(daily[dateStr].moto_ingressos),
      auto_ingressos:   clampFn(daily[dateStr].auto_ingressos),
      moto_cortesias:   clampFn(daily[dateStr].moto_cortesias),
      auto_cortesias:   clampFn(daily[dateStr].auto_cortesias),
    }));

  return { totals, daily: dailyList, last_sale_moto_at: lastSaleMotoAt, last_sale_auto_at: lastSaleAutoAt };
}

// ─── BUSCA INCREMENTAL (parte do snapshot do GitHub Actions) ─────
//
// O historico completo da campanha ja passou de 64 mil movimentos, e
// reprocessar tudo a cada cache-miss (5 em 5 min) estourava o limite de
// CPU da Cloudflare (erro 1102 -> 503). Em vez disso: parte do JSON que o
// robo Python ja gera (~1x por hora, sem limite de CPU porque roda no
// GitHub Actions), busca SO o que e novo desde o cursor guardado nesse
// JSON, e soma por cima. Sem estado proprio no Worker (sem KV): se uma
// rodada tiver algum erro de soma, ela se autocorrige sozinha na proxima
// vez que o snapshot for atualizado (no maximo ~1h depois), nunca acumula.
const BASELINE_URL = 'https://contato-lab.github.io/festival-interlagos-2026/ticketmaster-data.json';

async function fetchBaseline() {
  const r = await fetch(BASELINE_URL, { cf: { cacheTtl: 60, cacheEverything: true } });
  if (!r.ok) throw new Error('baseline HTTP ' + r.status);
  const d = await r.json();
  if (d.tm_cursor_last_update == null || d.tm_cursor_last_movement_id == null) {
    throw new Error('baseline sem cursor (tm_cursor_last_update/last_movement_id ausente)');
  }
  return d;
}

// Soma o delta (por dia) em cima do baseline, clampando em 0 so no resultado
// final ja somado (nunca no delta isolado, ver comentario em aggregate()).
function mergeDaily(baseDaily, deltaDaily) {
  const map = {};
  for (const d of (baseDaily || [])) map[d.date] = { ...d };
  for (const d of (deltaDaily || [])) {
    const cur = map[d.date] || {
      date: d.date, moto_receita: 0, auto_receita: 0,
      moto_ingressos: 0, auto_ingressos: 0, moto_cortesias: 0, auto_cortesias: 0,
    };
    cur.moto_receita   = Math.round((cur.moto_receita + d.moto_receita) * 100) / 100;
    cur.auto_receita   = Math.round((cur.auto_receita + d.auto_receita) * 100) / 100;
    cur.moto_ingressos = Math.max(0, cur.moto_ingressos + d.moto_ingressos);
    cur.auto_ingressos = Math.max(0, cur.auto_ingressos + d.auto_ingressos);
    cur.moto_cortesias = Math.max(0, cur.moto_cortesias + d.moto_cortesias);
    cur.auto_cortesias = Math.max(0, cur.auto_cortesias + d.auto_cortesias);
    map[d.date] = cur;
  }
  return Object.values(map).sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
}

function maisRecente(a, b) {
  if (!a) return b || null;
  if (!b) return a;
  return b > a ? b : a;
}

async function buildTmDataIncremental(baseline) {
  const deltaMovements = await fetchAllMovements(
    baseline.tm_cursor_last_update, baseline.tm_cursor_last_movement_id
  );

  // Nada novo desde a ultima rodada do robo: devolve o snapshot como esta.
  if (!deltaMovements.length) {
    return {
      updated_at:        new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
      source:            `Cloudflare Worker TM (tempo real, sem vendas novas desde ${baseline.updated_at})`,
      campaign_start:    '2026-03-25',
      moto_show_ids:      [...MOTO_SHOW_IDS].sort(),
      auto_show_ids:      [...AUTO_SHOW_IDS].sort(),
      last_sale_moto_at: baseline.last_sale_moto_at,
      last_sale_auto_at: baseline.last_sale_auto_at,
      totals:            baseline.totals,
      daily:             baseline.daily,
    };
  }

  // clamp:false aqui de proposito — o clamp final acontece so depois do merge.
  const delta = aggregate(deltaMovements, { clamp: false });

  const totals = { ...baseline.totals };
  for (const k of ['moto_receita', 'auto_receita', 'moto_ingressos', 'auto_ingressos',
                    'moto_cortesias', 'auto_cortesias']) {
    totals[k] = (totals[k] || 0) + (delta.totals[k] || 0);
  }
  totals.moto_receita    = Math.round(totals.moto_receita * 100) / 100;
  totals.auto_receita    = Math.round(totals.auto_receita * 100) / 100;
  totals.moto_ingressos  = Math.max(0, totals.moto_ingressos);
  totals.auto_ingressos  = Math.max(0, totals.auto_ingressos);
  totals.moto_cortesias  = Math.max(0, totals.moto_cortesias);
  totals.auto_cortesias  = Math.max(0, totals.auto_cortesias);
  totals.total_receita   = Math.round((totals.moto_receita + totals.auto_receita) * 100) / 100;
  totals.total_ingressos = totals.moto_ingressos + totals.auto_ingressos;

  return {
    updated_at:        new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
    source:            `Cloudflare Worker TM (tempo real, delta sobre snapshot de ${baseline.updated_at})`,
    campaign_start:    '2026-03-25',
    moto_show_ids:      [...MOTO_SHOW_IDS].sort(),
    auto_show_ids:      [...AUTO_SHOW_IDS].sort(),
    last_sale_moto_at: maisRecente(baseline.last_sale_moto_at, delta.last_sale_moto_at),
    last_sale_auto_at: maisRecente(baseline.last_sale_auto_at, delta.last_sale_auto_at),
    totals,
    daily: mergeDaily(baseline.daily, delta.daily),
  };
}

async function buildTmData() {
  // Caminho novo (leve): parte do snapshot do robo e busca so o delta.
  try {
    const baseline = await fetchBaseline();
    return await buildTmDataIncremental(baseline);
  } catch (err) {
    // Sem snapshot valido (1a implantacao, Pages fora do ar, ou snapshot
    // ainda sem o campo de cursor): cai pro fetch completo de sempre.
  }

  const movements = await fetchAllMovements();
  if (!movements.length) throw new Error('Nenhum movimento retornado');

  const { totals, daily, last_sale_moto_at, last_sale_auto_at } = aggregate(movements);

  return {
    updated_at:          new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
    source:              'Cloudflare Worker TM (fetch completo, sem baseline)',
    campaign_start:      '2026-03-25',
    moto_show_ids:       [...MOTO_SHOW_IDS].sort(),
    auto_show_ids:       [...AUTO_SHOW_IDS].sort(),
    last_sale_moto_at,
    last_sale_auto_at,
    totals,
    daily,
  };
}

// ─── HANDLER ─────────────────────────────────────────────
//
// Estrategia 'stale-while-error':
//   - Cache fresco (< CACHE_TTL): retorna direto
//   - Sem cache fresco: tenta API; se sucesso, atualiza cache fresco E stale
//   - API falha: tenta servir cache stale (ate 10min); se nada disponivel, 503
//
export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin':  '*',
          'Access-Control-Allow-Methods': 'GET, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      });
    }

    const cache         = caches.default;
    const baseUrl       = new URL(request.url).toString().split('?')[0];
    // Bump esse sufixo quando mudar lógica de agregação pra invalidar cache no edge.
    // v2 = UTC ao invés de BRT (2026-05-22).
    // v3 = ignora REFUND/CANCELLATION orfaos + clamp em 0 (2026-05-25).
    // v4 = trata cancelamento de cortesia como cortesia (nao como venda).
    // v5 = busca incremental sobre o snapshot do robo, em vez de reprocessar
    //      o historico inteiro a cada cache-miss (estava estourando CPU).
    // v6 = clamp em 0 tambem pras cortesias no merge do delta (v5 deixou
    //      passar -1387 cortesias num dia por nao clampar esse campo).
    const cacheKey      = new Request(baseUrl + '?v=fresh-utc-v6', request);
    const staleCacheKey = new Request(baseUrl + '?v=stale-utc-v6', request);

    // 1) Cache fresco?
    let cached = await cache.match(cacheKey);
    if (cached) {
      const r2 = new Response(cached.body, cached);
      r2.headers.set('X-Cache', 'HIT');
      return r2;
    }

    // 2) Tenta API
    try {
      const data = await buildTmData();
      const body = JSON.stringify(data);

      const freshResp = new Response(body, {
        headers: {
          'Content-Type':                 'application/json; charset=utf-8',
          'Cache-Control':                `public, max-age=${CACHE_TTL}`,
          'Access-Control-Allow-Origin':  '*',
          'X-Cache':                      'MISS',
        },
      });
      ctx.waitUntil(cache.put(cacheKey, freshResp.clone()));

      const staleResp = new Response(body, {
        headers: {
          'Content-Type':                 'application/json; charset=utf-8',
          'Cache-Control':                `public, max-age=${STALE_TTL}`,
          'Access-Control-Allow-Origin':  '*',
        },
      });
      ctx.waitUntil(cache.put(staleCacheKey, staleResp));

      return freshResp;
    } catch (err) {
      // 3) API falhou: tenta servir cache stale
      const stale = await cache.match(staleCacheKey);
      if (stale) {
        const body = await stale.text();
        return new Response(body, {
          headers: {
            'Content-Type':                'application/json; charset=utf-8',
            'Access-Control-Allow-Origin': '*',
            'X-Cache':                     'STALE',
            'X-Error':                     err.message,
          },
        });
      }

      // 4) Sem nenhum cache: 503
      return new Response(JSON.stringify({
        error:      err.message,
        updated_at: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
        hint:       'API origem indisponivel e sem cache stale.',
      }), {
        status:  503,
        headers: {
          'Content-Type':                'application/json',
          'Access-Control-Allow-Origin': '*',
        },
      });
    }
  },
};
