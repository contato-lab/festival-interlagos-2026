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
 * Timezone: a API getcrowder.com entrega timestamps em UTC. Convertemos
 * pra BRT (UTC-3 fixo, Brasil nao usa DST desde 2019) antes de fatiar
 * o YYYY-MM-DD. Sem isso, vendas entre 21:00-23:59 BRT caem no dia
 * seguinte (descalibra +N/-N vs o painel oficial da TM).
 */

const API_BASE         = 'https://data.getcrowder.com';
const API_ENDPOINT     = '/activity/organizer';
const API_KEY          = '4f3a9648a77d9dbf29969726d71521d8fba8a01af91129a51ac2d8e80fc15991';
const CAMPAIGN_START_MS = 1774396800000; // 2026-03-25 00:00:00 UTC

// Brasil/São Paulo: UTC-3 fixo (sem horário de verão desde 2019).
// Converte um timestamp ISO da API (UTC) pra string YYYY-MM-DD em BRT,
// para que vendas entre 21:00-23:59 BRT não caiam no dia seguinte.
const BRT_OFFSET_MS = 3 * 60 * 60 * 1000;
function toBrtDateStr(isoStr) {
  if (!isoStr) return '';
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return (isoStr || '').slice(0, 10);
  return new Date(d.getTime() - BRT_OFFSET_MS).toISOString().slice(0, 10);
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
async function fetchAllMovements() {
  const all = [];
  let lastUpdate     = CAMPAIGN_START_MS;
  let lastMovementId = 1;
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
function aggregate(movements) {
  // Indexa ISSUANCES por purchase.id -> date_str original (em BRT)
  const issuanceDateByPurchase = {};
  for (const mv of movements) {
    if (mv.operation === 'ISSUANCE') {
      const pid = mv.purchase?.id;
      if (pid != null && !(pid in issuanceDateByPurchase)) {
        issuanceDateByPurchase[pid] = toBrtDateStr(mv.date);
      }
    }
  }

  const daily  = {};
  const totals = {
    moto_receita: 0, auto_receita: 0,
    moto_ingressos: 0, auto_ingressos: 0,
    moto_cortesias: 0, auto_cortesias: 0,  // rastreio separado
  };
  let lastSaleMotoAt = null;
  let lastSaleAutoAt = null;

  for (const mv of movements) {
    const op      = mv.operation;
    const amount  = parseFloat(mv.amount || 0);
    const tc      = parseInt(mv.ticketCount || 0);
    const edition = classifyShow(mv.tickets);

    // ──────────────────────────────────────────────────────────────────
    // CORTESIAS / VOUCHERS: movimentações ISSUANCE com valor ZERO não
    // são vendas comerciais — são cortesias pra imprensa, patrocinador,
    // equipe, etc. Contar elas no "ingressos vendidos" infla o número e
    // descalibra o TKT médio (ex: 21/05/2026 teve 151 cortesias de moto
    // junto com 35 vendas reais, fazendo o TKT cair de ~R$ 90 pra R$ 17).
    // Mantemos contagem separada pra rastreabilidade.
    // CANCELLATION/REFUND com amount 0 são processados normalmente
    // (estornos zerados podem acontecer).
    // ──────────────────────────────────────────────────────────────────
    const isCortesia = (op === 'ISSUANCE' && amount === 0 && tc > 0);

    let dateStr;
    if (op === 'CANCELLATION' || op === 'REFUND') {
      const pid = mv.purchase?.id;
      dateStr = issuanceDateByPurchase[pid] || toBrtDateStr(mv.date);
    } else {
      dateStr = toBrtDateStr(mv.date);
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
  totals.total_receita   = Math.round((totals.moto_receita + totals.auto_receita) * 100) / 100;
  totals.total_ingressos = totals.moto_ingressos + totals.auto_ingressos;

  const dailyList = Object.keys(daily)
    .sort()
    .map(dateStr => ({
      date:             dateStr,
      moto_receita:     Math.round(daily[dateStr].moto_receita * 100) / 100,
      auto_receita:     Math.round(daily[dateStr].auto_receita * 100) / 100,
      moto_ingressos:   daily[dateStr].moto_ingressos,
      auto_ingressos:   daily[dateStr].auto_ingressos,
      moto_cortesias:   daily[dateStr].moto_cortesias,
      auto_cortesias:   daily[dateStr].auto_cortesias,
    }));

  return { totals, daily: dailyList, last_sale_moto_at: lastSaleMotoAt, last_sale_auto_at: lastSaleAutoAt };
}

async function buildTmData() {
  const movements = await fetchAllMovements();
  if (!movements.length) throw new Error('Nenhum movimento retornado');

  const { totals, daily, last_sale_moto_at, last_sale_auto_at } = aggregate(movements);

  return {
    updated_at:          new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
    source:              'Cloudflare Worker TM (real-time, cache 30s)',
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
    const cacheKey      = new Request(baseUrl + '?v=fresh', request);
    const staleCacheKey = new Request(baseUrl + '?v=stale', request);

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
