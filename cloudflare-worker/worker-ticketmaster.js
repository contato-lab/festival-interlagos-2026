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
 */

const API_BASE         = 'https://data.getcrowder.com';
const API_ENDPOINT     = '/activity/organizer';
const API_KEY          = '52087b883f65e8d2a684ee680c5beca66e425fdcc46c2b653da0fffd50088734';
const CAMPAIGN_START_MS = 1774396800000; // 2026-03-25 00:00:00 UTC

const MOTO_SHOW_IDS = new Set([195330, 195736, 195737, 195738]);
const AUTO_SHOW_IDS = new Set([195331, 195739, 195740, 195741]);

const CACHE_TTL = 30; // segundos

// ─── FETCH COM PAGINACAO ─────────────────────────────────
async function fetchAllMovements() {
  const all = [];
  let lastUpdate     = CAMPAIGN_START_MS;
  let lastMovementId = 1;
  let page           = 0;

  while (true) {
    page++;
    const url = `${API_BASE}${API_ENDPOINT}?lastUpdate=${lastUpdate}&lastMovementId=${lastMovementId}`;
    const r = await fetch(url, {
      headers: {
        'apiKey':       API_KEY,
        'Content-Type': 'application/json',
      },
    });
    if (!r.ok) throw new Error(`TM HTTP ${r.status} na pagina ${page}`);
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
  // Indexa ISSUANCES por purchase.id -> date_str original
  const issuanceDateByPurchase = {};
  for (const mv of movements) {
    if (mv.operation === 'ISSUANCE') {
      const pid = mv.purchase?.id;
      if (pid != null && !(pid in issuanceDateByPurchase)) {
        issuanceDateByPurchase[pid] = (mv.date || '').slice(0, 10);
      }
    }
  }

  const daily  = {};
  const totals = {
    moto_receita: 0, auto_receita: 0,
    moto_ingressos: 0, auto_ingressos: 0,
  };

  for (const mv of movements) {
    const op      = mv.operation;
    const amount  = parseFloat(mv.amount || 0);
    const tc      = parseInt(mv.ticketCount || 0);
    const edition = classifyShow(mv.tickets);

    let dateStr;
    if (op === 'CANCELLATION' || op === 'REFUND') {
      const pid = mv.purchase?.id;
      dateStr = issuanceDateByPurchase[pid] || (mv.date || '').slice(0, 10);
    } else {
      dateStr = (mv.date || '').slice(0, 10);
    }

    if (!daily[dateStr]) {
      daily[dateStr] = {
        moto_receita: 0, auto_receita: 0,
        moto_ingressos: 0, auto_ingressos: 0,
      };
    }

    if (edition === 'moto') {
      daily[dateStr].moto_receita   += amount;
      daily[dateStr].moto_ingressos += tc;
      totals.moto_receita           += amount;
      totals.moto_ingressos         += tc;
    } else if (edition === 'auto') {
      daily[dateStr].auto_receita   += amount;
      daily[dateStr].auto_ingressos += tc;
      totals.auto_receita           += amount;
      totals.auto_ingressos         += tc;
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
    }));

  return { totals, daily: dailyList };
}

async function buildTmData() {
  const movements = await fetchAllMovements();
  if (!movements.length) throw new Error('Nenhum movimento retornado');

  const { totals, daily } = aggregate(movements);

  return {
    updated_at:     new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
    source:         'Cloudflare Worker TM (real-time, cache 30s)',
    campaign_start: '2026-03-25',
    moto_show_ids:  [...MOTO_SHOW_IDS].sort(),
    auto_show_ids:  [...AUTO_SHOW_IDS].sort(),
    totals,
    daily,
  };
}

// ─── HANDLER ─────────────────────────────────────────────
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

    const cache    = caches.default;
    const cacheKey = new Request(new URL(request.url).toString(), request);
    let response   = await cache.match(cacheKey);
    if (response) {
      const r2 = new Response(response.body, response);
      r2.headers.set('X-Cache', 'HIT');
      return r2;
    }

    try {
      const data = await buildTmData();
      response = new Response(JSON.stringify(data), {
        headers: {
          'Content-Type':                 'application/json; charset=utf-8',
          'Cache-Control':                `public, max-age=${CACHE_TTL}`,
          'Access-Control-Allow-Origin':  '*',
          'X-Cache':                      'MISS',
        },
      });
      ctx.waitUntil(cache.put(cacheKey, response.clone()));
      return response;
    } catch (err) {
      return new Response(JSON.stringify({
        error:      err.message,
        updated_at: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
      }), {
        status:  500,
        headers: {
          'Content-Type':                'application/json',
          'Access-Control-Allow-Origin': '*',
        },
      });
    }
  },
};
