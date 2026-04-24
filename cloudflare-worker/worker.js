/**
 * Cloudflare Worker — Vendas Proprias Aggregator
 * Festival Interlagos 2026 — Agencia Lime
 *
 * Substitui o vendas-data.json estatico do GitHub Actions.
 * Chama a API da plataforma (ingressosmoto + ingressosauto) em tempo real,
 * agrega por dia e retorna JSON no mesmo formato.
 *
 * Cache interno de 30s (evita sobrecarregar a API com muitos acessos).
 *
 * Deploy:
 *   1. https://dash.cloudflare.com -> Workers & Pages -> Create Worker
 *   2. Cola este arquivo no editor
 *   3. Save and Deploy
 *   4. Copia a URL (ex: https://vendas-festival.XXX.workers.dev)
 *   5. Me passa a URL que eu atualizo os dashboards
 */

const API_MOTO        = 'https://ingressosmoto.festivalinterlagos.com.br';
const API_AUTO        = 'https://ingressosauto.festivalinterlagos.com.br';
const DATA_INICIO_STR = '2026-01-01';              // busca tudo desde jan/26
const DATA_INICIO     = new Date('2026-03-31T00:00:00Z'); // D+1
const PAGE_SIZE       = 100;
const CACHE_TTL       = 30; // segundos

// ─── HEADERS para escapar do 403 ─────────────────────────
function buildHeaders(base, token) {
  const h = {
    'Accept':           'application/json',
    'Content-Type':     'application/json',
    'Origin':           base,
    'Referer':          base + '/',
    'User-Agent':       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
                        '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
  };
  if (token) h['Authorization'] = 'Bearer ' + token;
  return h;
}

// ─── CHAMADAS API ────────────────────────────────────────
async function getToken(base) {
  const r = await fetch(base + '/apis/token', { headers: buildHeaders(base) });
  if (!r.ok) throw new Error(`Token ${base} HTTP ${r.status}`);
  const d = await r.json();
  if (d.status !== 'success') throw new Error('Token status: ' + d.status);
  return d.token;
}

async function getAllVendas(base, token) {
  const fim   = new Date().toISOString().split('T')[0];
  const sales = [];
  let page    = 1;
  while (true) {
    const url = `${base}/apis/vendas?data_inicio=${DATA_INICIO_STR}&data_fim=${fim}&page_size=${PAGE_SIZE}&page=${page}`;
    const r   = await fetch(url, { headers: buildHeaders(base, token) });
    if (!r.ok) throw new Error(`Vendas ${base} HTTP ${r.status}`);
    const d = await r.json();
    if (d.status !== 'success' || !d.data || !d.data.length) break;
    sales.push(...d.data);
    if (d.pagination?.isNextPage !== 'Y') break;
    page++;
  }
  return sales;
}

// ─── AGREGACAO ───────────────────────────────────────────
function aggregateByDay(sales) {
  const byDay = {};
  for (const v of sales) {
    if (String(v.venda_status) !== '3') continue;
    const ds = (v.created_at || '').split(' ')[0];
    if (!ds || ds.length < 10) continue;
    const dt = new Date(ds + 'T00:00:00Z');
    const d  = Math.floor((dt - DATA_INICIO) / 86400000) + 1;
    if (d < 1) continue;
    if (!byDay[d]) byDay[d] = { receita: 0, qtd: 0, date: ds };
    byDay[d].receita += parseFloat(v.venda_valor) || 0;
    byDay[d].qtd     += (v.qrcodes || []).length;
  }
  return byDay;
}

async function buildVendasData() {
  const [motoToken, autoToken] = await Promise.all([getToken(API_MOTO), getToken(API_AUTO)]);
  const [motoSales, autoSales] = await Promise.all([
    getAllVendas(API_MOTO, motoToken),
    getAllVendas(API_AUTO, autoToken),
  ]);

  const motoByDay = aggregateByDay(motoSales);
  const autoByDay = aggregateByDay(autoSales);

  const daySet = new Set([
    ...Object.keys(motoByDay).map(Number),
    ...Object.keys(autoByDay).map(Number),
  ]);
  const allDays = [...daySet].sort((a, b) => a - b);

  const daily  = [];
  const totals = {
    moto_receita: 0, auto_receita: 0,
    moto_ingressos: 0, auto_ingressos: 0,
  };

  for (const d of allDays) {
    const m  = motoByDay[d] || { receita: 0, qtd: 0 };
    const a  = autoByDay[d] || { receita: 0, qtd: 0 };
    const dt = new Date(DATA_INICIO);
    dt.setUTCDate(dt.getUTCDate() + d - 1);
    daily.push({
      d,
      date:            dt.toISOString().split('T')[0],
      moto_receita:    Math.round(m.receita * 100) / 100,
      moto_ingressos:  m.qtd,
      auto_receita:    Math.round(a.receita * 100) / 100,
      auto_ingressos:  a.qtd,
    });
    totals.moto_receita   += m.receita;
    totals.moto_ingressos += m.qtd;
    totals.auto_receita   += a.receita;
    totals.auto_ingressos += a.qtd;
  }

  totals.moto_receita    = Math.round(totals.moto_receita   * 100) / 100;
  totals.auto_receita    = Math.round(totals.auto_receita   * 100) / 100;
  totals.total_receita   = Math.round((totals.moto_receita + totals.auto_receita) * 100) / 100;
  totals.total_ingressos = totals.moto_ingressos + totals.auto_ingressos;

  return {
    updated_at:     new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
    source:         'Cloudflare Worker (real-time, cache 30s)',
    campaign_start: '2026-03-31',
    totals,
    daily,
  };
}

// ─── WORKER HANDLER ──────────────────────────────────────
export default {
  async fetch(request, env, ctx) {
    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin':  '*',
          'Access-Control-Allow-Methods': 'GET, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      });
    }

    // Tenta pegar do cache (30s TTL)
    const cache    = caches.default;
    const cacheKey = new Request(new URL(request.url).toString(), request);
    let response   = await cache.match(cacheKey);
    if (response) {
      const r2 = new Response(response.body, response);
      r2.headers.set('X-Cache', 'HIT');
      return r2;
    }

    // Sem cache: busca na API
    try {
      const data = await buildVendasData();
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
