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
const CACHE_TTL       = 30;        // segundos - cache normal
const STALE_TTL       = 600;       // 10min - cache 'velho aceitavel' quando API falha
const MAX_RETRIES     = 3;         // retry para chamadas individuais a API

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

// ─── CHAMADAS API com retry + backoff ────────────────────
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
        // Backoff exponencial: 200ms, 600ms, 1800ms
        await new Promise(res => setTimeout(res, 200 * Math.pow(3, attempt - 1)));
      }
    }
  }
  throw new Error(`${label} falhou apos ${MAX_RETRIES} tentativas: ${lastErr.message}`);
}

async function getToken(base) {
  const r = await fetchWithRetry(base + '/apis/token', { headers: buildHeaders(base) }, `Token ${base}`);
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
    const r   = await fetchWithRetry(url, { headers: buildHeaders(base, token) }, `Vendas ${base} pag.${page}`);
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
//
// Estrategia 'stale-while-error':
//   - Cache fresco (< CACHE_TTL): retorna direto (X-Cache: HIT)
//   - Cache velho (CACHE_TTL a STALE_TTL): tenta atualizar; se API falha, retorna o velho (X-Cache: STALE)
//   - Sem cache OU cache muito velho (> STALE_TTL): tenta API; se falha, retorna 503
//
// Usa 2 chaves de cache:
//   - cacheKey         : versao curta (CACHE_TTL = 30s) - serve respostas frescas
//   - staleCacheKey    : versao longa (STALE_TTL = 10min) - guarda ultimo dado bom pra fallback
//
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
      const data = await buildVendasData();
      const body = JSON.stringify(data);

      // Cache fresco (30s)
      const freshResp = new Response(body, {
        headers: {
          'Content-Type':                 'application/json; charset=utf-8',
          'Cache-Control':                `public, max-age=${CACHE_TTL}`,
          'Access-Control-Allow-Origin':  '*',
          'X-Cache':                      'MISS',
        },
      });
      ctx.waitUntil(cache.put(cacheKey, freshResp.clone()));

      // Cache stale (10min) - usado como fallback se API cair
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
      // 3) API falhou: tenta servir cache stale (ate 10min de idade)
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

      // 4) Sem nenhum cache disponivel: 503 com info
      return new Response(JSON.stringify({
        error:      err.message,
        updated_at: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
        hint:       'API origem indisponivel e sem cache stale. Tentar fallback estatico.',
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
