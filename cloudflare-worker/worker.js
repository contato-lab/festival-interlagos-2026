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
const DATA_INICIO_STR = '2026-01-01';              // fetch completo: tudo desde jan/26
const DATA_INICIO     = new Date('2026-03-31T00:00:00Z'); // D+1
// 500 e nao 100. Com 100, a varredura de moto precisava de ~141 paginas e
// batia no teto de 150 (contando canceladas, passava). O Worker entao se
// recusava a entregar lista parcial e estourava: era o erro 1101 que derrubou
// as vendas em tempo real em 16/08/2026, no ultimo dia da edicao moto.
// Com 500, os mesmos 14 mil registros cabem em 29 paginas.
// So da pra fazer isso porque o plano pago tirou o teto de 10ms de CPU: com o
// limite antigo, processar 500 registros de uma vez ja estourava sozinho.
const PAGE_SIZE       = 500;
const CACHE_TTL       = 30;        // segundos - cache normal
const STALE_TTL       = 600;       // 10min - cache 'velho aceitavel' quando API falha
const MAX_RETRIES     = 3;         // retry para chamadas individuais a API

// ─── BUSCA INCREMENTAL (parte do snapshot do GitHub Actions) ─────
//
// O fetch completo varria a campanha inteira (data_inicio=2026-01-01) de 100
// em 100 registros a cada cache-miss. Contando canceladas e expiradas isso ja
// passou de cem paginas por acesso e comecou a estourar o limite da Cloudflare
// (erro 1102 -> 503), deixando o dashboard so com o JSON estatico.
//
// Agora: parte do vendas-data.json que o robo Python gera (roda no GitHub
// Actions, sem limite de CPU) e rebusca SO a janela recente. Como a API filtra
// por data, a janela volta em DIAS INTEIROS e cada dia rebuscado SUBSTITUI o
// valor do snapshot em vez de somar. Isso e mais seguro que somar delta: nao
// existe risco de contar duas vezes, e qualquer divergencia se autocorrige na
// proxima vez que o robo rodar. Sem estado proprio no Worker (sem KV).
//
// A janela e sempre [snapshot - SAFETY_DAYS, hoje], entao ela cresce sozinha
// se o robo ficar parado, e cobre venda que mudou de status no periodo.
const BASELINE_URL   = 'https://contato-lab.github.io/festival-interlagos-2026/vendas-data.json';
const SAFETY_DAYS    = 2;    // dias rebuscados antes da data do snapshot
const MAX_DELTA_DAYS = 45;   // janela maior que isso: nao compensa, vai de fetch completo
// Teto de paginas por API, so pra nao existir loop infinito. Fica ALTO de
// proposito: o caminho completo legitimamente precisa de muitas paginas, e
// quando o teto e atingido a funcao LANCA em vez de devolver lista parcial
// (ver getAllVendas). Devolver metade do faturamento com HTTP 200 seria pior
// que falhar: o dashboard so cai no vendas-data.json correto quando da erro.
// Com pagina de 500, isto e um teto de 200 mil registros: folga de sobra pra
// campanha inteira e ainda longe de virar loop infinito.
const MAX_PAGES      = 400;
// Versao da chave de cache. A logica de agregacao mudou, entao a chave muda
// junto: senao um corpo gravado pela versao antiga continua sendo servido do
// edge por ate STALE_TTL depois do deploy.
const CACHE_VER      = 'inc-v4';   // subiu com o page_size 500 e o status finalizado-manual

// ─── O QUE CONTA COMO VENDA ─────────────────────────────
// Mesma regra do robo Python, e tem que continuar igual: se os dois contarem
// diferente, o numero em tempo real briga com o do arquivo e ninguem sabe em
// qual acreditar.
//   3                 venda paga
//   finalizado-manual venda fechada na mao (bilheteria, ajuste). E VENDA.
//   cupom             cortesia de valor zero. NAO e venda.
const STATUS_VENDA = ['3', 'finalizado-manual'];

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

// API v2.0 (29/07/2026): o token deixou de ser publico. Agora e POST com a chave
// no header X-Api-Key e vale 1 HORA. A chave vem do secret do Worker (env.FI_API_KEY),
// configurado com `wrangler secret put FI_API_KEY`. Nunca no codigo.
async function getToken(base, env) {
  const key = env && env.FI_API_KEY;
  if (!key) throw new Error('FI_API_KEY nao configurada no Worker (API do sistema proprio virou v2.0)');
  const r = await fetchWithRetry(
    base + '/apis/token',
    { method: 'POST', headers: { ...buildHeaders(base), 'X-Api-Key': key } },
    `Token ${base}`
  );
  const d = await r.json();
  if (d.status !== 'success' || !d.token) throw new Error('Token status: ' + d.status);
  return d.token;
}

// dataInicio: 'YYYY-MM-DD'. Sem argumento, varre a campanha inteira.
//
// LANCA em vez de devolver lista parcial nos dois casos ruins:
//   - a API respondeu 200 mas com status != success (token vencido, erro no
//     corpo). Antes isso virava "lista vazia", e lista vazia na janela APAGA
//     os dias dela do resultado, derrubando o faturamento sem avisar.
//   - a paginacao bateu no teto com pagina seguinte pendente.
// Falhar aqui e o comportamento certo: o handler serve o cache stale ou 503,
// e os dashboards caem no vendas-data.json inteiro.
async function getAllVendas(base, token, dataInicio) {
  const inicio = dataInicio || DATA_INICIO_STR;
  const fim    = new Date().toISOString().split('T')[0];
  const sales  = [];
  let page     = 1;
  let temMais  = false;   // isNextPage da ultima pagina lida
  while (page <= MAX_PAGES) {
    const url = `${base}/apis/vendas?data_inicio=${inicio}&data_fim=${fim}&page_size=${PAGE_SIZE}&page=${page}`;
    const r   = await fetchWithRetry(url, { headers: buildHeaders(base, token) }, `Vendas ${base} pag.${page}`);
    const d = await r.json();
    if (d.status !== 'success') {
      throw new Error(`Vendas ${base} pag.${page}: status "${d.status}" (esperado success)`);
    }
    if (!d.data || !d.data.length) { temMais = false; break; }
    sales.push(...d.data);
    temMais = d.pagination?.isNextPage === 'Y';
    if (!temMais) break;
    page++;
  }
  if (temMais) {
    throw new Error(
      `Vendas ${base}: paginacao truncada em ${MAX_PAGES} paginas ` +
      `(${sales.length} registros desde ${inicio}); recusando resultado parcial`
    );
  }
  return sales;
}

// ─── AGREGACAO ───────────────────────────────────────────
function aggregateByDay(sales) {
  const byDay = {};
  for (const v of sales) {
    if (!STATUS_VENDA.includes(String(v.venda_status))) continue;
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

// Retorna o ISO UTC do created_at mais recente (BRT → UTC)
function lastSaleAt(sales) {
  let latest = null;
  for (const v of sales) {
    if (!STATUS_VENDA.includes(String(v.venda_status))) continue;
    const ts = (v.created_at || '').trim();
    if (!ts) continue;
    if (!latest || ts > latest) latest = ts;
  }
  if (!latest) return null;
  try {
    // created_at vem em BRT (UTC-3): "2026-05-15 14:23:45"
    return new Date(latest.replace(' ', 'T') + '-03:00').toISOString();
  } catch { return null; }
}

// ─── HELPERS DE DATA / MONTAGEM ──────────────────────────
function dataDoDia(d) {
  const dt = new Date(DATA_INICIO);
  dt.setUTCDate(dt.getUTCDate() + d - 1);
  return dt.toISOString().split('T')[0];
}

function diaDeCampanha(ds) {
  const dt = new Date(ds + 'T00:00:00Z');
  if (isNaN(dt)) return null;
  return Math.floor((dt - DATA_INICIO) / 86400000) + 1;
}

// Monta a resposta final a partir de um Map(d -> linha), recalculando os totais
// pela soma dos dias. Recalcular (em vez de somar delta em cima do total antigo)
// garante que totals sempre bate com daily, mesmo se a janela mexeu em varios dias.
function montarSaida(mapaDias, lastMoto, lastAuto, source) {
  const daily = [...mapaDias.values()].sort((a, b) => a.d - b.d);
  const totals = { moto_receita: 0, auto_receita: 0, moto_ingressos: 0, auto_ingressos: 0 };
  for (const r of daily) {
    totals.moto_receita   += r.moto_receita   || 0;
    totals.moto_ingressos += r.moto_ingressos || 0;
    totals.auto_receita   += r.auto_receita   || 0;
    totals.auto_ingressos += r.auto_ingressos || 0;
  }
  totals.moto_receita    = Math.round(totals.moto_receita * 100) / 100;
  totals.auto_receita    = Math.round(totals.auto_receita * 100) / 100;
  totals.total_receita   = Math.round((totals.moto_receita + totals.auto_receita) * 100) / 100;
  totals.total_ingressos = totals.moto_ingressos + totals.auto_ingressos;

  return {
    updated_at:        new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'),
    source,
    campaign_start:    '2026-03-31',
    last_sale_moto_at: lastMoto,
    last_sale_auto_at: lastAuto,
    totals,
    daily,
  };
}

// Junta os dois lados (moto/auto) num Map(d -> linha) no formato de saida.
function mapaDeVendas(motoByDay, autoByDay) {
  const mapa = new Map();
  const dias = new Set([
    ...Object.keys(motoByDay).map(Number),
    ...Object.keys(autoByDay).map(Number),
  ]);
  for (const d of dias) {
    const m = motoByDay[d] || { receita: 0, qtd: 0 };
    const a = autoByDay[d] || { receita: 0, qtd: 0 };
    mapa.set(d, {
      d,
      date:           dataDoDia(d),
      moto_receita:   Math.round(m.receita * 100) / 100,
      moto_ingressos: m.qtd,
      auto_receita:   Math.round(a.receita * 100) / 100,
      auto_ingressos: a.qtd,
    });
  }
  return mapa;
}

function maisRecente(a, b) {
  if (!a) return b || null;
  if (!b) return a;
  return b > a ? b : a;
}

// ─── SNAPSHOT DO GITHUB (baseline do incremental) ────────
async function fetchBaseline() {
  const r = await fetch(BASELINE_URL, { cf: { cacheTtl: 60, cacheEverything: true } });
  if (!r.ok) throw new Error('baseline HTTP ' + r.status);
  const d = await r.json();
  if (!d || !Array.isArray(d.daily) || !d.daily.length) throw new Error('baseline sem daily');
  if (!d.updated_at) throw new Error('baseline sem updated_at');
  return d;
}

// Primeira data a rebuscar: dia do snapshot (em BRT) menos SAFETY_DAYS.
// updated_at vem em UTC e o created_at da API vem em BRT (UTC-3), por isso o -3h
// antes de cortar a data: perto da meia-noite os dois nao caem no mesmo dia.
function dataCorteISO(updatedAt) {
  const t = new Date(updatedAt).getTime();
  if (!isFinite(t)) return null;
  // Snapshot com carimbo no futuro (relogio errado, ou edicao manual do JSON)
  // empurraria a janela pra frente e os dias recentes sumiriam do resultado.
  // Trava no agora: no pior caso rebusca de mais, nunca de menos.
  const base = Math.min(t, Date.now());
  const brt  = new Date(base - 3 * 3600 * 1000 - SAFETY_DAYS * 86400000);
  return brt.toISOString().split('T')[0];
}

async function buildVendasDataIncremental(env) {
  const baseline = await fetchBaseline();

  const corteISO = dataCorteISO(baseline.updated_at);
  if (!corteISO) throw new Error('baseline com updated_at invalido');
  const corteDia = diaDeCampanha(corteISO);
  if (corteDia == null) throw new Error('data de corte invalida');

  const hojeDia = diaDeCampanha(new Date(Date.now() - 3 * 3600 * 1000).toISOString().split('T')[0]);
  if (hojeDia == null) throw new Error('nao consegui calcular o dia de hoje');
  // Corte depois de hoje nao rebuscaria nada e ainda apagaria os dias recentes
  // do snapshot (eles caem fora do passo 1 e nao voltam no passo 2).
  if (corteDia > hojeDia) throw new Error('data de corte no futuro');
  // Snapshot velho demais: a janela ficaria grande e o incremental perde a graca.
  if (hojeDia - corteDia > MAX_DELTA_DAYS) {
    throw new Error(`janela de ${hojeDia - corteDia} dias passa do limite de ${MAX_DELTA_DAYS}`);
  }

  const [motoToken, autoToken] = await Promise.all([getToken(API_MOTO, env), getToken(API_AUTO, env)]);
  const [motoSales, autoSales] = await Promise.all([
    getAllVendas(API_MOTO, motoToken, corteISO),
    getAllVendas(API_AUTO, autoToken, corteISO),
  ]);

  const mapa = new Map();
  // 1) dias anteriores a janela: valem os do snapshot
  for (const row of baseline.daily) {
    if (typeof row.d === 'number' && row.d < corteDia) mapa.set(row.d, { ...row });
  }
  // 2) dias da janela: valem os que acabaram de vir da API (SUBSTITUEM o snapshot).
  //    Dia da janela sem venda nenhuma simplesmente nao entra, que e o certo:
  //    a linha antiga dele ja ficou de fora no passo 1.
  //
  //    A GUARDA d >= corteDia NAO E OPCIONAL. O filtro data_inicio/data_fim da
  //    API nao corta por created_at: venda antiga que foi MEXIDA agora volta na
  //    janela. Sem a guarda, o dia dela (que esta fora da janela) era
  //    substituido por essa fatia de um registro so. Aconteceu de verdade em
  //    producao: D+1 caiu de R$ 625.255,11 / 1043 ingressos para R$ 571,06 / 1,
  //    e D+121 de R$ 12.150,77 / 16 para R$ 730,81 / 1. Dia fora da janela fica
  //    com o valor do snapshot, que veio de varredura completa e esta inteiro.
  const novos = mapaDeVendas(aggregateByDay(motoSales), aggregateByDay(autoSales));
  for (const [d, row] of novos) {
    if (d >= corteDia) mapa.set(d, row);
  }

  return montarSaida(
    mapa,
    maisRecente(baseline.last_sale_moto_at, lastSaleAt(motoSales)),
    maisRecente(baseline.last_sale_auto_at, lastSaleAt(autoSales)),
    `Cloudflare Worker (incremental desde ${corteISO}, sobre snapshot de ${baseline.updated_at})`
  );
}

async function buildVendasDataCompleto(env) {
  const [motoToken, autoToken] = await Promise.all([getToken(API_MOTO, env), getToken(API_AUTO, env)]);
  const [motoSales, autoSales] = await Promise.all([
    getAllVendas(API_MOTO, motoToken),
    getAllVendas(API_AUTO, autoToken),
  ]);
  return montarSaida(
    mapaDeVendas(aggregateByDay(motoSales), aggregateByDay(autoSales)),
    lastSaleAt(motoSales),
    lastSaleAt(autoSales),
    'Cloudflare Worker (fetch completo, sem baseline)'
  );
}

async function buildVendasData(env) {
  // Caminho leve: snapshot + janela recente. Poucas paginas por acesso.
  let motivo = null;
  try {
    return await buildVendasDataIncremental(env);
  } catch (err) {
    // Sem snapshot utilizavel (1a implantacao, Pages fora do ar, snapshot velho
    // demais): cai pro fetch completo, que e pesado mas nao depende de ninguem.
    // O motivo vai junto na resposta: engolir o erro em silencio deixava
    // impossivel saber, olhando o JSON, que o worker foi pro caminho pesado.
    motivo = (err && err.message) ? err.message : String(err);
  }
  const completo = await buildVendasDataCompleto(env);
  completo.fallback_reason = motivo;
  completo.source += ` [incremental indisponivel: ${motivo}]`;
  return completo;
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
    const cacheKey      = new Request(baseUrl + '?v=fresh-' + CACHE_VER, request);
    const staleCacheKey = new Request(baseUrl + '?v=stale-' + CACHE_VER, request);

    // 1) Cache fresco?
    let cached = await cache.match(cacheKey);
    if (cached) {
      const r2 = new Response(cached.body, cached);
      r2.headers.set('X-Cache', 'HIT');
      return r2;
    }

    // 2) Tenta API
    try {
      const data = await buildVendasData(env);
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
