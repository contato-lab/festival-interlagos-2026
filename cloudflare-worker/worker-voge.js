/**
 * Dados do dashboard de leads da VOGE Brasil (contato-lab.github.io/dash-voge).
 *
 * Por que Worker e não robô do GitHub: o agendamento do GitHub Actions neste repositório
 * pede 5 minutos e chega a demorar de 3 a 10 horas (medido em 09/11/2026).
 * Aqui o painel pede o dado a cada 1 minuto, e o Worker busca na Meta e no Google e guarda por 45 segundos.
 * Os 45 segundos protegem as APIs quando várias pessoas abrem o painel ao mesmo tempo.
 * Se uma API falhar, entrega a última resposta boa em vez de deixar o painel vazio.
 *
 * Lead Meta = envio de formulário (evento Lead) + contato e WhatsApp (evento Contact).
 * Lead Google = metrics.conversions, que só traz as conversões principais da conta.
 *
 * Chaves ficam só nos secrets, cadastradas pelo deploy-voge-worker.yml. Este repositório é público.
 */
const META_ACCT   = 'act_1397071468729208';
const GOOGLE_ID   = '6244436447';   // a conta não tem MCC vinculada: login direto nela
const SINCE       = '2026-11-01';   // início da campanha VOGE Sales Speed
const GRAPH       = 'https://graph.facebook.com/v20.0';
const GOOGLE_API  = 'https://googleads.googleapis.com/v26';

const CACHE_TTL   = 45;                 // segundos, menor que o intervalo de 1 minuto do painel
const STALE_TTL   = 7 * 24 * 60 * 60;   // 7 dias, reserva para quando a API falhar
const CACHE_VER   = 'v2';

// O nome do evento muda conforme a origem. Vale o primeiro que existir, para não contar duas vezes.
const TIPOS_LEAD    = ['lead', 'offsite_conversion.fb_pixel_lead'];
const TIPOS_CONTATO = ['contact_total', 'contact_website', 'contact', 'offsite_conversion.fb_pixel_contact'];
const TIPOS_VC      = ['view_content', 'omni_view_content', 'offsite_conversion.fb_pixel_view_content'];

const agoraBRT = () => new Date(Date.now() - 3 * 3600 * 1000).toISOString();   // só para ler data e hora
const hojeBRT  = () => agoraBRT().slice(0, 10);
function carimbo() {
  const s = agoraBRT();
  return `${s.slice(8, 10)}/${s.slice(5, 7)}/${s.slice(0, 4)} ${s.slice(11, 16)}`;
}
const dd_mm = iso => `${iso.slice(8, 10)}/${iso.slice(5, 7)}`;
const dinheiro = v => Math.round(Number(v || 0) * 100) / 100;

function contar(actions, tipos) {
  const valores = Object.fromEntries((actions || []).map(a => [a.action_type, a.value]));
  for (const t of tipos) if (t in valores) return Math.trunc(Number(valores[t]));
  return 0;
}

async function falha(origem, r) {
  console.error(origem, r.status, (await r.text()).slice(0, 400));
  throw new Error(`${origem} HTTP ${r.status}`);
}

/* ── META ── */
async function graphTudo(url) {
  const linhas = [];
  while (url) {
    const r = await fetch(url);
    if (!r.ok) await falha('Meta', r);
    const j = await r.json();
    linhas.push(...(j.data || []));
    url = j.paging && j.paging.next;
  }
  return linhas;
}

function urlInsights(env, level, fields) {
  const q = new URLSearchParams({
    fields, level, time_increment: '1', limit: '500',
    time_range: JSON.stringify({ since: SINCE, until: hojeBRT() }),
    access_token: env.META_TOKEN,
  });
  return `${GRAPH}/${META_ACCT}/insights?${q}`;
}

async function gerarMeta(env) {
  const linhas = await graphTudo(urlInsights(env, 'account', 'date_start,spend,impressions,reach,clicks,actions'));
  const data = linhas.map(d => {
    const form = contar(d.actions, TIPOS_LEAD), contatos = contar(d.actions, TIPOS_CONTATO);
    const leads = form + contatos, gasto = dinheiro(d.spend);
    return {
      data: dd_mm(d.date_start), dataISO: d.date_start, leads, leadsForm: form, contatos, gasto,
      cpl: leads ? dinheiro(gasto / leads) : null,
      impressoes: Number(d.impressions || 0), alcance: Number(d.reach || 0), cliques: Number(d.clicks || 0),
      lpViews: contar(d.actions, ['landing_page_view']), viewContent: contar(d.actions, TIPOS_VC),
    };
  });
  return { updated: carimbo(), account: 'VOGE Brasil | ' + META_ACCT, since: SINCE, data };
}

async function gerarCriativos(env) {
  const linhas = await graphTudo(urlInsights(env, 'ad', 'ad_id,ad_name,date_start,spend,impressions,clicks,actions'));
  const ads = {};
  for (const l of linhas) {
    const ad = ads[l.ad_id] || (ads[l.ad_id] = { id: l.ad_id, nome: l.ad_name || '', status: '', thumb: '', dias: [] });
    ad.dias.push({
      iso: l.date_start, gasto: dinheiro(l.spend),
      impressoes: Number(l.impressions || 0), cliques: Number(l.clicks || 0),
      leads: contar(l.actions, TIPOS_LEAD) + contar(l.actions, TIPOS_CONTATO),
    });
  }
  try {   // sem imagem o card ainda mostra os números
    const q = new URLSearchParams({
      fields: 'id,effective_status,creative.thumbnail_width(600).thumbnail_height(600){thumbnail_url,image_url}',
      limit: '200', access_token: env.META_TOKEN,
    });
    for (const info of await graphTudo(`${GRAPH}/${META_ACCT}/ads?${q}`)) {
      const ad = ads[info.id]; if (!ad) continue;
      const c = info.creative || {};
      ad.status = info.effective_status || '';
      ad.thumb = c.thumbnail_url || c.image_url || '';
    }
  } catch (e) { console.error('Imagens dos criativos:', e.message); }
  return { updated: carimbo(), since: SINCE, ads: Object.values(ads) };
}

/* ── GOOGLE ── */
async function gerarGoogle(env) {
  const oauth = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token', client_id: env.GOOGLE_ADS_CLIENT_ID,
      client_secret: env.GOOGLE_ADS_CLIENT_SECRET, refresh_token: env.GOOGLE_ADS_REFRESH_TOKEN,
    }),
  });
  if (!oauth.ok) await falha('Google OAuth', oauth);
  const { access_token } = await oauth.json();

  const query = `SELECT campaign.id, campaign.name, campaign.status, segments.date,
      metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions
    FROM campaign
    WHERE segments.date BETWEEN '${SINCE}' AND '${hojeBRT()}' AND campaign.status != 'REMOVED'
    ORDER BY segments.date ASC`;
  const r = await fetch(`${GOOGLE_API}/customers/${GOOGLE_ID}/googleAds:searchStream`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${access_token}`, 'developer-token': env.GOOGLE_ADS_DEVELOPER_TOKEN,
      'login-customer-id': GOOGLE_ID, 'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query }),
  });
  if (!r.ok) await falha('Google Ads', r);

  const porDia = {}, campanhas = {};
  for (const lote of await r.json()) {
    for (const row of lote.results || []) {
      const iso = row.segments.date, m = row.metrics || {}, c0 = row.campaign;
      const custo = Number(m.costMicros || 0) / 1e6, imp = Number(m.impressions || 0),
            clk = Number(m.clicks || 0), conv = Number(m.conversions || 0);
      const d = porDia[iso] || (porDia[iso] = { dataISO: iso, data: dd_mm(iso), gasto: 0, impressoes: 0, cliques: 0, conversoes: 0 });
      d.gasto += custo; d.impressoes += imp; d.cliques += clk; d.conversoes += conv;
      const c = campanhas[c0.id] || (campanhas[c0.id] = { id: c0.id, nome: c0.name, status: c0.status, gasto: 0, impressoes: 0, cliques: 0, conversoes: 0 });
      c.gasto += custo; c.impressoes += imp; c.cliques += clk; c.conversoes += conv;
    }
  }
  const data = Object.values(porDia).sort((a, b) => (a.dataISO < b.dataISO ? -1 : 1)).map(d => ({
    ...d, gasto: dinheiro(d.gasto), conversoes: dinheiro(d.conversoes),
    ctr: d.impressoes ? dinheiro(d.cliques / d.impressoes * 100) : 0,
    cpc: d.cliques ? dinheiro(d.gasto / d.cliques) : null,
    cpa: d.conversoes ? dinheiro(d.gasto / d.conversoes) : null,
  }));
  const lista = Object.values(campanhas).map(c => ({ ...c, gasto: dinheiro(c.gasto), conversoes: dinheiro(c.conversoes) }))
    .sort((a, b) => b.gasto - a.gasto);
  return { updated: carimbo(), account: 'VOGE Brasil | ' + GOOGLE_ID, since: SINCE, campanhas: lista, data };
}

/* ── ROTAS ── */
const ROTAS = {
  '/meta-data.json':       gerarMeta,
  '/google-ads-data.json': gerarGoogle,
  '/meta-criativos.json':  gerarCriativos,
};

const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS' };

function respostaJson(body, maxAge, xCache) {
  return new Response(body, {
    headers: { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': `public, max-age=${maxAge}`, 'X-Cache': xCache },
  });
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });
    const url = new URL(request.url);
    const gerar = ROTAS[url.pathname];
    if (request.method !== 'GET' || !gerar) return new Response('Not found', { status: 404, headers: CORS });

    const cache = caches.default;
    const base = url.origin + url.pathname;
    const chaveNova  = new Request(`${base}?v=novo-${CACHE_VER}`);
    const chaveVelha = new Request(`${base}?v=reserva-${CACHE_VER}`);

    const nova = await cache.match(chaveNova);
    if (nova) return respostaJson(await nova.text(), 60, 'HIT');

    try {
      const body = JSON.stringify(await gerar(env));
      ctx.waitUntil(cache.put(chaveNova, respostaJson(body, CACHE_TTL, 'MISS')));
      ctx.waitUntil(cache.put(chaveVelha, respostaJson(body, STALE_TTL, 'MISS')));
      return respostaJson(body, 60, 'MISS');
    } catch (e) {
      console.error(url.pathname, e.message);
      const velha = await cache.match(chaveVelha);
      if (velha) return respostaJson(await velha.text(), 60, 'STALE');
      return new Response(JSON.stringify({ erro: 'dados indisponiveis' }), {
        status: 502, headers: { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' },
      });
    }
  },
};
