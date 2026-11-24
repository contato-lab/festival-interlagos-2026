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
// Duas contas somadas: a antiga ficou parada pela verificação do anunciante e a nova assumiu em 16/11/2026.
// Nenhuma tem MCC vinculada: o login é direto em cada uma.
const GOOGLE_CONTAS = [
  { id: '9618420419', nome: 'VOGE BRASIL (VERIFICADA)' },   // conta nova, 961-842-0419
  { id: '6244436447', nome: 'VOGE Brasil (antiga)' },        // 624-443-6447, mantida pelo histórico
];
const SINCE       = '2026-11-01';   // início da campanha VOGE Sales Speed
const GRAPH       = 'https://graph.facebook.com/v20.0';
const GOOGLE_API  = 'https://googleads.googleapis.com/v26';

const CACHE_TTL   = 45;                 // segundos, menor que o intervalo de 1 minuto do painel
const STALE_TTL   = 7 * 24 * 60 * 60;   // 7 dias, reserva para quando a API falhar
const CACHE_VER   = 'v3';

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

// Uma linha de insights da Meta vira um dia no formato do painel (vale para conta e para campanha)
function diaMeta(d) {
  const form = contar(d.actions, TIPOS_LEAD), contatos = contar(d.actions, TIPOS_CONTATO);
  const leads = form + contatos, gasto = dinheiro(d.spend);
  return {
    data: dd_mm(d.date_start), dataISO: d.date_start, leads, leadsForm: form, contatos, gasto,
    cpl: leads ? dinheiro(gasto / leads) : null,
    impressoes: Number(d.impressions || 0), alcance: Number(d.reach || 0), cliques: Number(d.clicks || 0),
    lpViews: contar(d.actions, ['landing_page_view']), viewContent: contar(d.actions, TIPOS_VC),
  };
}

// "data" é a conta inteira; "campanhas" traz os mesmos números separados por campanha, para o filtro do painel.
// Alcance por campanha não soma com o da conta (a mesma pessoa pode ver duas campanhas).
async function gerarMeta(env) {
  const [linhas, porCampanha] = await Promise.all([
    graphTudo(urlInsights(env, 'account', 'date_start,spend,impressions,reach,clicks,actions')),
    graphTudo(urlInsights(env, 'campaign', 'campaign_id,campaign_name,date_start,spend,impressions,reach,clicks,actions')),
  ]);
  const campanhas = {};
  for (const l of porCampanha) {
    const c = campanhas[l.campaign_id] || (campanhas[l.campaign_id] = { id: l.campaign_id, nome: l.campaign_name || '', dias: [] });
    c.dias.push(diaMeta(l));
  }
  return { updated: carimbo(), account: 'VOGE Brasil | ' + META_ACCT, since: SINCE, data: linhas.map(diaMeta), campanhas: Object.values(campanhas) };
}

async function gerarCriativos(env) {
  const linhas = await graphTudo(urlInsights(env, 'ad', 'ad_id,ad_name,campaign_name,date_start,spend,impressions,clicks,actions'));
  const ads = {};
  for (const l of linhas) {
    const ad = ads[l.ad_id] || (ads[l.ad_id] = { id: l.ad_id, nome: l.ad_name || '', campanha: l.campaign_name || '', status: '', thumb: '', dias: [] });
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
async function tokenGoogle(env) {
  const oauth = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token', client_id: env.GOOGLE_ADS_CLIENT_ID,
      client_secret: env.GOOGLE_ADS_CLIENT_SECRET, refresh_token: env.GOOGLE_ADS_REFRESH_TOKEN,
    }),
  });
  if (!oauth.ok) await falha('Google OAuth', oauth);
  return (await oauth.json()).access_token;
}

// Roda a mesma consulta nas duas contas ao mesmo tempo. Conta que falhar volta com "erro" em vez de derrubar a outra.
function consultarContas(env, access_token, query) {
  return Promise.all(GOOGLE_CONTAS.map(async conta => {
    const r = await fetch(`${GOOGLE_API}/customers/${conta.id}/googleAds:searchStream`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${access_token}`, 'developer-token': env.GOOGLE_ADS_DEVELOPER_TOKEN,
        'login-customer-id': conta.id, 'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query }),
    });
    if (!r.ok) return { conta, erro: `HTTP ${r.status}: ${(await r.text()).slice(0, 300)}` };
    return { conta, lotes: await r.json() };
  }));
}
const linhasDe = lotes => (lotes || []).flatMap(l => l.results || []);

async function gerarGoogle(env) {
  const access_token = await tokenGoogle(env);
  const query = `SELECT campaign.id, campaign.name, campaign.status, segments.date,
      metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions
    FROM campaign
    WHERE segments.date BETWEEN '${SINCE}' AND '${hojeBRT()}' AND campaign.status != 'REMOVED'
    ORDER BY segments.date ASC`;
  const respostas = await consultarContas(env, access_token, query);
  const erros = respostas.filter(x => x.erro).map(x => ({ conta: x.conta.id, erro: x.erro }));
  if (erros.length === GOOGLE_CONTAS.length) throw new Error('Google Ads: ' + erros.map(e => `${e.conta} ${e.erro}`).join(' | '));
  erros.forEach(e => console.error('Google Ads conta', e.conta, e.erro));

  const porDia = {}, campanhas = {};
  for (const { conta, lotes } of respostas.filter(x => !x.erro)) {
    for (const row of linhasDe(lotes)) {
      const iso = row.segments.date, m = row.metrics || {}, c0 = row.campaign;
      const custo = Number(m.costMicros || 0) / 1e6, imp = Number(m.impressions || 0),
            clk = Number(m.clicks || 0), conv = Number(m.conversions || 0);
      const d = porDia[iso] || (porDia[iso] = { dataISO: iso, data: dd_mm(iso), gasto: 0, impressoes: 0, cliques: 0, conversoes: 0 });
      d.gasto += custo; d.impressoes += imp; d.cliques += clk; d.conversoes += conv;
      const c = campanhas[c0.id] || (campanhas[c0.id] = { id: c0.id, conta: conta.id, nome: c0.name, status: c0.status, gasto: 0, impressoes: 0, cliques: 0, conversoes: 0, dias: [] });
      c.gasto += custo; c.impressoes += imp; c.cliques += clk; c.conversoes += conv;
      c.dias.push({ dataISO: iso, data: dd_mm(iso), gasto: dinheiro(custo), impressoes: imp, cliques: clk, conversoes: dinheiro(conv) });
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
  return { updated: carimbo(), account: 'VOGE Brasil | ' + GOOGLE_CONTAS.map(c => c.id).join(' + '), since: SINCE, campanhas: lista, data, erros };
}

/* Imagens e vídeos que estão nas campanhas do Google (Performance Max e imagens de campanha).
 * A mesma imagem em vários grupos de recursos vira um item só.
 * Logos ficam de fora: não são o criativo.
 * Números por imagem vêm de uma segunda consulta, opcional: se o Google não entregar, o card mostra só a imagem. */
const FORMATOS = {
  MARKETING_IMAGE: 'Horizontal', SQUARE_MARKETING_IMAGE: 'Quadrada', PORTRAIT_MARKETING_IMAGE: 'Vertical',
  TALL_PORTRAIT_MARKETING_IMAGE: 'Vertical', AD_IMAGE: 'Imagem', YOUTUBE_VIDEO: 'Vídeo',
};
async function gerarGoogleCriativos(env) {
  const access_token = await tokenGoogle(env);
  const [doGrupo, daCampanha, numeros] = await Promise.all([
    consultarContas(env, access_token, `SELECT campaign.id, campaign.name, campaign.status,
        asset_group.name, asset_group.status, asset_group_asset.field_type, asset_group_asset.status,
        asset.id, asset.name, asset.type, asset.image_asset.full_size.url,
        asset.image_asset.full_size.width_pixels, asset.image_asset.full_size.height_pixels,
        asset.youtube_video_asset.youtube_video_id, asset.youtube_video_asset.youtube_video_title
      FROM asset_group_asset
      WHERE asset.type IN ('IMAGE', 'YOUTUBE_VIDEO') AND asset_group_asset.status != 'REMOVED'
        AND campaign.status != 'REMOVED'`),
    consultarContas(env, access_token, `SELECT campaign.id, campaign.name, campaign.status,
        campaign_asset.field_type, campaign_asset.status,
        asset.id, asset.name, asset.type, asset.image_asset.full_size.url,
        asset.image_asset.full_size.width_pixels, asset.image_asset.full_size.height_pixels
      FROM campaign_asset
      WHERE asset.type = 'IMAGE' AND campaign_asset.status != 'REMOVED' AND campaign.status != 'REMOVED'`),
    consultarContas(env, access_token, `SELECT asset.id, segments.date,
        metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions
      FROM asset_group_asset
      WHERE segments.date BETWEEN '${SINCE}' AND '${hojeBRT()}' AND asset.type IN ('IMAGE', 'YOUTUBE_VIDEO')`),
  ]);

  const itens = {};
  const juntar = (row, tipoCampo, ativoNoGrupo, grupo) => {
    const a = row.asset || {}, c = row.campaign || {};
    const formato = FORMATOS[tipoCampo];
    if (!formato) return;   // logo, ícone e afins
    const img = (a.imageAsset || {}).fullSize || {}, yt = a.youtubeVideoAsset || {};
    const thumb = img.url || (yt.youtubeVideoId ? `https://i.ytimg.com/vi/${yt.youtubeVideoId}/hqdefault.jpg` : '');
    if (!thumb) return;
    const it = itens[a.id] || (itens[a.id] = {
      id: a.id, nome: a.name || yt.youtubeVideoTitle || '', formato, thumb,
      largura: Number(img.widthPixels || 0), altura: Number(img.heightPixels || 0),
      video: yt.youtubeVideoId ? `https://www.youtube.com/watch?v=${yt.youtubeVideoId}` : '',
      ativo: false, campanhas: [], grupos: [], dias: [],
    });
    if (!it.campanhas.includes(c.name)) it.campanhas.push(c.name);
    if (grupo && !it.grupos.includes(grupo)) it.grupos.push(grupo);
    if (c.status === 'ENABLED' && ativoNoGrupo) it.ativo = true;
  };
  const erros = [];
  for (const x of [...doGrupo, ...daCampanha]) if (x.erro) erros.push({ conta: x.conta.id, erro: x.erro });
  for (const x of doGrupo.filter(x => !x.erro)) for (const row of linhasDe(x.lotes)) {
    const ag = row.assetGroup || {}, aga = row.assetGroupAsset || {};
    juntar(row, aga.fieldType, aga.status === 'ENABLED' && ag.status === 'ENABLED', ag.name);
  }
  for (const x of daCampanha.filter(x => !x.erro)) for (const row of linhasDe(x.lotes)) {
    const ca = row.campaignAsset || {};
    juntar(row, ca.fieldType === 'AD_IMAGE' ? 'AD_IMAGE' : ca.fieldType, ca.status === 'ENABLED', '');
  }
  let comNumeros = false;
  for (const x of numeros.filter(x => !x.erro)) for (const row of linhasDe(x.lotes)) {
    const it = itens[(row.asset || {}).id]; if (!it) continue;
    const m = row.metrics || {}, iso = row.segments.date;
    let d = it.dias.find(z => z.iso === iso);
    if (!d) it.dias.push(d = { iso, gasto: 0, impressoes: 0, cliques: 0, conversoes: 0 });
    d.gasto = dinheiro(d.gasto + Number(m.costMicros || 0) / 1e6);
    d.impressoes += Number(m.impressions || 0); d.cliques += Number(m.clicks || 0);
    d.conversoes = dinheiro(d.conversoes + Number(m.conversions || 0));
    comNumeros = true;
  }
  numeros.filter(x => x.erro).forEach(x => console.error('Google números por imagem', x.conta.id, x.erro));
  if (erros.length && !Object.keys(itens).length && erros.length === GOOGLE_CONTAS.length * 2)
    throw new Error('Google criativos: ' + erros.map(e => `${e.conta} ${e.erro}`).join(' | '));
  const lista = Object.values(itens).sort((a, b) => (b.ativo - a.ativo) || a.formato.localeCompare(b.formato));
  return { updated: carimbo(), since: SINCE, comNumeros, itens: lista, erros };
}

/* ── ROTAS ── */
const ROTAS = {
  '/meta-data.json':       gerarMeta,
  '/google-ads-data.json': gerarGoogle,
  '/meta-criativos.json':  gerarCriativos,
  '/google-criativos.json': gerarGoogleCriativos,
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
