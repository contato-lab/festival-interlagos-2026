/**
 * Ponte de leads da VOGE Brasil: formulários da Meta e do Google Ads → CRM Dynamo Vendas (TecSinapse).
 *
 * Meta: a cada 2 minutos o Worker lê os formulários ativos da página Voge Brasil e envia os leads
 *       das últimas 24 horas que ainda não foram enviados (o KV guarda o que já foi).
 * Google: o formulário de lead do Google Ads chama /google-webhook na hora em que o lead chega.
 *
 * Cada loja tem um dealerId e um token próprio. O lead vai para a loja escolhida no formulário;
 * sem resposta, tenta pela cidade e estado. Sem loja definida, fica guardado como pendente.
 *
 * Chaves ficam só nos secrets, cadastradas pelo deploy-voge-leads.yml. Este repositório é público.
 *   META_LEADS_TOKEN  token com leads_retrieval na página Voge Brasil (se faltar, usa META_TOKEN)
 *   VOGE_LOJAS_TOKENS JSON {"7":"token", "6":"token", ...} com o token de formulário de cada loja
 *   GOOGLE_LEAD_KEY   chave combinada com o formulário do Google Ads
 */
const GRAPH       = 'https://graph.facebook.com/v20.0';
const PAGE_ID     = '898107846725198';   // página Voge Brasil
const TECSINAPSE  = 'https://integration.tecsinapse.com';
const ENVIO_URL   = TECSINAPSE + '/public/voge/lead/web';
const JANELA_H    = 24;                  // horas de leads da Meta relidas a cada rodada
const GUARDA_DIAS = 90;                  // por quanto tempo o KV lembra que um lead já foi enviado

// O que vai no campo ORIGEM e na categoria. Confirmar os textos com a TecSinapse.
const ORIGEM = { fb: 'Facebook', ig: 'Instagram', meta: 'Meta Ads', google: 'Google Ads' };
const CATEGORIA_ORIGEM = 'Mídia Paga';

// Lojas da planilha "Token de Integração Voge" (13/11/2026). O token de cada uma fica no secret.
const LOJAS = [
  { dealerId: 7, nome: 'Brasília',       chaves: ['brasilia', 'distrito federal'], uf: ['DF'] },
  { dealerId: 6, nome: 'Campinas',       chaves: ['campinas'],                     cidades: ['campinas'] },
  { dealerId: 2, nome: 'Curitiba',       chaves: ['curitiba'],                     uf: ['PR'] },
  { dealerId: 4, nome: 'Rio de Janeiro', chaves: ['rio de janeiro'], exatas: ['rio', 'rj'], uf: ['RJ'] },
  { dealerId: 5, nome: 'São Paulo',      chaves: ['sao paulo'],                    uf: ['SP'] },
];

const semAcento = s => (s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
const digitos = s => (s || '').replace(/\D/g, '');

function celularBR(tel) {
  let d = digitos(tel);
  if (d.length > 11 && d.startsWith('55')) d = d.slice(2);
  return d;
}

/* Loja escolhida no formulário; se não houver, pela cidade e depois pelo estado */
function acharLoja({ resposta, cidade, uf }) {
  const r = semAcento(resposta);
  if (r) {
    // "rio" só vale sozinho, para "Rio Claro" não cair no Rio de Janeiro
    for (const l of LOJAS) {
      if (l.chaves.some(c => r.includes(c)) || (l.exatas || []).includes(r)) return l;
    }
  }
  const c = semAcento(cidade), u = (uf || '').toUpperCase().trim();
  for (const l of LOJAS) if (l.cidades && l.cidades.includes(c)) return l;
  for (const l of LOJAS) if (l.uf && l.uf.includes(u)) return l;
  return null;
}

/* ── ENVIO PARA A TECSINAPSE ── */
async function enviarCRM(env, lead) {
  const tokens = JSON.parse(env.VOGE_LOJAS_TOKENS || '{}');
  const token = tokens[String(lead.dealerId)];
  if (!token) throw new Error(`sem token para a loja ${lead.dealerId}`);
  const r = await fetch(ENVIO_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ...lead, tokenFormulario: token }),
  });
  const texto = await r.text();
  if (r.status !== 202) return { situacao: 'RECUSADO_NO_ENVIO', http: r.status, detalhe: texto.slice(0, 300) };
  const j = JSON.parse(texto);
  return { situacao: 'ACEITO', codigoRastreio: j.codigoRastreio, trackingStatusUrl: j.trackingStatusUrl };
}

/* O 202 só quer dizer que entrou na fila. O resultado final sai no trackingStatusUrl. */
async function acompanhar(trackingStatusUrl, tentativas = 5) {
  for (let i = 0; i < tentativas; i++) {
    await new Promise(ok => setTimeout(ok, 5000));
    const r = await fetch(TECSINAPSE + trackingStatusUrl, { redirect: 'manual' });
    if (r.status === 302) return { situacao: 'INTEGRADO' };
    const j = await r.json().catch(() => ({}));
    if (r.ok && j.status === 'PROCESSING') continue;
    if (r.ok) return { situacao: 'INTEGRADO' };
    return { situacao: 'REJEITADO', http: r.status, detalhe: (j.message || '').slice(0, 300) };
  }
  return { situacao: 'PROCESSANDO' };
}

async function processar(env, chave, lead) {
  if (await env.LEADS.get(chave)) return 'ja_enviado';
  if (!lead.dealerId) {
    await env.LEADS.put('pendente:' + chave, JSON.stringify(lead), { expirationTtl: GUARDA_DIAS * 86400 });
    await env.LEADS.put(chave, JSON.stringify({ situacao: 'SEM_LOJA', em: new Date().toISOString() }), { expirationTtl: GUARDA_DIAS * 86400 });
    console.error('Lead sem loja definida, guardado como pendente:', chave);
    return 'sem_loja';
  }
  let res;
  try { res = await enviarCRM(env, lead); }
  catch (e) { console.error('Falha ao enviar', chave, e.message); return 'erro'; }   // não grava: tenta de novo na próxima rodada
  if (res.situacao === 'ACEITO') Object.assign(res, await acompanhar(res.trackingStatusUrl));
  delete res.trackingStatusUrl;
  await env.LEADS.put(chave, JSON.stringify({ ...res, loja: lead.dealerId, em: new Date().toISOString() }), { expirationTtl: GUARDA_DIAS * 86400 });
  console.log('Lead', chave, '→ loja', lead.dealerId, res.situacao, res.codigoRastreio || res.detalhe || '');
  return res.situacao;
}

/* ── META ── */
const tokenMeta = env => env.META_LEADS_TOKEN || env.META_TOKEN;

async function graph(caminho, params, token) {
  const q = new URLSearchParams({ ...params, access_token: token });
  const r = await fetch(`${GRAPH}/${caminho}?${q}`);
  const j = await r.json();
  if (!r.ok) throw new Error(`Meta ${caminho}: ${(j.error && j.error.message) || r.status}`);
  return j;
}

async function tokenDaPagina(env) {
  const j = await graph(PAGE_ID, { fields: 'access_token' }, tokenMeta(env));
  return j.access_token || tokenMeta(env);
}

function leadDaMeta(l) {
  const campo = {};
  for (const f of l.field_data || []) campo[f.name] = (f.values || [])[0] || '';
  const achar = re => { const k = Object.keys(campo).find(n => re.test(semAcento(n))); return k ? campo[k] : ''; };
  const nome = campo.full_name || [campo.first_name, campo.last_name].filter(Boolean).join(' ');
  const cidade = campo.city || '', uf = campo.state || '';
  const loja = acharLoja({ resposta: achar(/loja|concession|revend|unidade/), cidade, uf });
  const modelo = achar(/modelo|moto|interesse/);
  return {
    dealerId: loja ? loja.dealerId : null,
    nome: nome || 'Sem nome',
    email: campo.email || '',
    celular: celularBR(campo.phone_number),
    cidade, uf, cep: digitos(campo.zip_code || campo.post_code),
    origem: ORIGEM[l.platform] || ORIGEM.meta,
    categoriaOrigem: CATEGORIA_ORIGEM,
    tipo: 'WEB',
    marca: 'VOGE',
    produtoInteresse: modelo,
    mensagem: `Lead de formulário da Meta${l.form_name ? ': ' + l.form_name : ''}${l.campaign_name ? ' · campanha ' + l.campaign_name : ''}`,
    idExterno: 'meta-' + l.id,
    metadados: {
      plataforma: l.platform || '', form_id: l.form_id || '', ad_id: l.ad_id || '',
      ad_name: l.ad_name || '', campaign_name: l.campaign_name || '', is_organic: String(!!l.is_organic),
      criado_em: l.created_time || '',
    },
  };
}

async function rodadaMeta(env) {
  const token = await tokenDaPagina(env);
  const forms = (await graph(`${PAGE_ID}/leadgen_forms`, { fields: 'id,name,status', limit: '100' }, token)).data || [];
  const desde = Math.floor(Date.now() / 1000) - JANELA_H * 3600;
  const placar = {};
  for (const f of forms.filter(f => f.status === 'ACTIVE')) {
    let url = null, params = {
      fields: 'id,created_time,field_data,ad_id,ad_name,campaign_name,form_id,platform,is_organic',
      filtering: JSON.stringify([{ field: 'time_created', operator: 'GREATER_THAN', value: desde }]),
      limit: '100',
    };
    let pagina = await graph(`${f.id}/leads`, params, token);
    while (true) {
      for (const l of pagina.data || []) {
        const r = await processar(env, 'meta:' + l.id, leadDaMeta({ ...l, form_name: f.name }));
        placar[r] = (placar[r] || 0) + 1;
      }
      url = pagina.paging && pagina.paging.next;
      if (!url) break;
      pagina = await (await fetch(url)).json();
    }
  }
  return placar;
}

/* ── GOOGLE ── */
function leadDoGoogle(b) {
  const col = {};
  for (const c of b.user_column_data || []) col[c.column_id] = { valor: c.string_value || '', nome: c.column_name || '' };
  const v = id => (col[id] && col[id].valor) || '';
  const extra = re => { const c = Object.values(col).find(c => re.test(semAcento(c.nome))); return c ? c.valor : ''; };
  const nome = v('FULL_NAME') || [v('FIRST_NAME'), v('LAST_NAME')].filter(Boolean).join(' ');
  const cidade = v('CITY'), uf = v('REGION');
  const loja = acharLoja({ resposta: extra(/loja|concession|revend|unidade/), cidade, uf });
  return {
    dealerId: loja ? loja.dealerId : null,
    nome: nome || 'Sem nome',
    email: v('EMAIL'),
    celular: celularBR(v('PHONE_NUMBER')),
    cidade, uf, cep: digitos(v('POSTAL_CODE')),
    origem: ORIGEM.google,
    categoriaOrigem: CATEGORIA_ORIGEM,
    tipo: 'WEB',
    marca: 'VOGE',
    produtoInteresse: extra(/modelo|moto|interesse/),
    mensagem: 'Lead de formulário do Google Ads' + (b.is_test ? ' (TESTE)' : ''),
    idExterno: 'google-' + b.lead_id,
    metadados: {
      form_id: String(b.form_id || ''), campaign_id: String(b.campaign_id || ''),
      adgroup_id: String(b.adgroup_id || ''), gclid: b.gcl_id || '', teste: String(!!b.is_test),
    },
  };
}

/* ── ROTAS ── */
const json = (obj, status = 200) => new Response(JSON.stringify(obj, null, 2), {
  status, headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' },
});

/* Mostra só nomes e situação, nunca dados de lead nem chaves */
async function diagnostico(env) {
  const out = {
    chaves: {
      META_LEADS_TOKEN: !!env.META_LEADS_TOKEN, META_TOKEN: !!env.META_TOKEN,
      VOGE_LOJAS_TOKENS: Object.keys(JSON.parse(env.VOGE_LOJAS_TOKENS || '{}')),
      GOOGLE_LEAD_KEY: !!env.GOOGLE_LEAD_KEY,
    },
  };
  try {
    const eu = await graph('me', { fields: 'name' }, tokenMeta(env));
    out.usuarioDoToken = eu.name;
    const contas = await graph('me/accounts', { fields: 'id', limit: '200' }, tokenMeta(env));
    out.temAcessoAPaginaVoge = (contas.data || []).some(c => c.id === PAGE_ID);
  } catch (e) { out.usuarioDoToken = 'erro: ' + e.message; }
  try {
    const p = await graph('me/permissions', {}, tokenMeta(env));
    out.permissoes = (p.data || []).filter(x => x.status === 'granted').map(x => x.permission);
  } catch (e) { out.permissoes = 'erro: ' + e.message; }
  try {
    const t = await tokenDaPagina(env);
    out.formularios = ((await graph(`${PAGE_ID}/leadgen_forms`, { fields: 'name,status', limit: '100' }, t)).data || [])
      .map(f => `${f.name} (${f.status})`);
  } catch (e) { out.formularios = 'erro: ' + e.message; }
  return out;
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/diagnostico') return json(await diagnostico(env));

    if (url.pathname === '/google-webhook' && request.method === 'POST') {
      const b = await request.json().catch(() => null);
      if (!b || !env.GOOGLE_LEAD_KEY || b.google_key !== env.GOOGLE_LEAD_KEY) return json({ erro: 'chave inválida' }, 403);
      ctx.waitUntil(processar(env, 'google:' + b.lead_id, leadDoGoogle(b)));
      return json({});   // o Google só espera 200
    }
    return new Response('Not found', { status: 404 });
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(rodadaMeta(env).then(p => console.log('Rodada Meta:', JSON.stringify(p)))
      .catch(e => console.error('Rodada Meta falhou:', e.message)));
  },
};
