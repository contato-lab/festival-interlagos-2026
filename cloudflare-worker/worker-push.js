/* Remetente das notificacoes da Central de Conteudo.
 *
 * POR QUE UM WORKER E NAO O GITHUB ACTIONS: ja existe um alerta rodando por
 * Actions neste repo, e o agendador do GitHub atrasa muito. Medido durante este
 * evento: cron pedido de 10 em 10 minutos chegando a demorar de 60 a 95. Aviso
 * de peca nova que chega uma hora depois nao serve pra nada num festival de
 * quatro dias. A Cloudflare respeita o minuto.
 *
 * POR QUE O PUSH VAI SEM CONTEUDO: mandar texto dentro do push exige cifrar o
 * corpo (aes128gcm com ECDH e HKDF), que e a parte mais chata e mais facil de
 * errar do Web Push. Aqui o push e so um toque de campainha: quem monta o texto
 * e o sw.js da Central, lendo o Firestore na hora que acorda. De quebra a
 * notificacao mostra o dado mais recente de verdade, e nao um texto congelado
 * no momento do envio.
 *
 * O estado ("ate onde ja avisei") mora no proprio Firestore, em board/push_state.
 * Nada de KV: uma coisa a menos pra configurar e pra alguem esquecer que existe.
 *
 * Secret exigido (cadastrado na Cloudflare, NUNCA neste arquivo, que esta em
 * repositorio publico):
 *   VAPID_PRIVATE_PKCS8 = chave privada P-256 em PKCS8, base64
 */
const PROJETO = "central-evento-fi";
const API_KEY = "AIzaSyD1spoy847dEtccSGJflKtG4-EYZMe23OQ";
const BASE = `https://firestore.googleapis.com/v1/projects/${PROJETO}/databases/(default)/documents`;
const VAPID_PUB = "BGpVLKMV09mK8-elVjc7xgDKZVvwZAgZc1t_fVDGtNIHks_dE6On1zvRELrAlXaykrJnLD4qJrsH26IUjr8q73Q";
const CONTATO = "mailto:contato@agenciaunderclick.net";
const VIGIADAS = ["cont_pecas", "cont_story"];

const b64url = (buf) =>
  btoa(String.fromCharCode(...new Uint8Array(buf)))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

function deB64(s) {
  const bin = atob(s.replace(/-/g, "+").replace(/_/g, "/"));
  const a = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) a[i] = bin.charCodeAt(i);
  return a;
}

function val(v) {
  if (!v || typeof v !== "object") return v;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return parseInt(v.integerValue, 10);
  if ("booleanValue" in v) return v.booleanValue;
  return null;
}

// novidade = o que NASCEU. Mesma regra da faixa dentro da Central: peca leva
// dezenas de toques por dia (status, link, editor) e cada toque reescreve "em".
// Se "em" contasse, o celular do time apitaria a tarde inteira.
function carimbo(f) {
  const c = val(f.criadoEm);
  if (typeof c === "number" && c > 0) return c;
  const e = val(f.em);
  return typeof e === "number" ? e : 0;
}

async function maiorCarimbo(colecao) {
  const r = await fetch(`${BASE}/${colecao}?key=${API_KEY}&pageSize=300`);
  if (!r.ok) return 0;
  const d = await r.json();
  let t = 0;
  for (const doc of d.documents || []) t = Math.max(t, carimbo(doc.fields || {}));
  return t;
}

async function lerEstado() {
  const r = await fetch(`${BASE}/board/push_state?key=${API_KEY}`);
  if (!r.ok) return 0;
  const d = await r.json();
  const v = val((d.fields || {}).ate);
  return typeof v === "number" ? v : 0;
}

async function gravarEstado(ate) {
  await fetch(`${BASE}/board/push_state?key=${API_KEY}&updateMask.fieldPaths=ate&updateMask.fieldPaths=em`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: { ate: { integerValue: String(ate) }, em: { integerValue: String(Date.now()) } } }),
  });
}

async function lerInscricoes() {
  const r = await fetch(`${BASE}/cont_push?key=${API_KEY}&pageSize=300`);
  if (!r.ok) return [];
  const d = await r.json();
  return (d.documents || [])
    .map((doc) => ({ id: doc.name.split("/").pop(), endpoint: val((doc.fields || {}).endpoint) }))
    .filter((s) => !!s.endpoint);
}

async function apagarInscricao(id) {
  await fetch(`${BASE}/cont_push/${id}?key=${API_KEY}`, { method: "DELETE" });
}

async function chavePrivada(env) {
  return crypto.subtle.importKey(
    "pkcs8", deB64(env.VAPID_PRIVATE_PKCS8),
    { name: "ECDSA", namedCurve: "P-256" }, false, ["sign"]
  );
}

// JWT do VAPID: e ele que prova ao servico de push (Apple, Google, Mozilla) que
// quem esta mandando e o mesmo dono da chave publica que o celular guardou.
async function autorizacao(endpoint, chave) {
  const aud = new URL(endpoint).origin;
  const cab = b64url(new TextEncoder().encode(JSON.stringify({ typ: "JWT", alg: "ES256" })));
  const cor = b64url(new TextEncoder().encode(JSON.stringify({
    aud, sub: CONTATO,
    exp: Math.floor(Date.now() / 1000) + 12 * 3600,   // teto do padrao e 24h
  })));
  const assinatura = await crypto.subtle.sign(
    { name: "ECDSA", hash: "SHA-256" }, chave, new TextEncoder().encode(`${cab}.${cor}`)
  );
  return `vapid t=${cab}.${cor}.${b64url(assinatura)}, k=${VAPID_PUB}`;
}

async function avisar(env) {
  const [estado, ...topos] = await Promise.all([lerEstado(), ...VIGIADAS.map(maiorCarimbo)]);
  const topo = Math.max(...topos);

  // Primeira execucao: so grava o marco. Sem isto o primeiro disparo avisaria
  // sobre o historico inteiro, e todo mundo receberia notificacao de peca de
  // ontem no primeiro minuto.
  if (!estado) { await gravarEstado(topo || Date.now()); return "primeira vez: marco gravado"; }
  if (topo <= estado) return "nada novo";

  const inscritos = await lerInscricoes();
  if (!inscritos.length) { await gravarEstado(topo); return "novidade, mas ninguem inscrito"; }

  const chave = await chavePrivada(env);
  let ok = 0, mortas = 0;

  for (const s of inscritos) {
    try {
      const r = await fetch(s.endpoint, {
        method: "POST",
        headers: {
          Authorization: await autorizacao(s.endpoint, chave),
          TTL: "3600",                 // se o celular estiver desligado, guarda 1h
          "Content-Length": "0",
        },
      });
      if (r.ok) ok++;
      // 404/410 = a pessoa desinstalou ou o navegador trocou a inscricao.
      // Sem essa limpeza a lista so cresce e o Worker fica batendo em porta morta.
      else if (r.status === 404 || r.status === 410) { await apagarInscricao(s.id); mortas++; }
    } catch (e) { /* uma inscricao ruim nao pode derrubar as outras */ }
  }

  await gravarEstado(topo);
  return `enviados ${ok}, removidos ${mortas}, de ${inscritos.length}`;
}

export default {
  async scheduled(evt, env, ctx) { ctx.waitUntil(avisar(env)); },

  // Rota manual pra testar sem esperar o minuto virar.
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === "/testar") {
      try { return new Response(await avisar(env), { status: 200 }); }
      catch (e) { return new Response("erro: " + e.message, { status: 500 }); }
    }
    if (u.pathname === "/estado") {
      const [ate, insc] = await Promise.all([lerEstado(), lerInscricoes()]);
      return Response.json({ ate, inscritos: insc.length });
    }
    return new Response("push da central de conteudo", { status: 200 });
  },
};
