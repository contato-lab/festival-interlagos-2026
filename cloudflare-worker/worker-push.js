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
// DOIS PROJETOS FIREBASE, e isso importa aqui.
// Em 19/08/2026 a Central de conteudo saiu de dentro do central-evento-fi e
// ganhou projeto proprio (conteudo-fi), porque a cota gratis de leitura e de 50
// mil por DIA e por PROJETO, e os dois juntos estouravam o mesmo balde. As
// pecas e o checklist agora nascem no conteudo-fi. Avisos e mural continuam no
// central-evento-fi, que e onde a Central das marcas mora.
//
// Se este arquivo apontar pro projeto errado, o sintoma NAO e erro: o Worker le
// a copia velha, nunca ve novidade, ninguem recebe notificacao e nada aparece
// quebrado em lugar nenhum. Por isso o projeto vem por canal, e nao global.
const PROJETOS = {
  conteudo: { id: "conteudo-fi",       key: "AIzaSyBUWmUynV6U0mqHZoNDm-2KPb3wh4Z9NuY" },
  central:  { id: "central-evento-fi", key: "AIzaSyD1spoy847dEtccSGJflKtG4-EYZMe23OQ" },
};
const base = (p) => `https://firestore.googleapis.com/v1/projects/${PROJETOS[p].id}/databases/(default)/documents`;
const apiKey = (p) => PROJETOS[p].key;
const VAPID_PUB = "BGpVLKMV09mK8-elVjc7xgDKZVvwZAgZc1t_fVDGtNIHks_dE6On1zvRELrAlXaykrJnLD4qJrsH26IUjr8q73Q";
const CONTATO = "mailto:contato@agenciaunderclick.net";

// DOIS PUBLICOS, DUAS LISTAS, DOIS MARCOS.
// O time de conteudo e a imprensa credenciada usam telas diferentes e nao podem
// receber o aviso um do outro: peca nova nao interessa a jornalista nenhum, e
// aviso de credenciamento nao interessa a quem esta editando video.
//
// "colecoes" = um documento por item (as pecas e o checklist).
// "docs"     = um documento so guardando uma lista em items (avisos e mural).
// O mural interno e a fila do locutor ficam DE FORA de proposito: interno e so
// da producao, e a fila muda o tempo todo, viraria celular apitando sem parar.
const CANAIS = [
  {
    nome: "conteudo",
    projeto: "conteudo",
    colecoes: ["cont_pecas", "cont_story"],
    docs: [],
    inscritos: "cont_push",
    estado: "board/push_state",
  },
  {
    nome: "credenciados",
    projeto: "central",
    colecoes: [],
    docs: ["board/avisos", "board/mural"],
    inscritos: "cred_push",
    estado: "board/push_state_cred",
  },
];

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

// PERGUNTA SO O ULTIMO, NAO A COLECAO INTEIRA.
//
// Aqui morava o bug que estourou a cota. A versao antiga listava a colecao toda
// (?pageSize=300) uma vez por minuto so pra achar o carimbo mais alto. No
// Firestore, listagem custa UMA LEITURA POR DOCUMENTO. Com cont_story em 121
// documentos e cont_pecas em 23, davam 144 leituras por minuto, 207 mil por
// dia, contra um teto de 50 mil. A cota morria umas 5 horas depois de zerar, e
// como toda gravacao da Central passa por transacao (que le antes de escrever),
// o efeito visivel era a producao clicando pra apagar um aviso e nada acontecer.
// Ninguem ligou uma coisa na outra porque o robo nao aparece em lugar nenhum.
//
// Ordenar por criadoEm e pegar 1 custa 1 leitura, e nao cresce quando a colecao
// cresce. De 144 por minuto para 2. Quem nao tem criadoEm fica de fora da
// ordenacao, e tudo bem: sao documentos antigos, nunca seriam "novidade".
async function maiorCarimbo(projeto, colecao) {
  const r = await fetch(`${base(projeto)}:runQuery?key=${apiKey(projeto)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      structuredQuery: {
        from: [{ collectionId: colecao }],
        orderBy: [{ field: { fieldPath: "criadoEm" }, direction: "DESCENDING" }],
        limit: 1,
      },
    }),
  });
  if (!r.ok) throw new Error(`consulta em ${colecao} falhou: HTTP ${r.status}`);
  const d = await r.json();
  const achado = (Array.isArray(d) ? d : []).find((x) => x && x.document);
  return achado ? carimbo(achado.document.fields || {}) : 0;
}

// Avisos e mural guardam TUDO num documento so, numa lista em "items". Aqui o
// carimbo que interessa e o de cada item, nao o do documento.
async function maiorCarimboDoc(projeto, caminho) {
  const r = await fetch(`${base(projeto)}/${caminho}?key=${apiKey(projeto)}`);
  if (!r.ok) return 0;
  const d = await r.json();
  const lista = ((d.fields || {}).items || {}).arrayValue?.values || [];
  let t = 0;
  for (const it of lista) {
    const f = (it.mapValue || {}).fields || {};
    const ts = val(f.ts) ?? val(f.criadoEm) ?? val(f.em);
    if (typeof ts === "number" && ts > t) t = ts;
  }
  return t;
}

async function lerEstado(projeto, caminho) {
  const r = await fetch(`${base(projeto)}/${caminho}?key=${apiKey(projeto)}`);
  if (!r.ok) return 0;
  const d = await r.json();
  const v = val((d.fields || {}).ate);
  return typeof v === "number" ? v : 0;
}

async function gravarEstado(projeto, caminho, ate) {
  await fetch(`${base(projeto)}/${caminho}?key=${apiKey(projeto)}&updateMask.fieldPaths=ate&updateMask.fieldPaths=em`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: { ate: { integerValue: String(ate) }, em: { integerValue: String(Date.now()) } } }),
  });
}

async function lerInscricoes(projeto, colecao) {
  const r = await fetch(`${base(projeto)}/${colecao}?key=${apiKey(projeto)}&pageSize=300`);
  if (!r.ok) return [];
  const d = await r.json();
  return (d.documents || [])
    .map((doc) => ({ id: doc.name.split("/").pop(), endpoint: val((doc.fields || {}).endpoint) }))
    .filter((s) => !!s.endpoint);
}

async function apagarInscricao(projeto, colecao, id) {
  await fetch(`${base(projeto)}/${colecao}/${id}?key=${apiKey(projeto)}`, { method: "DELETE" });
}

async function chavePrivada(env) {
  // Mensagem clara em vez de "Cannot read properties of undefined". Sem isto o
  // sintoma de "faltou cadastrar o secret" e um erro de JavaScript que nao diz
  // nada, e a causa comum e chata: cadastrar o secret no GitHub NAO dispara
  // deploy, entao o Worker segue no ar sem a chave ate alguem publicar de novo.
  if (!env.VAPID_PRIVATE_PKCS8) {
    throw new Error(
      "VAPID_PRIVATE_PKCS8 nao esta cadastrado neste Worker. " +
      "Rode o workflow Deploy Push Worker de novo (o secret sozinho nao dispara deploy)."
    );
  }
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

async function avisarCanal(env, canal) {
  const [estado, ...topos] = await Promise.all([
    lerEstado(canal.projeto, canal.estado),
    ...canal.colecoes.map((c) => maiorCarimbo(canal.projeto, c)),
    ...canal.docs.map((c) => maiorCarimboDoc(canal.projeto, c)),
  ]);
  const topo = topos.length ? Math.max(...topos) : 0;

  // Primeira execucao: so grava o marco. Sem isto o primeiro disparo avisaria
  // sobre o historico inteiro, e todo mundo receberia aviso de ontem no
  // primeiro minuto.
  if (!estado) {
    await gravarEstado(canal.projeto, canal.estado, topo || Date.now());
    return `${canal.nome}: primeira vez, marco gravado`;
  }
  if (topo <= estado) return `${canal.nome}: nada novo`;

  const inscritos = await lerInscricoes(canal.projeto, canal.inscritos);
  if (!inscritos.length) {
    await gravarEstado(canal.projeto, canal.estado, topo);
    return `${canal.nome}: novidade, mas ninguem inscrito`;
  }

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
      else if (r.status === 404 || r.status === 410) {
        await apagarInscricao(canal.projeto, canal.inscritos, s.id); mortas++;
      }
    } catch (e) { /* uma inscricao ruim nao pode derrubar as outras */ }
  }

  await gravarEstado(canal.projeto, canal.estado, topo);
  return `${canal.nome}: enviados ${ok}, removidos ${mortas}, de ${inscritos.length}`;
}

async function avisar(env) {
  // Um canal quebrado nao pode calar o outro: se o Firestore recusar a leitura
  // de avisos, o time de conteudo continua recebendo peca nova normalmente.
  const r = await Promise.allSettled(CANAIS.map((c) => avisarCanal(env, c)));
  return r
    .map((x, i) => (x.status === "fulfilled" ? x.value : `${CANAIS[i].nome}: erro ${x.reason?.message || x.reason}`))
    .join(" | ");
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
      const linhas = await Promise.all(CANAIS.map(async (c) => {
        const [ate, insc] = await Promise.all([lerEstado(c.projeto, c.estado), lerInscricoes(c.projeto, c.inscritos)]);
        return [c.nome, { ate, inscritos: insc.length }];
      }));
      return Response.json(Object.fromEntries(linhas));
    }
    return new Response("push das centrais do evento", { status: 200 });
  },
};
