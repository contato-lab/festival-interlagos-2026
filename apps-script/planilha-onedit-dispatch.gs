/**
 * Festival Interlagos 2026 — Apps Script da Planilha (Lime AG)
 *
 * Toda vez que alguém edita uma aba SEMANA, esse script:
 *   1) Lê os valores TM (Moto + Auto) da planilha direto via SpreadsheetApp
 *   2) Faz MERGE com o ticketmaster-data.json atual do GitHub
 *   3) Commita o novo JSON direto via GitHub API
 *
 * Bypass do workflow do GitHub Actions = total ~5-10s da edição até o
 * dashboard mostrar (era ~60-100s). Workflow do GitHub continua rodando
 * a cada 2min como rede de segurança / re-sincronização.
 *
 * Setup (uma vez):
 *   1) Script Properties → GITHUB_TOKEN (PAT com scope `contents:write` no repo)
 *   2) Rode setup() pra criar o trigger onEdit
 *   3) Autorize as permissões (Spreadsheet + UrlFetch)
 */

// ─── Config ─────────────────────────────────────────────────────────────────
const GITHUB_OWNER  = 'contato-lab';
const GITHUB_REPO   = 'festival-interlagos-2026';
const GITHUB_BRANCH = 'main';
const TM_DATA_PATH  = 'ticketmaster-data.json';

const PLANILHA_SEMANA_0_START = new Date(2026, 2, 30); // 30/mar/2026 (mes 0-indexed)
const DEBOUNCE_SECONDS = 5;  // edicoes em sequencia esperam 5s pra disparar

// Fallback caso direct commit falhe: aciona o workflow_dispatch (caminho lento)
const WORKFLOW_FILE = 'update-vendas-unificado.yml';

// ═══════════════════════════════════════════════════════════════════════════
// SETUP / TEARDOWN
// ═══════════════════════════════════════════════════════════════════════════
function setup() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'onSheetEdit') ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger('onSheetEdit')
    .forSpreadsheet(SpreadsheetApp.getActiveSpreadsheet())
    .onEdit()
    .create();
  const token = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  Logger.log(token ? '✓ Trigger criado e token presente.' : '⚠️  Trigger criado MAS GITHUB_TOKEN não está configurado.');
}

function removerTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(t => ScriptApp.deleteTrigger(t));
  Logger.log('Removidos ' + triggers.length + ' trigger(s).');
}

function testarAgora() {
  Logger.log('Disparando atualização manualmente…');
  updateTicketmasterDataDirect();
}

// ═══════════════════════════════════════════════════════════════════════════
// HANDLER
// ═══════════════════════════════════════════════════════════════════════════
function onSheetEdit(e) {
  if (!e || !e.range) return;
  const sheet = e.range.getSheet().getName();
  if (!sheet.toUpperCase().startsWith('SEMANA')) return;

  // Debounce — varias celulas em sequencia disparam 1x so
  const props = PropertiesService.getScriptProperties();
  const lastFire = parseInt(props.getProperty('lastFire') || '0', 10);
  const now = Date.now();
  if (now - lastFire < DEBOUNCE_SECONDS * 1000) {
    Logger.log('Skip: debounce ativo');
    return;
  }
  props.setProperty('lastFire', String(now));

  try {
    updateTicketmasterDataDirect();
  } catch (err) {
    Logger.log('❌ Direct update falhou, fallback workflow_dispatch: ' + err);
    triggerWorkflowFallback();
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// FAST PATH — commita direto no GitHub (~5-10s end-to-end)
// ═══════════════════════════════════════════════════════════════════════════
function updateTicketmasterDataDirect() {
  const t0 = Date.now();
  Logger.log('1) Lendo planilha…');
  const planilhaTm = parsePlanilhaTM();

  Logger.log('2) Buscando ' + TM_DATA_PATH + ' atual do GitHub…');
  const file = getFileFromGitHub(TM_DATA_PATH);
  const currentData = file
    ? JSON.parse(Utilities.newBlob(Utilities.base64Decode(file.content)).getDataAsString())
    : { daily: [], totals: {} };

  Logger.log('3) Mesclando dados da planilha com baseline…');
  const merged = mergeWithPlanilha(currentData, planilhaTm);

  Logger.log('4) Commitando no GitHub…');
  putFileToGitHub(TM_DATA_PATH, JSON.stringify(merged, null, 2), file ? file.sha : null);

  const dt = Math.round((Date.now() - t0) / 100) / 10;
  Logger.log(`✓ Concluído em ${dt}s — TM Moto ${merged.totals.moto_ingressos} ing, Auto ${merged.totals.auto_ingressos} ing.`);
}

// ─── Parser da planilha ─────────────────────────────────────────────────────
function parsePlanilhaTM() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const out = { moto: {}, auto: {} };

  ss.getSheets().forEach(sheet => {
    const name = sheet.getName();
    const m = name.match(/SEMANA\s+(\d+)/i);
    if (!m) return;
    const weekNum = parseInt(m[1], 10);
    const weekDates = computeWeekDates(weekNum);
    const rows = sheet.getDataRange().getValues();

    let section = null, canal = null;
    for (const row of rows) {
      const label = String(row[0] || '').trim().toUpperCase();
      if (!label) continue;

      if (label.includes('VENDAS MOTOS'))                    { section = 'moto'; canal = null; continue; }
      if (label.includes('VENDAS AUTOS') || label.includes('VENDAS CARROS')) { section = 'auto'; canal = null; continue; }
      if (label.startsWith('SISTEMA PR'))                    { canal = 'proprio'; continue; }
      if (label.includes('TICKET MASTER') || label.includes('TICKETMASTER')) { canal = 'tm'; continue; }
      if (label.startsWith('TOTAL') || label.includes('TKT') || label.includes('BALAN') || label.includes('INVEST')) {
        canal = null; continue;
      }

      if (section && canal === 'tm') {
        if (label.startsWith('VENDAS VALOR')) {
          for (let i = 0; i < 7; i++) {
            const v = parseMoney(row[i + 1]);
            if (v === 0 && !out[section][weekDates[i]]) continue;
            out[section][weekDates[i]] = out[section][weekDates[i]] || { qtd: 0, rec: 0 };
            out[section][weekDates[i]].rec = v;
          }
        } else if (label.includes('QTD INGRESSOS')) {
          for (let i = 0; i < 7; i++) {
            const q = parseQtd(row[i + 1]);
            if (q === 0 && !out[section][weekDates[i]]) continue;
            out[section][weekDates[i]] = out[section][weekDates[i]] || { qtd: 0, rec: 0 };
            out[section][weekDates[i]].qtd = q;
          }
        }
      }
    }
  });

  return out;
}

function computeWeekDates(weekNum) {
  const dates = [];
  const start = new Date(PLANILHA_SEMANA_0_START);
  start.setDate(start.getDate() + weekNum * 7);
  for (let i = 0; i < 7; i++) {
    const d = new Date(start);
    d.setDate(d.getDate() + i);
    dates.push(Utilities.formatDate(d, 'UTC', 'yyyy-MM-dd'));
  }
  return dates;
}

function parseMoney(v) {
  if (v === null || v === undefined || v === '') return 0;
  if (typeof v === 'number') return v;
  const s = String(v).trim().replace(/R\$\s*/g, '').replace(/\./g, '').replace(',', '.');
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

function parseQtd(v) {
  if (v === null || v === undefined || v === '') return 0;
  if (typeof v === 'number') return Math.floor(v);
  const s = String(v).trim().replace(/\./g, '').replace(',', '');
  const n = parseInt(s, 10);
  return isNaN(n) ? 0 : n;
}

// ─── Merge ──────────────────────────────────────────────────────────────────
function mergeWithPlanilha(currentData, planilhaTm) {
  const byDate = {};
  (currentData.daily || []).forEach(d => { byDate[d.date] = d; });

  const allDates = new Set(Object.keys(byDate));
  Object.keys(planilhaTm.moto).forEach(d => allDates.add(d));
  Object.keys(planilhaTm.auto).forEach(d => allDates.add(d));

  const newDaily = [];
  let mr = 0, ar = 0, mi = 0, ai = 0;

  Array.from(allDates).sort().forEach(date => {
    const existing = byDate[date] || { date, moto_receita: 0, auto_receita: 0, moto_ingressos: 0, auto_ingressos: 0 };
    const m = planilhaTm.moto[date];
    const a = planilhaTm.auto[date];
    const entry = {
      date,
      moto_receita:   m ? round2(m.rec) : existing.moto_receita,
      auto_receita:   a ? round2(a.rec) : existing.auto_receita,
      moto_ingressos: m ? m.qtd : existing.moto_ingressos,
      auto_ingressos: a ? a.qtd : existing.auto_ingressos,
    };
    newDaily.push(entry);
    mr += entry.moto_receita; ar += entry.auto_receita;
    mi += entry.moto_ingressos; ai += entry.auto_ingressos;
  });

  return Object.assign({}, currentData, {
    updated_at: new Date().toISOString().replace(/\.\d+Z$/, 'Z'),
    source: 'Planilha Linha do Tempo (Apps Script direto)',
    source_type: 'planilha_direct',
    totals: {
      moto_receita: round2(mr),
      auto_receita: round2(ar),
      moto_ingressos: mi,
      auto_ingressos: ai,
      total_receita: round2(mr + ar),
      total_ingressos: mi + ai,
    },
    daily: newDaily,
  });
}

function round2(n) { return Math.round(n * 100) / 100; }

// ─── GitHub API ────────────────────────────────────────────────────────────
function getFileFromGitHub(path) {
  const token = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  if (!token) throw new Error('GITHUB_TOKEN não configurado');

  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${path}?ref=${GITHUB_BRANCH}`;
  const r = UrlFetchApp.fetch(url, {
    headers: { Authorization: 'token ' + token, Accept: 'application/vnd.github+json', 'User-Agent': 'GAS-FestivalInterlagos' },
    muteHttpExceptions: true,
  });
  const code = r.getResponseCode();
  if (code === 404) return null;
  if (code !== 200) throw new Error('GET ' + path + ' HTTP ' + code + ': ' + r.getContentText().slice(0, 200));
  return JSON.parse(r.getContentText());
}

function putFileToGitHub(path, content, sha) {
  const token = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${path}`;
  const payload = {
    message: 'chore: ' + path + ' via Apps Script (planilha edit) [skip ci]',
    content: Utilities.base64Encode(content, Utilities.Charset.UTF_8),
    branch: GITHUB_BRANCH,
  };
  if (sha) payload.sha = sha;

  const r = UrlFetchApp.fetch(url, {
    method: 'put',
    headers: { Authorization: 'token ' + token, Accept: 'application/vnd.github+json', 'User-Agent': 'GAS-FestivalInterlagos' },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  });
  const code = r.getResponseCode();
  if (code !== 200 && code !== 201) {
    throw new Error('PUT ' + path + ' HTTP ' + code + ': ' + r.getContentText().slice(0, 300));
  }
}

// ─── Fallback: workflow_dispatch (caminho lento que ja existia) ─────────────
function triggerWorkflowFallback() {
  const token = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  if (!token) { Logger.log('Sem token, abortando fallback'); return; }
  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_FILE}/dispatches`;
  const r = UrlFetchApp.fetch(url, {
    method: 'post',
    headers: { Authorization: 'token ' + token, Accept: 'application/vnd.github+json', 'User-Agent': 'GAS-FestivalInterlagos' },
    payload: JSON.stringify({ ref: GITHUB_BRANCH }),
    muteHttpExceptions: true,
  });
  Logger.log('Fallback dispatch HTTP ' + r.getResponseCode());
}
