/**
 * Festival Interlagos 2026 — Webhook da Planilha Linha do Tempo
 * Lime AG
 *
 * Cole esse código em:
 *   Planilha → Extensões → Apps Script
 *
 * Toda vez que alguém editar uma aba SEMANA, ele dispara o workflow
 * `update-vendas-unificado.yml` no GitHub, que baixa o XLSX da planilha
 * e atualiza o ticketmaster-data.json em tempo (quase) real.
 *
 * Setup:
 *   1) Configure as Script Properties:
 *        File → Project properties → Script properties → Add row
 *        Key: GITHUB_TOKEN
 *        Value: <seu PAT fine-grained, scope actions:write no repo
 *               contato-lab/festival-interlagos-2026>
 *   2) Rode a função `setup()` UMA VEZ pra criar o trigger.
 *      O Apps Script vai pedir permissão pra acessar a planilha e
 *      fazer chamadas externas — autorize.
 *   3) Pronto: edição em qualquer aba SEMANA dispara o workflow.
 *
 * Para testar a chamada sem editar a planilha, rode `testarAgora()`.
 * Para remover tudo, rode `removerTriggers()`.
 */

const GITHUB_REPO     = 'contato-lab/festival-interlagos-2026';
const WORKFLOW_FILE   = 'update-vendas-unificado.yml';
const WORKFLOW_REF    = 'main';
const DEBOUNCE_SECONDS = 60;  // máximo 1 dispatch a cada minuto

// ─── Setup / Teardown ───────────────────────────────────────────────────

function setup() {
  // Remove triggers anteriores deste handler pra não duplicar
  const existing = ScriptApp.getProjectTriggers();
  for (const t of existing) {
    if (t.getHandlerFunction() === 'onSheetEdit') ScriptApp.deleteTrigger(t);
  }

  // Cria o trigger novo (instalável — necessário pra usar UrlFetchApp)
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ScriptApp.newTrigger('onSheetEdit')
    .forSpreadsheet(ss)
    .onEdit()
    .create();

  // Sanity check do token
  const token = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  if (!token) {
    Logger.log('⚠️  ATENÇÃO: GITHUB_TOKEN não está configurado em Script Properties.');
    Logger.log('    Configure antes que a primeira edição dispare.');
    return;
  }

  Logger.log('✓ Trigger criado e token presente. Edita qualquer SEMANA pra testar.');
}

function removerTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  for (const t of triggers) {
    ScriptApp.deleteTrigger(t);
  }
  Logger.log('Removidos ' + triggers.length + ' trigger(s).');
}

function testarAgora() {
  Logger.log('Disparando workflow manualmente pra teste…');
  triggerWorkflow();
}

// ─── Handler do trigger ─────────────────────────────────────────────────

function onSheetEdit(e) {
  if (!e || !e.range) return;

  const sheet = e.range.getSheet().getName();
  if (!sheet.toUpperCase().startsWith('SEMANA')) {
    // Só dispara em aba SEMANA. Edições em RESUMO etc. são ignoradas.
    return;
  }

  // Debounce — evita dispatch a cada tecla quando o usuário está
  // preenchendo várias células em sequência.
  const props = PropertiesService.getScriptProperties();
  const lastFire = parseInt(props.getProperty('lastFire') || '0', 10);
  const now = Date.now();
  if (now - lastFire < DEBOUNCE_SECONDS * 1000) {
    const ago = Math.round((now - lastFire) / 1000);
    Logger.log('Skip: debounce ativo (último dispatch há ' + ago + 's). ' +
               'Próxima edição depois de ' + (DEBOUNCE_SECONDS - ago) + 's volta a disparar.');
    return;
  }
  props.setProperty('lastFire', String(now));

  triggerWorkflow();
}

// ─── Chamada GitHub ─────────────────────────────────────────────────────

function triggerWorkflow() {
  const token = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  if (!token) {
    Logger.log('❌ GITHUB_TOKEN não configurado em Script Properties.');
    return;
  }

  const url = 'https://api.github.com/repos/' + GITHUB_REPO +
              '/actions/workflows/' + WORKFLOW_FILE + '/dispatches';

  try {
    const response = UrlFetchApp.fetch(url, {
      method: 'post',
      headers: {
        'Authorization': 'token ' + token,
        'Accept':        'application/vnd.github+json',
        'User-Agent':    'GoogleAppsScript-PlanilhaLinhaDoTempo',
      },
      payload: JSON.stringify({ ref: WORKFLOW_REF }),
      muteHttpExceptions: true,
    });

    const code = response.getResponseCode();
    if (code === 204) {
      Logger.log('✓ Workflow disparado (HTTP 204). JSON deve atualizar em ~30-90s.');
    } else if (code === 401 || code === 403) {
      Logger.log('❌ Token inválido ou sem permissão (HTTP ' + code + '). ' +
                 'Verifique: scope actions:write, repo certo, token não expirado.');
      Logger.log('Resposta: ' + response.getContentText());
    } else {
      Logger.log('❌ GitHub respondeu HTTP ' + code + ': ' + response.getContentText());
    }
  } catch (err) {
    Logger.log('❌ Erro ao chamar GitHub: ' + err);
  }
}
