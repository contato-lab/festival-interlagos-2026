# Apps Script — Webhook da Planilha Linha do Tempo

## O que é

Quando alguém edita a planilha **LM FESTIVAL INTERLAGOS 2026 LINHA DO TEMPO - VENDAS**, este Apps Script chama o `workflow_dispatch` do GitHub Actions, que executa o `update-vendas-unificado.py`. Resultado: o `ticketmaster-data.json` atualiza em ~30-90s, sem depender do cron de 2 minutos que o GitHub às vezes atrasa em horas.

## Setup (uma vez)

### 1. Criar o GitHub Personal Access Token (PAT)

1. Acesse https://github.com/settings/personal-access-tokens/new
2. Token name: `Planilha Linha Tempo → workflow dispatch`
3. Expiration: 90 days (ou o que preferir; pode renovar depois)
4. Repository access: **Only select repositories** → `contato-lab/festival-interlagos-2026`
5. Repository permissions:
   - **Actions**: Read and write
   - **Metadata**: Read (auto-ativado)
6. Generate token → **copia agora** (não vai aparecer de novo)

### 2. Instalar o script na planilha

1. Abra a planilha no Google Sheets
2. Menu **Extensões → Apps Script**
3. Apaga o `function myFunction()` vazio que aparece
4. Cola o conteúdo de `planilha-onedit-dispatch.gs`
5. **Salvar** (ícone do disquete ou Ctrl+S). Dá um nome ao projeto, ex.: "Webhook Linha do Tempo"

### 3. Salvar o token nas Script Properties

1. No Apps Script, ícone de engrenagem à esquerda (**Project Settings**)
2. Role até **Script Properties** → **Add script property**
3. Key: `GITHUB_TOKEN`
   Value: (cola o PAT que você gerou)
4. Save

### 4. Autorizar e ativar

1. Volta no editor de código
2. No seletor de função no topo, escolhe `setup`
3. Clica **Run**
4. Vai abrir uma janela pedindo autorização. Aceita.
   - Se aparecer "Google hasn't verified this app", clica **Advanced → Go to <nome> (unsafe)** e segue.
5. Depois que rodar com sucesso, abre o **Executions log** (relógio à esquerda) pra confirmar que apareceu `✓ Trigger criado e token presente`

### 5. Testar

- Roda `testarAgora` no editor — deve aparecer `✓ Workflow disparado (HTTP 204)` no log.
- Vai em https://github.com/contato-lab/festival-interlagos-2026/actions e confirma que o workflow rodou.
- Edita qualquer célula em uma aba SEMANA — workflow deve disparar em segundos (com debounce de 60s entre disparos).

## Como saber se está funcionando

- No Apps Script: **Triggers** (ícone do relógio com check) → deve mostrar 1 trigger ativo (`onSheetEdit` em `From spreadsheet → On edit`).
- No GitHub: aba **Actions** → frequência aumenta toda vez que você edita a planilha.
- No dashboard: `ticketmaster-data.json` reflete o que você acabou de digitar em ~1 min.

## Manutenção

- **Token expirou?** Gera outro PAT, atualiza o valor em Script Properties.
- **Não dispara?** Roda `testarAgora` pra ver erros no log. Se token estiver errado, vai aparecer "Token inválido ou sem permissão".
- **Quer desligar?** Roda `removerTriggers` no editor.
- **Muito ruído** (dispara demais)? Aumenta `DEBOUNCE_SECONDS` no topo do script.

## Por que o webhook + cron coexistem

- **Webhook** = atualização rápida quando alguém edita (em segundos)
- **Cron** (a cada 2 min) = rede de segurança, pega quando webhook falha ou quando a API TM volta a funcionar

Os dois sistemas usam o MESMO workflow, então não tem efeito colateral em ter os dois ligados.
