#!/usr/bin/env python3
"""
Monitor de Campanhas - Festival Interlagos 2026
Roda via GitHub Actions a cada 30 minutos.
Envia alertas via Telegram Bot quando detecta problemas em Meta Ads ou Google Ads.
"""

import json
import os
import sys
import urllib.request
from datetime import datetime, timezone, timedelta

TELEGRAM_TOKEN   = os.environ.get('TELEGRAM_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

BASE = 'https://raw.githubusercontent.com/contato-lab/festival-interlagos-2026/main/'

# ── Thresholds de alerta ───────────────────────────────────────────────────────
META_CONFIGS = {
    'principal': {
        'label':   'Meta Principal (Automoveis)',
        'cpp_max': 300,   # R$ - CPP acima disso = alerta
        'min_purchases_for_cpp': 3,
    },
    'nova_motos': {
        'label':   'Meta Nova Motos',
        'cpp_max': 200,
        'min_purchases_for_cpp': 3,
    },
}

# Hora BRT a partir da qual esperamos gasto (campanhas normalmente ativas a partir das 8h)
HORA_INICIO_OPERACAO = 8


# ── Helpers de data/hora ───────────────────────────────────────────────────────
def _brt_now():
    return datetime.now(timezone.utc) - timedelta(hours=3)

def hoje_str():
    return _brt_now().strftime('%Y-%m-%d')

def ontem_str():
    return (_brt_now() - timedelta(days=1)).strftime('%Y-%m-%d')

def hora_brt():
    return _brt_now().hour


# ── Fetch ──────────────────────────────────────────────────────────────────────
def fetch(url, timeout=30):
    req = urllib.request.Request(url, headers={'User-Agent': 'Monitor-FestivalInterlagos/1.0'})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


# ── Telegram ───────────────────────────────────────────────────────────────────
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print('⚠️  TELEGRAM_TOKEN ou TELEGRAM_CHAT_ID nao configurados')
        return
    url  = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    body = json.dumps({
        'chat_id':    TELEGRAM_CHAT_ID,
        'text':       msg,
        'parse_mode': 'HTML',
    }).encode('utf-8')
    req = urllib.request.Request(url, data=body, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=15):
        pass


# ── Checagens Meta Ads ─────────────────────────────────────────────────────────
def check_meta(meta):
    alertas = []
    hoje   = hoje_str()
    ontem  = ontem_str()
    hora   = hora_brt()

    accounts = meta.get('accounts', {})

    for acc_key, cfg in META_CONFIGS.items():
        acc   = accounts.get(acc_key, {})
        label = cfg['label']
        series = acc.get('daily_series', [])

        by_date = {d['date']: d for d in series}
        hoje_d  = by_date.get(hoje, {})
        ontem_d = by_date.get(ontem, {})

        spend_hoje  = hoje_d.get('cost', 0) or 0
        spend_ontem = ontem_d.get('cost', 0) or 0

        # 1. Gasto zerado depois da hora de operacao
        if hora >= HORA_INICIO_OPERACAO and spend_hoje == 0:
            alertas.append(
                f"🔴 <b>META SEM GASTO - {label}</b>\n"
                f"Gasto hoje: R$ 0 (verificado às {hora}h BRT)\n"
                f"Ontem foi: R$ {spend_ontem:,.0f}"
            )

        # 2. CPP muito alto (só se há compras suficientes para ser estatisticamente relevante)
        purchases_hoje = hoje_d.get('purchases', 0) or 0
        cpp_hoje       = hoje_d.get('cpp') or 0

        if purchases_hoje >= cfg['min_purchases_for_cpp'] and cpp_hoje > cfg['cpp_max']:
            alertas.append(
                f"🟡 <b>META CPP ALTO - {label}</b>\n"
                f"CPP hoje: R$ {cpp_hoje:,.0f} (limite: R$ {cfg['cpp_max']:,})\n"
                f"Compras: {purchases_hoje} | Gasto: R$ {spend_hoje:,.0f}"
            )

        # 3. CPP 3x acima da media dos ultimos 7 dias
        historico = sorted([d for d in series if d.get('date', '') < hoje], key=lambda x: x['date'])
        ult7 = historico[-7:]
        cpp_list = [d.get('cpp') for d in ult7 if d.get('cpp') and d.get('cpp') > 0]
        if cpp_list and cpp_hoje > 0:
            baseline = sum(cpp_list) / len(cpp_list)
            if cpp_hoje > baseline * 3 and purchases_hoje >= cfg['min_purchases_for_cpp']:
                alertas.append(
                    f"🟠 <b>META CPP 3x a media - {label}</b>\n"
                    f"CPP hoje: R$ {cpp_hoje:,.0f} | Media 7d: R$ {baseline:,.0f}\n"
                    f"Compras: {purchases_hoje}"
                )

    return alertas


# ── Checagens Google Ads ───────────────────────────────────────────────────────
def check_google(gads):
    alertas = []
    hoje  = hoje_str()
    ontem = ontem_str()
    hora  = hora_brt()

    campaigns = gads.get('campaigns', [])
    if not campaigns:
        return alertas

    ENABLED = {'ENABLED', 'ACTIVE'}
    PAUSED  = {'PAUSED', 'PAUSADA'}

    ativas  = [c for c in campaigns if c.get('status', '').upper() in ENABLED]
    pausadas = [c for c in campaigns if c.get('status', '').upper() in PAUSED]

    # 1. Todas as campanhas pausadas
    if hora >= HORA_INICIO_OPERACAO and len(ativas) == 0 and campaigns:
        nomes = [c.get('name', '?')[:45] for c in campaigns[:3]]
        alertas.append(
            f"🔴 <b>GOOGLE ADS PARADO!</b>\n"
            f"0 campanhas ativas ({len(pausadas)} pausadas, {len(campaigns)} total)\n"
            f"Ex: {' | '.join(nomes)}"
        )

    # 2. [MOTO PESQUISA] especificamente - melhor CPA do portfolio
    pesquisa = next(
        (c for c in campaigns if 'PESQUISA' in c.get('name', '').upper() and 'MOTO' in c.get('name', '').upper()),
        None
    )
    if pesquisa:
        status_pesquisa = pesquisa.get('status', '').upper()
        if status_pesquisa not in ENABLED:
            alertas.append(
                f"🟡 <b>[MOTO PESQUISA] FORA DO AR</b>\n"
                f"Campanha com melhor CPA (historico ~R$ 11) esta {status_pesquisa}\n"
                f"Reative em: ads.google.com"
            )

    # 3. Gasto zerado hoje vs gasto ontem
    daily = gads.get('daily_series', [])
    if daily and hora >= 12:
        by_date   = {d['date']: d for d in daily}
        hoje_d    = by_date.get(hoje, {})
        ontem_d   = by_date.get(ontem, {})
        gasto_hoje  = hoje_d.get('cost', 0) or 0
        gasto_ontem = ontem_d.get('cost', 0) or 0
        if gasto_hoje == 0 and gasto_ontem > 50:
            alertas.append(
                f"🔴 <b>GOOGLE ADS SEM GASTO HOJE</b>\n"
                f"Hoje: R$ 0 | Ontem: R$ {gasto_ontem:,.0f}\n"
                f"Verifique se o budget foi atingido ou se ha problema de pagamento."
            )

    return alertas


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    hoje  = hoje_str()
    hora  = hora_brt()
    brt   = _brt_now()
    hora_str = brt.strftime('%H:%M')
    alertas  = []
    erros    = []

    print(f'[Monitor] {hoje} {hora_str} BRT — iniciando verificacao...')

    # Meta Ads
    try:
        meta = fetch(BASE + 'meta-data.json')
        result = check_meta(meta)
        alertas += result
        print(f'  Meta Ads: {len(result)} alertas')
    except Exception as e:
        msg = f'⚠️ <b>Erro ao ler Meta Ads</b>\n{type(e).__name__}: {str(e)[:120]}'
        erros.append(msg)
        print(f'  Meta Ads ERRO: {e}')

    # Google Ads
    try:
        gads = fetch(BASE + 'google-ads-data.json')
        result = check_google(gads)
        alertas += result
        print(f'  Google Ads: {len(result)} alertas')
    except Exception as e:
        msg = f'⚠️ <b>Erro ao ler Google Ads</b>\n{type(e).__name__}: {str(e)[:120]}'
        erros.append(msg)
        print(f'  Google Ads ERRO: {e}')

    todos = alertas + erros

    if todos:
        cabecalho = (
            f"🚨 <b>ALERTA - Festival Interlagos 2026</b>\n"
            f"📅 {hoje} | {hora_str} BRT\n"
            f"{'─' * 30}"
        )
        corpo = '\n\n'.join(todos)
        rodape = f"\n\n<i>Verificado automaticamente a cada 30 min</i>"
        send_telegram(cabecalho + '\n\n' + corpo + rodape)
        print(f'[Monitor] {len(todos)} alertas enviados ao Telegram.')
        sys.exit(0)  # sucesso mesmo com alertas (alertas nao sao falhas de workflow)
    else:
        print(f'[Monitor] OK — sem alertas em {hoje} {hora_str} BRT.')


if __name__ == '__main__':
    main()
