#!/usr/bin/env python3
"""
Festival Interlagos 2026 — AUDITORIA de gasto do Meta Ads.

POR QUE ESTE ARQUIVO EXISTE, separado do update-meta-ads.py:
o coletor de producao puxa a partir de 2026-03-31 ("abertura das vendas") e
esse recorte esta certo pro painel, que compara campanha com campanha. Mas o
TETO DE VERBA e da conta, nao da campanha: em 28/08/2026 o painel mostrava
R$ 312.699 na conta principal enquanto a tela do Meta mostrava ~R$ 339 mil, e
a diferenca de ~R$ 26 mil so podia ser gasto anterior a 31/03.

Este script responde essa pergunta e NAO ESCREVE em meta-data.json. Ele grava
auditoria-meta.json, um relatorio de leitura. Assim da pra saber o numero real
sem mudar a base historica do painel no meio do evento, que trocaria todos os
comparativos que o cliente ja viu.

Varre desde 2026-01-01 pra pegar qualquer gasto antes da abertura das vendas.
"""

import os, json, sys, urllib.request, urllib.parse
from datetime import date
from collections import defaultdict

ACCT_PRINCIPAL  = os.environ.get('META_ACCT',       'act_2044706169171045')
ACCT_NOVA_MOTOS = os.environ.get('META_ACCT_MOTOS', 'act_1326431289216611')
TOKEN           = os.environ.get('META_TOKEN', '')
SINCE           = os.environ.get('AUDIT_SINCE', '2026-01-01')
UNTIL           = date.today().strftime('%Y-%m-%d')
API_VERSION     = 'v21.0'
CORTE_PAINEL    = '2026-03-31'   # de onde o coletor de producao comeca


def fetch_insights(acct):
    """Pagina ate o fim: com time_increment=1 e varios meses, uma pagina so
    nao cobre o periodo, e parar na primeira subestima o gasto, que e
    exatamente o erro que esta auditoria existe pra achar."""
    params = {
        'fields':         'date_start,spend,impressions,clicks',
        'time_range':     json.dumps({'since': SINCE, 'until': UNTIL}),
        'time_increment': '1',
        'level':          'account',
        'access_token':   TOKEN,
        'limit':          '500',
    }
    url = f'https://graph.facebook.com/{API_VERSION}/{acct}/insights?{urllib.parse.urlencode(params)}'
    linhas, paginas = [], 0
    while url and paginas < 40:
        with urllib.request.urlopen(url, timeout=60) as resp:
            d = json.loads(resp.read())
        linhas.extend(d.get('data', []))
        url = ((d.get('paging') or {}).get('next'))
        paginas += 1
    return linhas, paginas


def auditar(acct, label):
    print(f'== {label} ({acct}) — {SINCE} a {UNTIL}', flush=True)
    try:
        linhas, paginas = fetch_insights(acct)
    except Exception as e:
        print(f'   FALHOU: {e}', file=sys.stderr, flush=True)
        return {'label': label, 'account_id': acct, 'erro': str(e)}

    porMes, porDia = defaultdict(float), []
    total = antes = depois = 0.0
    for r in linhas:
        d = r['date_start']
        s = float(r.get('spend', 0) or 0)
        porMes[d[:7]] += s
        porDia.append({'date': d, 'cost': round(s, 2)})
        total += s
        if d < CORTE_PAINEL: antes += s
        else:                depois += s

    porDia.sort(key=lambda x: x['date'])
    res = {
        'label': label, 'account_id': acct, 'paginas_lidas': paginas,
        'dias': len(porDia),
        'primeiro_dia': porDia[0]['date'] if porDia else None,
        'ultimo_dia':   porDia[-1]['date'] if porDia else None,
        'total':                round(total, 2),
        'antes_de_31_03':       round(antes, 2),
        'de_31_03_em_diante':   round(depois, 2),
        'por_mes': {k: round(v, 2) for k, v in sorted(porMes.items())},
        'por_dia': porDia,
    }
    print(f'   {len(porDia)} dias em {paginas} pagina(s)')
    print(f'   TOTAL geral        R$ {total:,.2f}')
    print(f'   antes de 31/03     R$ {antes:,.2f}   <- nao aparece no painel')
    print(f'   de 31/03 em diante R$ {depois:,.2f}   <- e o que o painel mostra')
    for k, v in sorted(porMes.items()):
        print(f'     {k}  R$ {v:,.2f}')
    return res


def main():
    if not TOKEN:
        print('META_TOKEN nao definido'); sys.exit(1)

    p = auditar(ACCT_PRINCIPAL, 'principal')
    n = auditar(ACCT_NOVA_MOTOS, 'nova conta motos')

    somaTotal  = (p.get('total') or 0) + (n.get('total') or 0)
    somaAntes  = (p.get('antes_de_31_03') or 0) + (n.get('antes_de_31_03') or 0)
    somaDepois = (p.get('de_31_03_em_diante') or 0) + (n.get('de_31_03_em_diante') or 0)

    out = {
        'gerado_em': date.today().isoformat(),
        'janela': {'since': SINCE, 'until': UNTIL},
        'corte_do_painel': CORTE_PAINEL,
        'observacao': ('Relatorio de leitura. NAO altera meta-data.json nem a base do painel. '
                       'antes_de_31_03 e o gasto que o coletor de producao nao enxerga.'),
        'contas': {'principal': p, 'nova_motos': n},
        'somado': {
            'total_meta':               round(somaTotal, 2),
            'antes_de_31_03':           round(somaAntes, 2),
            'de_31_03_em_diante':       round(somaDepois, 2),
        },
    }
    with open('auditoria-meta.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print()
    print('===== META SOMANDO AS DUAS CONTAS =====')
    print(f'  total geral        R$ {somaTotal:,.2f}')
    print(f'  antes de 31/03     R$ {somaAntes:,.2f}')
    print(f'  de 31/03 em diante R$ {somaDepois:,.2f}')
    print('auditoria-meta.json gerado')


if __name__ == '__main__':
    main()
