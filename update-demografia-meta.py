#!/usr/bin/env python3
"""
update-demografia-meta.py
Perfil demografico de quem foi alcancado pelos anuncios do Festival Interlagos
em 2026, nas duas edicoes (conta principal + conta motos).

Gera demografia-meta.json com recortes de idade, genero, idade x genero,
estado, plataforma e aparelho. Agregado no periodo inteiro, sem serie diaria.

O GA4 nao entrega idade e genero porque os Indicadores do Google estao
desligados na propriedade. O Meta declara esse dado na propria plataforma,
entao esta e a fonte de idade e genero.
"""

import os, json, sys, urllib.request, urllib.parse, urllib.error
from datetime import date, datetime, timezone

CONTAS = [
    ('auto',  os.environ.get('META_ACCT',       'act_2044706169171045')),
    ('motos', os.environ.get('META_ACCT_MOTOS', 'act_1326431289216611')),
]
TOKEN       = os.environ.get('META_TOKEN', '')
SINCE       = os.environ.get('DEMO_SINCE', '2026-03-30')
UNTIL       = os.environ.get('DEMO_UNTIL', date.today().strftime('%Y-%m-%d'))
API_VERSION = 'v21.0'
OUTPUT_FILE = 'demografia-meta.json'

# cada recorte: chave de saida -> breakdowns pedidos na API
RECORTES = {
    'idade':            ['age'],
    'genero':           ['gender'],
    'idade_genero':     ['age', 'gender'],
    'estado':           ['region'],
    'plataforma':       ['publisher_platform'],
    'aparelho':         ['impression_device'],
}

CAMPOS_BASE = ['impressions', 'clicks', 'spend', 'actions']


def chamar(acct, breakdowns, com_reach=True):
    campos = list(CAMPOS_BASE)
    if com_reach:
        campos.append('reach')
    params = {
        'fields':       ','.join(campos),
        'breakdowns':   ','.join(breakdowns),
        'time_range':   json.dumps({'since': SINCE, 'until': UNTIL}),
        'level':        'account',
        'access_token': TOKEN,
        'limit':        '500',
    }
    url = f'https://graph.facebook.com/{API_VERSION}/{acct}/insights?{urllib.parse.urlencode(params)}'
    linhas = []
    while url:
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                pagina = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            corpo = e.read().decode('utf-8', 'replace')[:500]
            if com_reach:
                # reach nao e permitido em alguns recortes, tenta sem
                print(f'  reach recusado em {breakdowns}, repetindo sem reach', file=sys.stderr)
                return chamar(acct, breakdowns, com_reach=False)
            print(f'  ERRO {e.code} em {breakdowns}: {corpo}', file=sys.stderr)
            return []
        linhas.extend(pagina.get('data', []))
        url = (pagina.get('paging') or {}).get('next')
    return linhas


def compras(row):
    acoes = row.get('actions') or []
    p = sum(float(a['value']) for a in acoes if a.get('action_type') == 'purchase')
    o = sum(float(a['value']) for a in acoes if a.get('action_type') == 'omni_purchase')
    return max(p, o)


def rotulo(row, breakdowns):
    partes = [str(row.get(b, 'nao informado')) for b in breakdowns]
    return ' / '.join(partes)


def coletar():
    saida = {}
    for nome, breakdowns in RECORTES.items():
        acumulado = {}
        for edicao, acct in CONTAS:
            linhas = chamar(acct, breakdowns)
            print(f'{nome} [{edicao}]: {len(linhas)} linhas', file=sys.stderr)
            for row in linhas:
                k = rotulo(row, breakdowns)
                d = acumulado.setdefault(k, {
                    'chave': k,
                    'impressoes': 0,
                    'alcance': 0,
                    'cliques': 0,
                    'investimento': 0.0,
                    'compras': 0.0,
                })
                d['impressoes']   += int(float(row.get('impressions', 0)))
                d['alcance']      += int(float(row.get('reach', 0) or 0))
                d['cliques']      += int(float(row.get('clicks', 0)))
                d['investimento'] += float(row.get('spend', 0))
                d['compras']      += compras(row)

        itens = sorted(acumulado.values(), key=lambda x: -x['impressoes'])
        tot_imp = sum(i['impressoes'] for i in itens)
        tot_alc = sum(i['alcance'] for i in itens)
        for i in itens:
            i['pct_impressoes'] = round(i['impressoes'] / tot_imp * 100, 2) if tot_imp else 0
            i['pct_alcance']    = round(i['alcance'] / tot_alc * 100, 2) if tot_alc else 0
            i['ctr']            = round(i['cliques'] / i['impressoes'] * 100, 2) if i['impressoes'] else 0
        saida[nome] = {
            'total_impressoes': tot_imp,
            'total_alcance':    tot_alc,
            'itens':            itens,
        }
    return saida


def main():
    if not TOKEN:
        print('META_TOKEN vazio', file=sys.stderr)
        sys.exit(1)
    dados = {
        'atualizado_em': datetime.now(timezone.utc).isoformat(),
        'periodo':       {'inicio': SINCE, 'fim': UNTIL},
        'contas':        [c[1] for c in CONTAS],
        'observacao':    'alcance e por celula do recorte e nao pode ser somado entre recortes diferentes',
        'recortes':      coletar(),
    }
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    print(f'gravado {OUTPUT_FILE}')


if __name__ == '__main__':
    main()
