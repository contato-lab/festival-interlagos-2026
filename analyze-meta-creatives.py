#!/usr/bin/env python3
"""
analyze-meta-creatives.py — script one-shot de análise

Gera análise focada dos últimos N dias (default 10):
1. Top 10 criativos de TODAS as campanhas (2 contas Meta) por compras
2. Top criativos da campanha de REMARKETING (conta principal apenas)

Output: analise-meta-criativos.json + log no stdout.
"""

import os
import json
import sys
import urllib.request
import urllib.parse
from datetime import date, timedelta

ACCT_PRINCIPAL  = 'act_2044706169171045'
ACCT_NOVA_MOTOS = 'act_1326431289216611'
TOKEN           = os.environ.get('META_TOKEN', '')
API_VERSION     = 'v21.0'

LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', '10'))
UNTIL = date.today()
SINCE = UNTIL - timedelta(days=LOOKBACK_DAYS)

FIELDS = ','.join([
    'ad_id', 'ad_name', 'adset_name', 'campaign_name',
    'impressions', 'clicks', 'ctr', 'spend', 'actions',
])


def api_get(url, timeout=60):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


def fetch_ads_insights(acct):
    params = {
        'fields':       FIELDS,
        'time_range':   json.dumps({'since': SINCE.isoformat(), 'until': UNTIL.isoformat()}),
        'level':        'ad',
        'access_token': TOKEN,
        'limit':        '500',
    }
    url = f'https://graph.facebook.com/{API_VERSION}/{acct}/insights?{urllib.parse.urlencode(params)}'
    rows = []
    while url:
        data = api_get(url)
        rows.extend(data.get('data', []))
        url = (data.get('paging') or {}).get('next')
    return rows


def parse_purchases(actions):
    if not actions:
        return 0
    p = sum(float(a['value']) for a in actions if a.get('action_type') == 'purchase')
    o = sum(float(a['value']) for a in actions if a.get('action_type') == 'omni_purchase')
    return max(p, o)


def fetch_creative_thumbnail(ad_id):
    try:
        params = {
            'fields':       'creative{thumbnail_url,image_url}',
            'access_token': TOKEN,
        }
        url = f'https://graph.facebook.com/{API_VERSION}/{ad_id}?{urllib.parse.urlencode(params)}'
        data = api_get(url, timeout=15)
        creative = data.get('creative') or {}
        return {
            'thumbnail_url': creative.get('thumbnail_url', ''),
            'image_url':     creative.get('image_url', ''),
        }
    except Exception:
        return {'thumbnail_url': '', 'image_url': ''}


def build_ads(acct, account_label):
    rows = fetch_ads_insights(acct)
    out = []
    for r in rows:
        spend = float(r.get('spend', 0))
        pur   = int(parse_purchases(r.get('actions')))
        ad = {
            'account':       account_label,
            'account_id':    acct,
            'ad_id':         r.get('ad_id'),
            'ad_name':       r.get('ad_name', ''),
            'adset_name':    r.get('adset_name', ''),
            'campaign_name': r.get('campaign_name', ''),
            'impressions':   int(r.get('impressions', 0)),
            'clicks':        int(r.get('clicks', 0)),
            'ctr':           round(float(r.get('ctr', 0) or 0), 2),
            'spend':         round(spend, 2),
            'purchases':     pur,
            'cpp':           round(spend / pur, 2) if pur > 0 else None,
        }
        out.append(ad)
    return out


def main():
    if not TOKEN:
        print('META_TOKEN nao definido', file=sys.stderr)
        sys.exit(1)

    print(f'Janela: {SINCE} a {UNTIL} ({LOOKBACK_DAYS} dias)')
    print()
    print(f'Buscando insights da conta principal {ACCT_PRINCIPAL}...')
    principal = build_ads(ACCT_PRINCIPAL, 'Principal')
    print(f'  -> {len(principal)} ads')
    print(f'Buscando insights da conta nova motos {ACCT_NOVA_MOTOS}...')
    nova_motos = build_ads(ACCT_NOVA_MOTOS, 'Nova Motos')
    print(f'  -> {len(nova_motos)} ads')

    todos = principal + nova_motos

    # Filtra ads que de fato tiveram >=1 compra
    com_compra = [a for a in todos if a['purchases'] > 0]
    print(f'\nAds com pelo menos 1 compra nos ultimos {LOOKBACK_DAYS} dias: {len(com_compra)}')

    # TOP 10 GERAL (todas campanhas, 2 contas)
    top10 = sorted(com_compra, key=lambda x: -x['purchases'])[:10]

    # REMARKETING (conta principal apenas) — campanha usa [RMKT] no nome
    def is_remktg(name):
        u = (name or '').upper()
        return ('RMKT' in u) or ('REMARKETING' in u) or ('REMKT' in u)
    remktg = [a for a in principal if is_remktg(a['campaign_name']) and a['purchases'] > 0]
    remktg_sorted = sorted(remktg, key=lambda x: -x['purchases'])[:10]

    # Busca thumbnails dos top
    ids_to_fetch = set()
    for a in top10:        ids_to_fetch.add(a['ad_id'])
    for a in remktg_sorted: ids_to_fetch.add(a['ad_id'])
    print(f'\nBuscando thumbnails de {len(ids_to_fetch)} criativos...')
    thumbs = {}
    for aid in ids_to_fetch:
        thumbs[aid] = fetch_creative_thumbnail(aid)

    for a in top10:
        a.update(thumbs.get(a['ad_id'], {}))
    for a in remktg_sorted:
        a.update(thumbs.get(a['ad_id'], {}))

    output = {
        'analise_em': UNTIL.isoformat(),
        'periodo': {
            'since': SINCE.isoformat(),
            'until': UNTIL.isoformat(),
            'dias':  LOOKBACK_DAYS,
        },
        'total_ads_analisados': len(todos),
        'total_ads_com_compra': len(com_compra),
        'top10_geral':            top10,
        'top_remarketing_principal': remktg_sorted,
    }

    with open('analise-meta-criativos.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print()
    print('=' * 70)
    print(f'TOP 10 GERAL - Ultimos {LOOKBACK_DAYS} dias (todas campanhas, 2 contas)')
    print('=' * 70)
    print(f'{"Pos":<4} {"Conta":<12} {"Compras":<8} {"Spend":<11} {"CPP":<10} {"CTR":<6} Ad / Campanha')
    print('-' * 130)
    for i, a in enumerate(top10, 1):
        spend = f'R$ {a["spend"]:>7,.2f}'
        cpp = f'R$ {a["cpp"]:>6,.2f}' if a['cpp'] is not None else '   ---'
        ctr = f'{a["ctr"]:.2f}%'
        print(f'{i:<4} {a["account"]:<12} {a["purchases"]:<8} {spend:<11} {cpp:<10} {ctr:<6} '
              f'{a["ad_name"][:50]} | {a["campaign_name"][:45]}')

    print()
    print('=' * 70)
    print(f'TOP REMARKETING - Conta Principal - Ultimos {LOOKBACK_DAYS} dias')
    print('=' * 70)
    if not remktg_sorted:
        print('Nenhum ad de campanha [REMARKETING] com compra no periodo.')
    else:
        for i, a in enumerate(remktg_sorted, 1):
            spend = f'R$ {a["spend"]:>7,.2f}'
            cpp = f'R$ {a["cpp"]:>6,.2f}' if a['cpp'] is not None else '   ---'
            ctr = f'{a["ctr"]:.2f}%'
            print(f'{i:<4} {a["purchases"]:<8} {spend:<11} {cpp:<10} {ctr:<6} '
                  f'{a["ad_name"][:50]} | {a["campaign_name"][:45]}')


if __name__ == '__main__':
    main()
