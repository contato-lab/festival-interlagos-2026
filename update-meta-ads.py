#!/usr/bin/env python3
"""
Festival Interlagos 2026 — Meta Ads data fetcher
Gera meta-data.json com daily_series de impressões, cliques,
pageviews, add to cart, checkout, compras e custo por compra.
"""

import os, json, urllib.request, urllib.parse
from datetime import date, timedelta

ACCT        = os.environ.get('META_ACCT', 'act_2044706169171045')
TOKEN       = os.environ.get('META_TOKEN', '')
SINCE       = '2026-03-31'  # abertura das vendas
UNTIL       = date.today().strftime('%Y-%m-%d')
API_VERSION = 'v21.0'

FIELDS = ','.join([
    'date_start',
    'impressions',
    'clicks',
    'spend',
    'actions',
    'action_values',
])

ACTION_TYPES = {
    'landing_page_view': 'pageviews',
    'add_to_cart':       'add_to_cart',
    'initiate_checkout': 'checkout',
}


def fetch_insights():
    params = {
        'fields':         FIELDS,
        'time_range':     json.dumps({'since': SINCE, 'until': UNTIL}),
        'time_increment': '1',
        'level':          'account',
        'filtering':      json.dumps([{'field': 'campaign.name', 'operator': 'CONTAIN', 'value': 'FESTIVAL INTERLAGOS 2026'}]),
        'access_token':   TOKEN,
        'limit':          '500',
    }
    url = f'https://graph.facebook.com/{API_VERSION}/{ACCT}/insights?{urllib.parse.urlencode(params)}'
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read())


def parse_actions(row):
    result = {v: 0.0 for v in ACTION_TYPES.values()}
    for action in (row.get('actions') or []):
        key = ACTION_TYPES.get(action.get('action_type'))
        if key:
            result[key] += float(action.get('value', 0))
    # purchases: max(purchase, omni_purchase) para evitar contagem dupla
    actions = row.get('actions') or []
    p_val = sum(float(a['value']) for a in actions if a.get('action_type') == 'purchase')
    o_val = sum(float(a['value']) for a in actions if a.get('action_type') == 'omni_purchase')
    result['purchases'] = max(p_val, o_val)
    return result


def build_daily_series(data):
    series = []
    for row in data.get('data', []):
        actions = parse_actions(row)
        spend   = float(row.get('spend', 0))
        comp    = actions.get('purchases', 0)
        series.append({
            'date':       row['date_start'],
            'impressions': int(row.get('impressions', 0)),
            'clicks':      int(row.get('clicks', 0)),
            'cost':        round(spend, 2),
            'pageviews':   int(actions.get('pageviews', 0)),
            'add_to_cart': int(actions.get('add_to_cart', 0)),
            'checkout':    int(actions.get('checkout', 0)),
            'purchases':   int(comp),
            'cpp':         round(spend / comp, 2) if comp > 0 else None,
        })
    return sorted(series, key=lambda x: x['date'])


def build_totals(series):
    t = dict(impressions=0, clicks=0, cost=0.0,
             pageviews=0, add_to_cart=0, checkout=0, purchases=0)
    for d in series:
        for k in t:
            t[k] += d.get(k, 0) or 0
    t['cost'] = round(t['cost'], 2)
    t['cpp']  = round(t['cost'] / t['purchases'], 2) if t['purchases'] > 0 else None
    return t


def main():
    if not TOKEN:
        print('❌ META_TOKEN não definido')
        return

    print(f'📊 Buscando insights Meta Ads — {ACCT} — {SINCE} a {UNTIL}')
    raw    = fetch_insights()
    series = build_daily_series(raw)
    totals = build_totals(series)

    output = {
        'updated_at':   date.today().isoformat(),
        'account_id':   ACCT,
        'period':       {'start': SINCE, 'end': UNTIL},
        'totals':       totals,
        'daily_series': series,
    }

    with open('meta-data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f'✅ meta-data.json gerado — {len(series)} dias, '
          f'R$ {totals["cost"]:,.2f} investidos')


if __name__ == '__main__':
    main()
