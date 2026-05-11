#!/usr/bin/env python3
"""
Festival Interlagos 2026 — Meta Ads data fetcher
Gera meta-data.json com daily_series de impressões, cliques,
pageviews, add to cart, checkout, compras e custo por compra.

Conta principal (META_ACCT)  -> alimenta os campos top-level (compat).
Conta motos  (META_ACCT_MOTOS) -> alimenta accounts.nova_motos como breakdown.
"""

import os, json, sys, urllib.request, urllib.parse
from datetime import date

ACCT_PRINCIPAL  = os.environ.get('META_ACCT',       'act_2044706169171045')
ACCT_NOVA_MOTOS = os.environ.get('META_ACCT_MOTOS', 'act_1326431289216611')
TOKEN           = os.environ.get('META_TOKEN', '')
SINCE           = '2026-03-31'  # abertura das vendas
UNTIL           = date.today().strftime('%Y-%m-%d')
API_VERSION     = 'v21.0'

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


def fetch_insights(acct):
    params = {
        'fields':         FIELDS,
        'time_range':     json.dumps({'since': SINCE, 'until': UNTIL}),
        'time_increment': '1',
        'level':          'account',
        'access_token':   TOKEN,
        'limit':          '500',
    }
    url = f'https://graph.facebook.com/{API_VERSION}/{acct}/insights?{urllib.parse.urlencode(params)}'
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
    for row in (data or {}).get('data', []):
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


def fetch_account_safe(acct, label):
    """Busca insights de uma conta. Se falhar (sem permissão / token ruim),
    devolve serie vazia e segue. Evita derrubar o pipeline da conta principal
    por causa de um problema na conta secundaria."""
    try:
        print(f'📊 Buscando insights {label} ({acct}) — {SINCE} a {UNTIL}', flush=True)
        raw = fetch_insights(acct)
        series = build_daily_series(raw)
        totals = build_totals(series)
        print(f'   ok: {len(series)} dias, R$ {totals["cost"]:,.2f}, '
              f'{totals["purchases"]} compras', flush=True)
        return series, totals
    except Exception as e:
        print(f'   ⚠️  falha em {label} ({acct}): {e}', file=sys.stderr, flush=True)
        return [], build_totals([])


def main():
    if not TOKEN:
        print('❌ META_TOKEN não definido')
        return

    principal_series, principal_totals = fetch_account_safe(ACCT_PRINCIPAL, 'principal')
    motos_series,     motos_totals     = fetch_account_safe(ACCT_NOVA_MOTOS, 'nova conta motos')

    # Top-level mantém SOMENTE a conta principal (backward compat para os
    # gráficos e KPIs que já existem). A conta nova entra como breakdown
    # adicional em `accounts.nova_motos` e é renderizada à parte no painel.
    output = {
        'updated_at':   date.today().isoformat(),
        'account_id':   ACCT_PRINCIPAL,
        'period':       {'start': SINCE, 'end': UNTIL},
        'totals':       principal_totals,
        'daily_series': principal_series,
        'accounts': {
            'principal': {
                'account_id':   ACCT_PRINCIPAL,
                'label':        'Meta Ads',
                'totals':       principal_totals,
                'daily_series': principal_series,
            },
            'nova_motos': {
                'account_id':   ACCT_NOVA_MOTOS,
                'label':        'Meta ads - Nova conta Motos',
                'totals':       motos_totals,
                'daily_series': motos_series,
            },
        },
    }

    with open('meta-data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f'✅ meta-data.json gerado')
    print(f'   principal:   {len(principal_series)} dias, R$ {principal_totals["cost"]:,.2f}, '
          f'{principal_totals["purchases"]} compras')
    print(f'   nova motos:  {len(motos_series)} dias, R$ {motos_totals["cost"]:,.2f}, '
          f'{motos_totals["purchases"]} compras')


if __name__ == '__main__':
    main()
