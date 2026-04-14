#!/usr/bin/env python3
"""
Festival Interlagos 2026 - Meta Ads CRIATIVOS data fetcher
Puxa performance a nivel de anuncio (criativo) com:
- CTR, cliques, impressoes
- Compras e custo por compra
- Nome do criativo + thumbnail
"""

import os, json, urllib.request, urllib.parse
from datetime import date

ACCT        = os.environ.get('META_ACCT', 'act_2044706169171045')
TOKEN       = os.environ.get('META_TOKEN', '')
SINCE       = '2026-03-31'
UNTIL       = date.today().strftime('%Y-%m-%d')
API_VERSION = 'v21.0'

INSIGHT_FIELDS = ','.join([
    'ad_id',
    'ad_name',
    'adset_name',
    'campaign_name',
    'impressions',
    'clicks',
    'ctr',
    'spend',
    'actions',
])


def api_get(path, params):
    url = f'https://graph.facebook.com/{API_VERSION}/{path}?{urllib.parse.urlencode(params)}'
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_all_ads_insights():
    """Puxa insights a nivel de ad (um row por criativo, somado no periodo)."""
    params = {
        'fields':       INSIGHT_FIELDS,
        'time_range':   json.dumps({'since': SINCE, 'until': UNTIL}),
        'level':        'ad',
        'access_token': TOKEN,
        'limit':        '500',
    }
    data = api_get(f'{ACCT}/insights', params)
    rows = data.get('data', [])
    # paginacao
    while data.get('paging', {}).get('next'):
        next_url = data['paging']['next']
        with urllib.request.urlopen(next_url, timeout=30) as resp:
            data = json.loads(resp.read())
        rows += data.get('data', [])
    return rows


def fetch_ad_creative(ad_id):
    """Pega thumbnail e preview do criativo."""
    try:
        params = {
            'fields':       'creative{thumbnail_url,image_url,effective_object_story_id,object_story_spec}',
            'access_token': TOKEN,
        }
        data = api_get(ad_id, params)
        creative = data.get('creative', {})
        return {
            'thumbnail_url': creative.get('thumbnail_url', ''),
            'image_url':     creative.get('image_url', ''),
        }
    except Exception as e:
        print(f'   aviso: thumbnail {ad_id}: {e}')
        return {'thumbnail_url': '', 'image_url': ''}


def parse_purchases(actions):
    if not actions:
        return 0
    p = sum(float(a['value']) for a in actions if a.get('action_type') == 'purchase')
    o = sum(float(a['value']) for a in actions if a.get('action_type') == 'omni_purchase')
    return max(p, o)


def build_creatives(rows):
    creatives = []
    for row in rows:
        spend  = float(row.get('spend', 0) or 0)
        imp    = int(row.get('impressions', 0) or 0)
        clk    = int(row.get('clicks', 0) or 0)
        ctr    = float(row.get('ctr', 0) or 0)
        purch  = parse_purchases(row.get('actions', []))
        cpp    = round(spend / purch, 2) if purch > 0 else None

        creatives.append({
            'ad_id':         row.get('ad_id', ''),
            'ad_name':       row.get('ad_name', 'Sem nome'),
            'adset_name':    row.get('adset_name', ''),
            'campaign_name': row.get('campaign_name', ''),
            'impressions':   imp,
            'clicks':        clk,
            'ctr':           round(ctr, 2),
            'spend':         round(spend, 2),
            'purchases':     int(purch),
            'cpp':           cpp,
            'thumbnail_url': '',
            'image_url':     '',
        })
    # ordena por gasto desc
    creatives.sort(key=lambda x: x['spend'], reverse=True)
    return creatives


def enrich_top_creatives(creatives, top=20):
    """Busca thumbnails so dos top N (evita rate limit)."""
    for c in creatives[:top]:
        if not c['ad_id']:
            continue
        media = fetch_ad_creative(c['ad_id'])
        c['thumbnail_url'] = media['thumbnail_url']
        c['image_url']     = media['image_url']


def main():
    if not TOKEN:
        print('META_TOKEN nao definido')
        return

    print(f'Buscando criativos Meta Ads - {ACCT} - {SINCE} a {UNTIL}')
    rows = fetch_all_ads_insights()
    print(f'   {len(rows)} linhas de insights (ads)')

    creatives = build_creatives(rows)
    print(f'   {len(creatives)} criativos com dados')

    print('Buscando thumbnails dos top 20...')
    enrich_top_creatives(creatives, top=20)

    totals = {
        'spend':       round(sum(c['spend'] for c in creatives), 2),
        'impressions': sum(c['impressions'] for c in creatives),
        'clicks':      sum(c['clicks'] for c in creatives),
        'purchases':   sum(c['purchases'] for c in creatives),
        'active_ads':  len([c for c in creatives if c['spend'] > 0]),
    }
    totals['ctr'] = round(totals['clicks'] / totals['impressions'] * 100, 2) if totals['impressions'] > 0 else 0
    totals['cpp'] = round(totals['spend'] / totals['purchases'], 2) if totals['purchases'] > 0 else None

    output = {
        'updated_at': date.today().isoformat(),
        'account_id': ACCT,
        'period':     {'start': SINCE, 'end': UNTIL},
        'totals':     totals,
        'creatives':  creatives,
    }

    with open('meta-creatives-data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f'meta-creatives-data.json gerado - {len(creatives)} criativos, R$ {totals["spend"]:,.2f}')


if __name__ == '__main__':
    main()
