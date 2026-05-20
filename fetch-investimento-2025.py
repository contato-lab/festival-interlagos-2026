#!/usr/bin/env python3
"""
fetch-investimento-2025.py — script one-shot

Puxa o investimento REAL da campanha 2025 (14/03 → 15/06) via API:
- META:   act_2044706169171045 (Duas Rodas — Conta Principal)
- GOOGLE: customer 5879952911 (Duas Rodas)

Confirmado pelo cliente: todas as campanhas ativas no período eram do
Festival Interlagos. Sem filtro de nome — soma TUDO que rodou.

Gera investimento-2025.json com daily + breakdown por campanha.
"""

import os
import json
import sys
import urllib.request
import urllib.parse
from datetime import date, datetime, timedelta

SINCE = '2025-03-14'
UNTIL = '2025-06-15'

# ═══════════════════════════════════════════════════════════════════════
# META ADS
# ═══════════════════════════════════════════════════════════════════════
META_TOKEN   = os.environ.get('META_TOKEN', '')
META_ACCT    = 'act_2044706169171045'
META_VERSION = 'v21.0'


def meta_purchases(actions):
    if not actions:
        return 0
    p = sum(float(a['value']) for a in actions if a.get('action_type') == 'purchase')
    o = sum(float(a['value']) for a in actions if a.get('action_type') == 'omni_purchase')
    return max(p, o)


def fetch_meta_2025():
    if not META_TOKEN:
        print('⚠️  META_TOKEN não definido — pulando Meta', file=sys.stderr)
        return None

    print(f'📊 Meta · {META_ACCT} · {SINCE} → {UNTIL}', flush=True)

    params = {
        'fields':         'date_start,campaign_id,campaign_name,impressions,clicks,spend,actions',
        'time_range':     json.dumps({'since': SINCE, 'until': UNTIL}),
        'time_increment': '1',
        'level':          'campaign',
        'limit':          '500',
        'access_token':   META_TOKEN,
    }
    url = f'https://graph.facebook.com/{META_VERSION}/{META_ACCT}/insights?{urllib.parse.urlencode(params)}'
    rows = []
    while url:
        with urllib.request.urlopen(url, timeout=60) as resp:
            payload = json.loads(resp.read())
        rows.extend(payload.get('data', []))
        url = (payload.get('paging') or {}).get('next')

    by_camp = {}
    by_date = {}
    for r in rows:
        spend = float(r.get('spend', 0))
        imps  = int(r.get('impressions', 0))
        clk   = int(r.get('clicks', 0))
        pur   = int(meta_purchases(r.get('actions')))
        cid   = r.get('campaign_id') or 'unknown'

        if cid not in by_camp:
            by_camp[cid] = {
                'id':   cid,
                'name': r.get('campaign_name', ''),
                'cost': 0.0, 'impressions': 0, 'clicks': 0, 'purchases': 0,
            }
        by_camp[cid]['cost']        += spend
        by_camp[cid]['impressions'] += imps
        by_camp[cid]['clicks']      += clk
        by_camp[cid]['purchases']   += pur

        d = r['date_start']
        if d not in by_date:
            by_date[d] = {'date': d, 'cost': 0.0, 'impressions': 0, 'clicks': 0, 'purchases': 0}
        by_date[d]['cost']        += spend
        by_date[d]['impressions'] += imps
        by_date[d]['clicks']      += clk
        by_date[d]['purchases']   += pur

    daily = sorted(by_date.values(), key=lambda x: x['date'])
    for d in daily:
        d['cost'] = round(d['cost'], 2)
    campaigns = sorted(by_camp.values(), key=lambda x: -x['cost'])
    for c in campaigns:
        c['cost'] = round(c['cost'], 2)

    totals = {
        'cost':        round(sum(d['cost'] for d in daily), 2),
        'impressions': sum(d['impressions'] for d in daily),
        'clicks':      sum(d['clicks'] for d in daily),
        'purchases':   sum(d['purchases'] for d in daily),
    }

    print(f'   ✅ Meta: R$ {totals["cost"]:,.2f} · {len(campaigns)} campanhas · {len(daily)} dias', flush=True)

    return {
        'account_id': META_ACCT,
        'totals':     totals,
        'daily':      daily,
        'campaigns':  campaigns,
    }


# ═══════════════════════════════════════════════════════════════════════
# GOOGLE ADS
# ═══════════════════════════════════════════════════════════════════════
def fetch_google_2025():
    try:
        from google.ads.googleads.client import GoogleAdsClient
    except ImportError:
        print('⚠️  google-ads-python não instalado — pulando Google', file=sys.stderr)
        return None

    required = ['GOOGLE_ADS_DEVELOPER_TOKEN', 'GOOGLE_ADS_CLIENT_ID',
                'GOOGLE_ADS_CLIENT_SECRET', 'GOOGLE_ADS_REFRESH_TOKEN',
                'GOOGLE_ADS_CUSTOMER_ID', 'GOOGLE_ADS_LOGIN_CUSTOMER_ID']
    for r in required:
        if not os.environ.get(r):
            print(f'⚠️  {r} não definido — pulando Google', file=sys.stderr)
            return None

    config = {
        'developer_token':   os.environ['GOOGLE_ADS_DEVELOPER_TOKEN'],
        'client_id':         os.environ['GOOGLE_ADS_CLIENT_ID'],
        'client_secret':     os.environ['GOOGLE_ADS_CLIENT_SECRET'],
        'refresh_token':     os.environ['GOOGLE_ADS_REFRESH_TOKEN'],
        'login_customer_id': os.environ['GOOGLE_ADS_LOGIN_CUSTOMER_ID'],
        'use_proto_plus':    True,
    }
    client = GoogleAdsClient.load_from_dict(config)
    customer_id = os.environ['GOOGLE_ADS_CUSTOMER_ID']  # 5879952911 (Duas Rodas)

    print(f'📊 Google · customer {customer_id} · {SINCE} → {UNTIL}', flush=True)

    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions
        FROM campaign
        WHERE segments.date BETWEEN '{SINCE}' AND '{UNTIL}'
          AND campaign.status != 'REMOVED'
    """
    ga = client.get_service('GoogleAdsService')
    response = ga.search_stream(customer_id=customer_id, query=query)

    by_camp = {}
    by_date = {}
    for batch in response:
        for row in batch.results:
            cost = (row.metrics.cost_micros or 0) / 1_000_000.0
            cid  = str(row.campaign.id)
            name = row.campaign.name
            d    = row.segments.date

            if cid not in by_camp:
                by_camp[cid] = {
                    'id':   cid, 'name': name, 'cost': 0.0,
                    'impressions': 0, 'clicks': 0, 'conversions': 0.0,
                }
            by_camp[cid]['cost']        += cost
            by_camp[cid]['impressions'] += row.metrics.impressions or 0
            by_camp[cid]['clicks']      += row.metrics.clicks or 0
            by_camp[cid]['conversions'] += row.metrics.conversions or 0

            if d not in by_date:
                by_date[d] = {'date': d, 'cost': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0.0}
            by_date[d]['cost']        += cost
            by_date[d]['impressions'] += row.metrics.impressions or 0
            by_date[d]['clicks']      += row.metrics.clicks or 0
            by_date[d]['conversions'] += row.metrics.conversions or 0

    daily = sorted(by_date.values(), key=lambda x: x['date'])
    for d in daily:
        d['cost'] = round(d['cost'], 2)
        d['conversions'] = round(d['conversions'], 2)
    campaigns = sorted(by_camp.values(), key=lambda x: -x['cost'])
    for c in campaigns:
        c['cost'] = round(c['cost'], 2)
        c['conversions'] = round(c['conversions'], 2)

    totals = {
        'cost':        round(sum(d['cost'] for d in daily), 2),
        'impressions': sum(d['impressions'] for d in daily),
        'clicks':      sum(d['clicks'] for d in daily),
        'conversions': round(sum(d['conversions'] for d in daily), 2),
    }

    print(f'   ✅ Google: R$ {totals["cost"]:,.2f} · {len(campaigns)} campanhas · {len(daily)} dias', flush=True)

    return {
        'customer_id': customer_id,
        'totals':      totals,
        'daily':       daily,
        'campaigns':   campaigns,
    }


# ═══════════════════════════════════════════════════════════════════════
# MERGE + OUTPUT
# ═══════════════════════════════════════════════════════════════════════
def build_diario_campanha(meta, google):
    start = datetime.fromisoformat(SINCE).date()
    end   = datetime.fromisoformat(UNTIL).date()
    dias  = (end - start).days + 1
    meta_by_date   = {d['date']: d['cost'] for d in (meta or {}).get('daily', [])}
    google_by_date = {d['date']: d['cost'] for d in (google or {}).get('daily', [])}
    out = []
    for i in range(dias):
        dt = start + timedelta(days=i)
        iso = dt.isoformat()
        m = meta_by_date.get(iso, 0.0)
        g = google_by_date.get(iso, 0.0)
        out.append({
            'd':      i + 1,
            'date':   iso,
            'meta':   round(m, 2),
            'google': round(g, 2),
            'total':  round(m + g, 2),
        })
    return out


def main():
    meta   = fetch_meta_2025()
    google = fetch_google_2025()
    diario = build_diario_campanha(meta, google)

    meta_cost   = (meta or {}).get('totals', {}).get('cost', 0) or 0
    google_cost = (google or {}).get('totals', {}).get('cost', 0) or 0
    total_cost  = round(meta_cost + google_cost, 2)

    output = {
        'updated_at': date.today().isoformat(),
        'campanha': {
            'start': SINCE,
            'end':   UNTIL,
            'dias':  (datetime.fromisoformat(UNTIL).date() - datetime.fromisoformat(SINCE).date()).days + 1,
        },
        'meta':   meta,
        'google': google,
        'totals': {
            'cost_total':  total_cost,
            'meta_cost':   meta_cost,
            'google_cost': google_cost,
        },
        'diario_por_dia_campanha': diario,
    }

    with open('investimento-2025.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print()
    print('═════════════════════════════════════════════════')
    print(f'✅ investimento-2025.json gerado')
    print(f'   Meta:   R$ {meta_cost:>12,.2f}')
    print(f'   Google: R$ {google_cost:>12,.2f}')
    print(f'   TOTAL:  R$ {total_cost:>12,.2f}')
    if total_cost > 0:
        print(f'   {len(diario)} dias · média/dia R$ {total_cost/len(diario):.2f}')
    print('═════════════════════════════════════════════════')


if __name__ == '__main__':
    main()
