#!/usr/bin/env python3
"""
fetch-investimento-2025.py — script one-shot

Puxa o investimento REAL da campanha 2025 (14/03 → 15/06) das duas
plataformas e grava em investimento-2025.json. Substitui os valores
hardcoded (médias semanais) que estavam vindo da planilha.

Roda via workflow_dispatch (manual) — não precisa rodar de novo
até o cliente pedir uma nova baseline.

Saída: investimento-2025.json com estrutura:
{
  "campanha": {"start": "2025-03-14", "end": "2025-06-15", "dias": 94},
  "meta": {
    "totals":  {"cost": ..., "impressions": ..., "clicks": ..., "purchases": ...},
    "daily":   [{"date":"2025-03-14","cost":...}, ...],
    "campaigns": [{"id":"...","name":"...","cost":...}, ...]
  },
  "google": { mesma estrutura },
  "totals": {"cost_total": ..., "meta_cost": ..., "google_cost": ...},
  "diario_por_dia_campanha": [
    {"d": 1, "date": "2025-03-14", "meta": 1234.56, "google": 567.89, "total": 1802.45},
    ...
  ]
}
"""

import os
import json
import sys
import urllib.request
import urllib.parse
from datetime import date, datetime, timedelta

# ── Período da campanha 2025 ────────────────────────────────────────────
SINCE = '2025-03-14'
UNTIL = '2025-06-15'

# ── Filtro de campanha (nome) ───────────────────────────────────────────
# Em 2026 usamos 'FESTIVAL INTERLAGOS' no nome. Em 2025 pode ter sido
# o mesmo padrão (ou variações). Se não pegar nada, o script lista todas
# as campanhas ativas no período pra você ajustar o filtro.
NAME_FILTER = os.environ.get('CAMPAIGN_NAME_FILTER', 'FESTIVAL INTERLAGOS').strip()

# ═══════════════════════════════════════════════════════════════════════
# META ADS
# ═══════════════════════════════════════════════════════════════════════
META_TOKEN   = os.environ.get('META_TOKEN', '')
META_ACCT    = os.environ.get('META_ACCT', 'act_2044706169171045')
META_VERSION = 'v21.0'


def meta_fetch(level: str):
    """Busca insights da conta no nível desejado (account ou campaign)."""
    params = {
        'fields':         'date_start,campaign_id,campaign_name,impressions,clicks,spend,actions',
        'time_range':     json.dumps({'since': SINCE, 'until': UNTIL}),
        'time_increment': '1',
        'level':          level,
        'access_token':   META_TOKEN,
        'limit':          '500',
    }
    url = f'https://graph.facebook.com/{META_VERSION}/{META_ACCT}/insights?{urllib.parse.urlencode(params)}'
    out = {'data': []}
    while url:
        with urllib.request.urlopen(url, timeout=60) as resp:
            payload = json.loads(resp.read())
        out['data'].extend(payload.get('data', []))
        # paginação
        url = (payload.get('paging') or {}).get('next')
    return out


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

    print(f'📊 Meta Ads · {SINCE} → {UNTIL} · conta {META_ACCT}', flush=True)

    # Pega por CAMPANHA (pra filtrar pelo nome) e depois agrega por dia
    raw = meta_fetch(level='campaign')
    print(f'   {len(raw["data"])} linhas (campanha × dia)', flush=True)

    # Lista todas as campanhas únicas
    all_campaigns = {}
    for row in raw['data']:
        cid = row.get('campaign_id')
        if cid not in all_campaigns:
            all_campaigns[cid] = {
                'id': cid,
                'name': row.get('campaign_name', ''),
                'cost': 0.0,
                'impressions': 0,
                'clicks': 0,
                'purchases': 0,
            }
        all_campaigns[cid]['cost']        += float(row.get('spend', 0))
        all_campaigns[cid]['impressions'] += int(row.get('impressions', 0))
        all_campaigns[cid]['clicks']      += int(row.get('clicks', 0))
        all_campaigns[cid]['purchases']   += int(meta_purchases(row.get('actions')))

    # Filtra campanhas por nome (case-insensitive contains)
    nf = NAME_FILTER.upper()
    matched = [c for c in all_campaigns.values() if nf in (c['name'] or '').upper()]

    print(f'   {len(all_campaigns)} campanhas no período · {len(matched)} com "{NAME_FILTER}"', flush=True)
    if not matched:
        print(f'   ⚠️  nenhuma campanha bateu com filtro "{NAME_FILTER}".')
        print(f'   Top 10 campanhas com gasto no período:')
        for c in sorted(all_campaigns.values(), key=lambda x: -x['cost'])[:10]:
            print(f'      R$ {c["cost"]:>10,.2f}  {c["name"]}')
        # Em caso de nenhum match, devolve TUDO pra você revisar
        matched = list(all_campaigns.values())

    matched_ids = {c['id'] for c in matched}

    # Agrega por dia (apenas campanhas matched)
    by_day = {}
    for row in raw['data']:
        if row.get('campaign_id') not in matched_ids:
            continue
        d = row['date_start']
        if d not in by_day:
            by_day[d] = {'date': d, 'cost': 0.0, 'impressions': 0, 'clicks': 0, 'purchases': 0}
        by_day[d]['cost']        += float(row.get('spend', 0))
        by_day[d]['impressions'] += int(row.get('impressions', 0))
        by_day[d]['clicks']      += int(row.get('clicks', 0))
        by_day[d]['purchases']   += int(meta_purchases(row.get('actions')))

    daily = sorted(by_day.values(), key=lambda x: x['date'])
    totals = {
        'cost':        round(sum(d['cost'] for d in daily), 2),
        'impressions': sum(d['impressions'] for d in daily),
        'clicks':      sum(d['clicks'] for d in daily),
        'purchases':   sum(d['purchases'] for d in daily),
    }

    # Arredonda custos
    for d in daily:
        d['cost'] = round(d['cost'], 2)
    for c in matched:
        c['cost'] = round(c['cost'], 2)

    print(f'   ✅ Meta: R$ {totals["cost"]:,.2f} em {len(daily)} dias · {totals["purchases"]} compras')

    return {
        'totals':    totals,
        'daily':     daily,
        'campaigns': sorted(matched, key=lambda x: -x['cost']),
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
    customer_id = os.environ['GOOGLE_ADS_CUSTOMER_ID']

    print(f'📊 Google Ads · {SINCE} → {UNTIL} · customer {customer_id}', flush=True)

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
        ORDER BY segments.date
    """

    ga_service = client.get_service('GoogleAdsService')
    response = ga_service.search_stream(customer_id=customer_id, query=query)

    all_campaigns = {}
    raw_rows = []
    for batch in response:
        for row in batch.results:
            cid = str(row.campaign.id)
            name = row.campaign.name
            cost = (row.metrics.cost_micros or 0) / 1_000_000.0
            d = row.segments.date  # 'YYYY-MM-DD'
            if cid not in all_campaigns:
                all_campaigns[cid] = {
                    'id': cid,
                    'name': name,
                    'cost': 0.0,
                    'impressions': 0,
                    'clicks': 0,
                    'conversions': 0.0,
                }
            all_campaigns[cid]['cost']        += cost
            all_campaigns[cid]['impressions'] += row.metrics.impressions or 0
            all_campaigns[cid]['clicks']      += row.metrics.clicks or 0
            all_campaigns[cid]['conversions'] += row.metrics.conversions or 0
            raw_rows.append({
                'date': d,
                'campaign_id': cid,
                'name': name,
                'cost': cost,
                'impressions': row.metrics.impressions or 0,
                'clicks': row.metrics.clicks or 0,
                'conversions': row.metrics.conversions or 0,
            })

    print(f'   {len(all_campaigns)} campanhas no período · {len(raw_rows)} linhas', flush=True)

    nf = NAME_FILTER.upper()
    matched = [c for c in all_campaigns.values() if nf in (c['name'] or '').upper()]
    print(f'   {len(matched)} bate com filtro "{NAME_FILTER}"', flush=True)

    if not matched:
        print(f'   ⚠️  nenhuma campanha bateu com filtro "{NAME_FILTER}".')
        print(f'   Top 10 campanhas com gasto no período:')
        for c in sorted(all_campaigns.values(), key=lambda x: -x['cost'])[:10]:
            print(f'      R$ {c["cost"]:>10,.2f}  {c["name"]}')
        matched = list(all_campaigns.values())

    matched_ids = {c['id'] for c in matched}

    by_day = {}
    for r in raw_rows:
        if r['campaign_id'] not in matched_ids:
            continue
        d = r['date']
        if d not in by_day:
            by_day[d] = {'date': d, 'cost': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0.0}
        by_day[d]['cost']        += r['cost']
        by_day[d]['impressions'] += r['impressions']
        by_day[d]['clicks']      += r['clicks']
        by_day[d]['conversions'] += r['conversions']

    daily = sorted(by_day.values(), key=lambda x: x['date'])
    for d in daily:
        d['cost'] = round(d['cost'], 2)
        d['conversions'] = round(d['conversions'], 2)
    for c in matched:
        c['cost'] = round(c['cost'], 2)
        c['conversions'] = round(c['conversions'], 2)

    totals = {
        'cost':        round(sum(d['cost'] for d in daily), 2),
        'impressions': sum(d['impressions'] for d in daily),
        'clicks':      sum(d['clicks'] for d in daily),
        'conversions': round(sum(d['conversions'] for d in daily), 2),
    }

    print(f'   ✅ Google: R$ {totals["cost"]:,.2f} em {len(daily)} dias · {totals["conversions"]} conv')

    return {
        'totals':    totals,
        'daily':     daily,
        'campaigns': sorted(matched, key=lambda x: -x['cost']),
    }


# ═══════════════════════════════════════════════════════════════════════
# MERGE + OUTPUT
# ═══════════════════════════════════════════════════════════════════════
def build_diario_campanha(meta, google):
    """Constrói tabela 'dia X da campanha → meta + google + total'.
    Permite o dashboard comparar D+1, D+2, ..., D+94 entre 2025 e 2026
    sem precisar mapear datas."""
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
            'filtro_nome': NAME_FILTER,
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
    print(f'   {len(diario)} dias de campanha · média/dia R$ {total_cost/len(diario):.2f}')
    print('═════════════════════════════════════════════════')


if __name__ == '__main__':
    main()
