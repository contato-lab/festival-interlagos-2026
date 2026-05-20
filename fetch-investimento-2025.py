#!/usr/bin/env python3
"""
fetch-investimento-2025.py — script one-shot · VERSÃO VARREDURA TOTAL

Puxa o investimento REAL da campanha 2025 (14/03 → 15/06) das duas
plataformas via API e grava em investimento-2025.json.

Estratégia (após validação do cliente):
- META: descobre TODAS as ad accounts que o token tem acesso e soma cada uma.
  Em 2025 a Lime usava múltiplas contas (oficial + parceiros). Sem filtro de
  nome — todas as campanhas com gasto entram.
- GOOGLE: descobre TODOS os customers acessíveis via MCC e soma cada um. Sem
  filtro de nome também.

O resultado vem como breakdown por conta + breakdown por campanha pra você
auditar qualquer divergência contra a planilha.
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
# META ADS — varredura de todas as contas
# ═══════════════════════════════════════════════════════════════════════
META_TOKEN   = os.environ.get('META_TOKEN', '')
META_VERSION = 'v21.0'


def meta_get(path, params=None):
    p = dict(params or {})
    p['access_token'] = META_TOKEN
    url = f'https://graph.facebook.com/{META_VERSION}/{path}?{urllib.parse.urlencode(p)}'
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.loads(resp.read())


def meta_list_accounts():
    """Descobre todas as ad accounts que o token enxerga (via /me/adaccounts)."""
    out = []
    url_path = 'me/adaccounts'
    params = {
        'fields': 'id,account_id,name,account_status',
        'limit':  '200',
    }
    next_url = None
    payload = meta_get(url_path, params)
    while payload:
        for acct in payload.get('data', []):
            out.append({
                'id':     acct.get('id'),  # já vem como act_XXX
                'name':   acct.get('name', ''),
                'status': acct.get('account_status'),
            })
        next_url = (payload.get('paging') or {}).get('next')
        if not next_url:
            break
        with urllib.request.urlopen(next_url, timeout=60) as resp:
            payload = json.loads(resp.read())
    return out


def meta_fetch_account_insights(acct_id):
    """Pega insights por campanha+dia da conta no período."""
    params = {
        'fields':         'date_start,campaign_id,campaign_name,impressions,clicks,spend,actions',
        'time_range':     json.dumps({'since': SINCE, 'until': UNTIL}),
        'time_increment': '1',
        'level':          'campaign',
        'limit':          '500',
        'access_token':   META_TOKEN,
    }
    url = f'https://graph.facebook.com/{META_VERSION}/{acct_id}/insights?{urllib.parse.urlencode(params)}'
    rows = []
    while url:
        with urllib.request.urlopen(url, timeout=60) as resp:
            payload = json.loads(resp.read())
        rows.extend(payload.get('data', []))
        url = (payload.get('paging') or {}).get('next')
    return rows


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

    print(f'📊 Meta Ads · descobrindo contas acessíveis pelo token...', flush=True)
    try:
        accounts = meta_list_accounts()
    except Exception as e:
        print(f'   ❌ falha listando contas: {e}', file=sys.stderr)
        return None
    print(f'   ✅ {len(accounts)} ad accounts visíveis', flush=True)
    for a in accounts[:30]:
        st = a.get('status')
        # status 1=active, 2=disabled, 3=unsettled, 7=pending_risk_review, 101=closed
        st_lbl = {1:'active',2:'disabled',3:'unsettled',7:'pending',101:'closed'}.get(st, str(st))
        print(f'      {a["id"]:25} [{st_lbl:8}] {a["name"]}', flush=True)
    if len(accounts) > 30:
        print(f'      ... +{len(accounts)-30} contas (truncado)')

    all_by_account = []
    daily_by_date  = {}
    total_cost     = 0.0
    total_imps     = 0
    total_clicks   = 0
    total_purch    = 0
    all_campaigns_global = []

    for acct in accounts:
        try:
            rows = meta_fetch_account_insights(acct['id'])
        except Exception as e:
            err_str = str(e)[:120]
            print(f'   ⚠️  {acct["id"]} ({acct["name"]}): {err_str}', file=sys.stderr)
            continue

        if not rows:
            continue

        by_camp = {}
        acct_cost = 0.0
        acct_imps = 0
        acct_clk  = 0
        acct_pur  = 0
        for r in rows:
            spend = float(r.get('spend', 0))
            imps  = int(r.get('impressions', 0))
            clk   = int(r.get('clicks', 0))
            pur   = int(meta_purchases(r.get('actions')))

            acct_cost += spend
            acct_imps += imps
            acct_clk  += clk
            acct_pur  += pur

            cid = r.get('campaign_id') or 'unknown'
            if cid not in by_camp:
                by_camp[cid] = {
                    'id':   cid,
                    'name': r.get('campaign_name', ''),
                    'cost': 0.0,
                    'impressions': 0,
                    'clicks': 0,
                    'purchases': 0,
                }
            by_camp[cid]['cost']        += spend
            by_camp[cid]['impressions'] += imps
            by_camp[cid]['clicks']      += clk
            by_camp[cid]['purchases']   += pur

            d = r['date_start']
            if d not in daily_by_date:
                daily_by_date[d] = {'date': d, 'cost': 0.0, 'impressions': 0, 'clicks': 0, 'purchases': 0}
            daily_by_date[d]['cost']        += spend
            daily_by_date[d]['impressions'] += imps
            daily_by_date[d]['clicks']      += clk
            daily_by_date[d]['purchases']   += pur

        # Só registra conta se gastou algo
        if acct_cost > 0:
            campaigns = sorted(by_camp.values(), key=lambda x: -x['cost'])
            for c in campaigns:
                c['cost'] = round(c['cost'], 2)
                c['account_id']   = acct['id']
                c['account_name'] = acct['name']
                all_campaigns_global.append(c)
            all_by_account.append({
                'account_id':   acct['id'],
                'account_name': acct['name'],
                'cost':         round(acct_cost, 2),
                'impressions':  acct_imps,
                'clicks':       acct_clk,
                'purchases':    acct_pur,
                'campaigns':    campaigns,
            })
            total_cost   += acct_cost
            total_imps   += acct_imps
            total_clicks += acct_clk
            total_purch  += acct_pur
            print(f'   📦 {acct["id"]:25} {acct["name"][:35]:35} → R$ {acct_cost:>11,.2f} ({len(campaigns)} campanhas)', flush=True)

    daily = sorted(daily_by_date.values(), key=lambda x: x['date'])
    for d in daily:
        d['cost'] = round(d['cost'], 2)

    print(f'\n   ✅ META TOTAL: R$ {total_cost:,.2f} em {len(daily)} dias · {len(all_by_account)} contas ativas', flush=True)

    return {
        'totals': {
            'cost':        round(total_cost, 2),
            'impressions': total_imps,
            'clicks':      total_clicks,
            'purchases':   total_purch,
        },
        'daily':     daily,
        'accounts':  sorted(all_by_account, key=lambda x: -x['cost']),
        'campaigns': sorted(all_campaigns_global, key=lambda x: -x['cost'])[:50],  # top 50 pra debug
    }


# ═══════════════════════════════════════════════════════════════════════
# GOOGLE ADS — varredura de todos os customers do MCC
# ═══════════════════════════════════════════════════════════════════════
def fetch_google_2025():
    try:
        from google.ads.googleads.client import GoogleAdsClient
        from google.ads.googleads.errors import GoogleAdsException
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
    login_customer_id  = os.environ['GOOGLE_ADS_LOGIN_CUSTOMER_ID']
    main_customer_id   = os.environ['GOOGLE_ADS_CUSTOMER_ID']

    print(f'\n📊 Google Ads · descobrindo customers acessíveis pelo MCC {login_customer_id}...', flush=True)

    # Lista customers acessíveis a partir do MCC
    customer_ids = []
    try:
        customer_service = client.get_service('CustomerService')
        accessible = customer_service.list_accessible_customers()
        # resource names tipo 'customers/1234567890'
        for rn in accessible.resource_names:
            customer_ids.append(rn.split('/')[-1])
        print(f'   ✅ {len(customer_ids)} customers via list_accessible_customers', flush=True)
    except Exception as e:
        print(f'   ⚠️  list_accessible_customers falhou: {str(e)[:120]}', file=sys.stderr)

    # Como o list_accessible só lista os DIRETOS, descobre também os filhos do MCC
    try:
        ga = client.get_service('GoogleAdsService')
        q = """
            SELECT customer_client.id, customer_client.descriptive_name,
                   customer_client.manager, customer_client.status
            FROM customer_client
            WHERE customer_client.manager = FALSE
        """
        resp = ga.search(customer_id=login_customer_id, query=q)
        for row in resp:
            cid = str(row.customer_client.id)
            if cid not in customer_ids:
                customer_ids.append(cid)
        print(f'   ✅ {len(customer_ids)} customers totais (incluindo filhos do MCC)', flush=True)
    except Exception as e:
        print(f'   ⚠️  enumeração de filhos do MCC falhou: {str(e)[:120]}', file=sys.stderr)

    # Garante que o customer principal está na lista
    if main_customer_id not in customer_ids:
        customer_ids.insert(0, main_customer_id)

    all_by_account = []
    daily_by_date  = {}
    total_cost     = 0.0
    total_imps     = 0
    total_clk      = 0
    total_conv     = 0.0
    all_campaigns_global = []

    ga_service = client.get_service('GoogleAdsService')

    for cid in customer_ids:
        try:
            query = f"""
                SELECT
                    customer.descriptive_name,
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
            response = ga_service.search_stream(customer_id=cid, query=query)

            cust_name = ''
            by_camp = {}
            acct_cost = 0.0
            acct_imps = 0
            acct_clk  = 0
            acct_conv = 0.0
            for batch in response:
                for row in batch.results:
                    cust_name = row.customer.descriptive_name
                    cost = (row.metrics.cost_micros or 0) / 1_000_000.0
                    camp_id   = str(row.campaign.id)
                    camp_name = row.campaign.name
                    d = row.segments.date

                    acct_cost += cost
                    acct_imps += row.metrics.impressions or 0
                    acct_clk  += row.metrics.clicks or 0
                    acct_conv += row.metrics.conversions or 0

                    if camp_id not in by_camp:
                        by_camp[camp_id] = {
                            'id':   camp_id,
                            'name': camp_name,
                            'cost': 0.0,
                            'impressions': 0,
                            'clicks': 0,
                            'conversions': 0.0,
                        }
                    by_camp[camp_id]['cost']        += cost
                    by_camp[camp_id]['impressions'] += row.metrics.impressions or 0
                    by_camp[camp_id]['clicks']      += row.metrics.clicks or 0
                    by_camp[camp_id]['conversions'] += row.metrics.conversions or 0

                    if d not in daily_by_date:
                        daily_by_date[d] = {'date': d, 'cost': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0.0}
                    daily_by_date[d]['cost']        += cost
                    daily_by_date[d]['impressions'] += row.metrics.impressions or 0
                    daily_by_date[d]['clicks']      += row.metrics.clicks or 0
                    daily_by_date[d]['conversions'] += row.metrics.conversions or 0

            if acct_cost > 0:
                campaigns = sorted(by_camp.values(), key=lambda x: -x['cost'])
                for c in campaigns:
                    c['cost'] = round(c['cost'], 2)
                    c['conversions'] = round(c['conversions'], 2)
                    c['customer_id']   = cid
                    c['customer_name'] = cust_name
                    all_campaigns_global.append(c)
                all_by_account.append({
                    'customer_id':   cid,
                    'customer_name': cust_name,
                    'cost':          round(acct_cost, 2),
                    'impressions':   acct_imps,
                    'clicks':        acct_clk,
                    'conversions':   round(acct_conv, 2),
                    'campaigns':     campaigns,
                })
                total_cost += acct_cost
                total_imps += acct_imps
                total_clk  += acct_clk
                total_conv += acct_conv
                print(f'   📦 {cid:12} {cust_name[:35]:35} → R$ {acct_cost:>11,.2f} ({len(campaigns)} campanhas)', flush=True)
        except Exception as e:
            err = str(e)[:120]
            # Customer pode estar sem permissão — apenas registra e segue
            if 'PERMISSION_DENIED' in err or 'AUTHENTICATION_ERROR' in err or 'CUSTOMER_NOT_FOUND' in err:
                continue
            print(f'   ⚠️  {cid}: {err}', file=sys.stderr)

    daily = sorted(daily_by_date.values(), key=lambda x: x['date'])
    for d in daily:
        d['cost'] = round(d['cost'], 2)
        d['conversions'] = round(d['conversions'], 2)

    print(f'\n   ✅ GOOGLE TOTAL: R$ {total_cost:,.2f} em {len(daily)} dias · {len(all_by_account)} customers ativos', flush=True)

    return {
        'totals': {
            'cost':        round(total_cost, 2),
            'impressions': total_imps,
            'clicks':      total_clk,
            'conversions': round(total_conv, 2),
        },
        'daily':     daily,
        'accounts':  sorted(all_by_account, key=lambda x: -x['cost']),
        'campaigns': sorted(all_campaigns_global, key=lambda x: -x['cost'])[:50],
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
