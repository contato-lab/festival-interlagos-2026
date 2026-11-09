"""
update-google-ads.py (VOGE Brasil)
Busca o dia a dia da conta 624-443-6447 e grava voge/google-ads-data.json.
Roda no repositório do Festival porque as credenciais e o MCC da Lime já estão lá.

metrics.conversions traz só as conversões principais:
Lead (test ride e interesse), Contato Formulário e Contato WhatsApp.
"""
import json, os
from datetime import datetime, timedelta, timezone

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

BRT               = timezone(timedelta(hours=-3))
DEVELOPER_TOKEN   = os.environ['GOOGLE_ADS_DEVELOPER_TOKEN']
CLIENT_ID         = os.environ['GOOGLE_ADS_CLIENT_ID']
CLIENT_SECRET     = os.environ['GOOGLE_ADS_CLIENT_SECRET']
REFRESH_TOKEN     = os.environ['GOOGLE_ADS_REFRESH_TOKEN']
CUSTOMER_ID       = os.environ.get('VOGE_GOOGLE_CUSTOMER_ID', '6244436447').replace('-', '')
LOGIN_CUSTOMER_ID = os.environ.get('GOOGLE_ADS_LOGIN_CUSTOMER_ID', '4736269396').replace('-', '')
SINCE             = os.environ.get('GOOGLE_ADS_SINCE', '2026-11-01')   # início da campanha VOGE Sales Speed
OUT_FILE          = 'google-ads-data.json'


def get_client():
    return GoogleAdsClient.load_from_dict({
        'developer_token':   DEVELOPER_TOKEN,
        'client_id':         CLIENT_ID,
        'client_secret':     CLIENT_SECRET,
        'refresh_token':     REFRESH_TOKEN,
        'login_customer_id': LOGIN_CUSTOMER_ID,
        'use_proto_plus':    True,
    })


def fetch(client):
    svc = client.get_service('GoogleAdsService')
    until = datetime.now(BRT).strftime('%Y-%m-%d')
    query = f"""
        SELECT
            campaign.id, campaign.name, campaign.status, segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions
        FROM campaign
        WHERE segments.date BETWEEN '{SINCE}' AND '{until}'
            AND campaign.status != 'REMOVED'
        ORDER BY segments.date ASC
    """
    por_dia, campanhas = {}, {}
    for batch in svc.search_stream(customer_id=CUSTOMER_ID, query=query):
        for row in batch.results:
            iso   = row.segments.date
            custo = row.metrics.cost_micros / 1_000_000
            d = por_dia.setdefault(iso, {'dataISO': iso, 'data': f'{iso[8:10]}/{iso[5:7]}',
                                         'gasto': 0.0, 'impressoes': 0, 'cliques': 0, 'conversoes': 0.0})
            d['gasto']      += custo
            d['impressoes'] += row.metrics.impressions
            d['cliques']    += row.metrics.clicks
            d['conversoes'] += row.metrics.conversions

            c = campanhas.setdefault(str(row.campaign.id), {'id': str(row.campaign.id), 'nome': row.campaign.name,
                                                             'status': row.campaign.status.name, 'gasto': 0.0,
                                                             'impressoes': 0, 'cliques': 0, 'conversoes': 0.0})
            c['gasto'] += custo; c['impressoes'] += row.metrics.impressions
            c['cliques'] += row.metrics.clicks; c['conversoes'] += row.metrics.conversions

    dias = []
    for d in sorted(por_dia.values(), key=lambda x: x['dataISO']):
        d['gasto']      = round(d['gasto'], 2)
        d['conversoes'] = round(d['conversoes'], 2)
        d['ctr']        = round(d['cliques'] / d['impressoes'] * 100, 2) if d['impressoes'] else 0
        d['cpc']        = round(d['gasto'] / d['cliques'], 2) if d['cliques'] else None
        d['cpa']        = round(d['gasto'] / d['conversoes'], 2) if d['conversoes'] else None
        dias.append(d)
    for c in campanhas.values():
        c['gasto'] = round(c['gasto'], 2); c['conversoes'] = round(c['conversoes'], 2)

    return {
        'updated':   datetime.now(BRT).strftime('%d/%m/%Y %H:%M'),
        'account':   'VOGE Brasil | ' + CUSTOMER_ID,
        'since':     SINCE,
        'campanhas': sorted(campanhas.values(), key=lambda c: -c['gasto']),
        'data':      dias,
    }


def main():
    try:
        out = fetch(get_client())
    except GoogleAdsException as e:
        for err in e.failure.errors:
            print('Google Ads API:', err.message)
        raise
    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    tot = sum(d['gasto'] for d in out['data'])
    print(f"OK: {OUT_FILE}: {len(out['data'])} dias desde {SINCE}, R$ {tot:,.2f}, {len(out['campanhas'])} campanha(s)")


if __name__ == '__main__':
    main()
