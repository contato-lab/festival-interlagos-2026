#!/usr/bin/env python3
"""
update-regions.py
Festival Interlagos 2026 — Geographic breakdown per campaign
Gera regions-data.json com investimento por campanha + região (UF) para Meta e Google Ads.
"""

import json
import os
import urllib.request
import urllib.parse
from datetime import date, timedelta

# ── Configurações ───────────────────────────────────────────────────────────
META_TOKEN  = os.environ.get('META_TOKEN', '')
META_ACCT   = os.environ.get('META_ACCT', 'act_2044706169171045')
SINCE       = '2026-03-31'
UNTIL       = date.today().strftime('%Y-%m-%d')
API_VERSION = 'v21.0'

# ── Mapeamento UF (sigla) ───────────────────────────────────────────────────
# Meta retorna estados em português ("São Paulo"). Convertemos para UF.
ESTADO_PARA_UF = {
    'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM',
    'Bahia': 'BA', 'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES',
    'Goiás': 'GO', 'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS',
    'Minas Gerais': 'MG', 'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR',
    'Pernambuco': 'PE', 'Piauí': 'PI', 'Rio de Janeiro': 'RJ',
    'Rio Grande do Norte': 'RN', 'Rio Grande do Sul': 'RS', 'Rondônia': 'RO',
    'Roraima': 'RR', 'Santa Catarina': 'SC', 'São Paulo': 'SP',
    'Sergipe': 'SE', 'Tocantins': 'TO',
}

# ── Classifica campanha por evento ──────────────────────────────────────────
def classificar_evento(nome_campanha: str) -> str:
    n = nome_campanha.upper()
    if '[MOTO]' in n or 'MOTO' in n.split():
        return 'MOTO'
    if '[AUTO]' in n or 'AUTO' in n.split() or 'AUTO PMAX' in n:
        return 'AUTO'
    return 'GERAL'  # PMAX/PESQUI/RMKT/NOVA DEMANDA — atende ambos

def classificar_plataforma(nome_campanha: str) -> str:
    n = nome_campanha.upper()
    if 'TICKET' in n or 'TM' in n.split() or 'EVENTIM' in n:
        return 'TICKETMASTER'
    return 'NOSSO'  # default — sistema próprio


# ════════════════════════════════════════════════════════════════════════
# META ADS — Spend por campanha + região
# ════════════════════════════════════════════════════════════════════════
def fetch_meta_regions():
    if not META_TOKEN:
        print('  ⚠ META_TOKEN não definido, pulando Meta')
        return []

    params = {
        'fields':         'campaign_id,campaign_name,spend',
        'breakdowns':     'region',
        'level':          'campaign',
        'time_range':     json.dumps({'since': SINCE, 'until': UNTIL}),
        'access_token':   META_TOKEN,
        'limit':          '500',
    }
    url = f'https://graph.facebook.com/{API_VERSION}/{META_ACCT}/insights?{urllib.parse.urlencode(params)}'

    rows = []
    while url:
        with urllib.request.urlopen(url, timeout=60) as resp:
            data = json.loads(resp.read())
        for r in data.get('data', []):
            estado = r.get('region', '').strip()
            uf = ESTADO_PARA_UF.get(estado, estado[:2].upper() if estado else 'OUTROS')
            rows.append({
                'campaign_id':   r.get('campaign_id', ''),
                'campaign_name': r.get('campaign_name', ''),
                'evento':        classificar_evento(r.get('campaign_name', '')),
                'plataforma':    classificar_plataforma(r.get('campaign_name', '')),
                'estado':        estado,
                'uf':            uf,
                'spend':         float(r.get('spend', 0) or 0),
            })
        # paginação
        paging = data.get('paging', {})
        url = paging.get('next')

    return rows


# ════════════════════════════════════════════════════════════════════════
# GOOGLE ADS — Spend por campanha + região
# ════════════════════════════════════════════════════════════════════════
def fetch_google_regions():
    try:
        from google.ads.googleads.client import GoogleAdsClient
    except ImportError:
        print('  ⚠ google-ads não instalado, pulando Google')
        return []

    needed = ['GOOGLE_ADS_DEVELOPER_TOKEN', 'GOOGLE_ADS_CLIENT_ID', 'GOOGLE_ADS_CLIENT_SECRET',
              'GOOGLE_ADS_REFRESH_TOKEN', 'GOOGLE_ADS_CUSTOMER_ID', 'GOOGLE_ADS_LOGIN_CUSTOMER_ID']
    if not all(os.environ.get(k) for k in needed):
        print('  ⚠ Google Ads secrets ausentes, pulando')
        return []

    config = {
        'developer_token':   os.environ['GOOGLE_ADS_DEVELOPER_TOKEN'],
        'client_id':         os.environ['GOOGLE_ADS_CLIENT_ID'],
        'client_secret':     os.environ['GOOGLE_ADS_CLIENT_SECRET'],
        'refresh_token':     os.environ['GOOGLE_ADS_REFRESH_TOKEN'],
        'login_customer_id': os.environ['GOOGLE_ADS_LOGIN_CUSTOMER_ID'],
        'use_proto_plus':    True,
    }
    client = GoogleAdsClient.load_from_dict(config)
    ga_service = client.get_service('GoogleAdsService')
    customer_id = os.environ['GOOGLE_ADS_CUSTOMER_ID']

    today = date.today()
    start_date = today - timedelta(days=60)

    # Query 1: gasto por campanha + region resource
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            geographic_view.country_criterion_id,
            segments.geo_target_region,
            metrics.cost_micros
        FROM geographic_view
        WHERE segments.date BETWEEN '{start_date}' AND '{today}'
            AND campaign.status != 'REMOVED'
            AND campaign.name LIKE '%FESTIVAL INTERLAGOS%'
    """
    response = ga_service.search_stream(customer_id=customer_id, query=query)

    rows = []
    region_resources = set()
    for batch in response:
        for r in batch.results:
            region_res = r.segments.geo_target_region
            cost = r.metrics.cost_micros / 1_000_000
            if cost <= 0:
                continue
            rows.append({
                'campaign_id':     str(r.campaign.id),
                'campaign_name':   r.campaign.name,
                'region_resource': region_res,
                'spend':           cost,
            })
            if region_res:
                region_resources.add(region_res)

    # Query 2: lookup canonical names dos region resources
    name_map = {}
    if region_resources:
        ids_str = ','.join(f"'{r}'" for r in region_resources)
        name_query = f"""
            SELECT
                geo_target_constant.resource_name,
                geo_target_constant.canonical_name,
                geo_target_constant.name,
                geo_target_constant.country_code,
                geo_target_constant.target_type
            FROM geo_target_constant
            WHERE geo_target_constant.resource_name IN ({ids_str})
        """
        name_response = ga_service.search_stream(customer_id=customer_id, query=name_query)
        for batch in name_response:
            for r in batch.results:
                # canonical_name = "São Paulo,Brazil" — pegar só o estado
                canonical = r.geo_target_constant.canonical_name or ''
                estado = canonical.split(',')[0].strip()
                name_map[r.geo_target_constant.resource_name] = estado

    # Anexa nome + UF + classificação
    enriched = []
    for r in rows:
        estado = name_map.get(r['region_resource'], 'Desconhecido')
        uf = ESTADO_PARA_UF.get(estado, estado[:2].upper() if estado else 'OUTROS')
        enriched.append({
            'campaign_id':   r['campaign_id'],
            'campaign_name': r['campaign_name'],
            'evento':        classificar_evento(r['campaign_name']),
            'plataforma':    classificar_plataforma(r['campaign_name']),
            'estado':        estado,
            'uf':            uf,
            'spend':         r['spend'],
        })

    return enriched


# ════════════════════════════════════════════════════════════════════════
# AGREGADORES
# ════════════════════════════════════════════════════════════════════════
def agregar(rows):
    """Agrega por evento → plataforma → UF."""
    matriz = {}
    for r in rows:
        ev = r['evento']
        pl = r['plataforma']
        uf = r['uf']
        spend = r['spend']
        matriz.setdefault(ev, {}).setdefault(pl, {}).setdefault(uf, 0.0)
        matriz[ev][pl][uf] += spend

    # arredonda
    for ev in matriz:
        for pl in matriz[ev]:
            for uf in matriz[ev][pl]:
                matriz[ev][pl][uf] = round(matriz[ev][pl][uf], 2)
    return matriz


def agregar_geral_por_uf(meta_rows, google_rows):
    """Total combinado Meta+Google por UF, separado por evento."""
    out = {}
    for r in meta_rows + google_rows:
        ev = r['evento']
        uf = r['uf']
        out.setdefault(ev, {}).setdefault(uf, 0.0)
        out[ev][uf] += r['spend']
    for ev in out:
        for uf in out[ev]:
            out[ev][uf] = round(out[ev][uf], 2)
    return out


# ════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════
def main():
    print(f'🌍 Fetching regions data — {SINCE} a {UNTIL}')

    print('  📘 Meta Ads...')
    meta_rows = fetch_meta_regions()
    print(f'     ✓ {len(meta_rows)} linhas')

    print('  📕 Google Ads...')
    google_rows = fetch_google_regions()
    print(f'     ✓ {len(google_rows)} linhas')

    output = {
        'updated_at': date.today().isoformat(),
        'period':     {'start': SINCE, 'end': UNTIL},
        'meta': {
            'rows':  meta_rows,
            'matriz': agregar(meta_rows),
        },
        'google': {
            'rows':  google_rows,
            'matriz': agregar(google_rows),
        },
        'consolidado_por_evento_uf': agregar_geral_por_uf(meta_rows, google_rows),
    }

    with open('regions-data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total_meta   = sum(r['spend'] for r in meta_rows)
    total_google = sum(r['spend'] for r in google_rows)
    print(f'✅ regions-data.json gerado')
    print(f'   Meta:   R$ {total_meta:,.2f}')
    print(f'   Google: R$ {total_google:,.2f}')
    print(f'   TOTAL:  R$ {total_meta + total_google:,.2f}')


if __name__ == '__main__':
    main()
