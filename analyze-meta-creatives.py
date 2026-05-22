#!/usr/bin/env python3
"""
analyze-meta-creatives.py — script one-shot de análise (v2)

Gera análise focada dos últimos N dias (default 10) AGRUPANDO POR CRIATIVO
(não por ad_id), porque o mesmo criativo pode rodar em vários adsets/campanhas
e cada instância vira um ad_id diferente. Pra ter "compras totais do criativo
X", precisamos somar todas as instâncias.

Agrupamento:
- Por `creative.effective_object_story_id` (formato page_id_post_id) — chave
  ideal porque o post do FB/IG que virou anúncio é o creative real
- Fallback: por ad_name normalizado (sem prefix [ADx], lower, trimmed)

Output: top criativos por compras unificadas, com lista de adsets/campanhas
em que cada um rodou, e link pra Biblioteca de Anúncios do Meta (público).
"""

import os
import json
import re
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
    'ad_id', 'ad_name', 'adset_name', 'campaign_name', 'campaign_id',
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


def fetch_creative_details(ad_id):
    """Pega creative + post id + thumbnail.

    Retorna dict com: thumbnail_url, image_url, effective_object_story_id, page_id, post_id
    """
    try:
        params = {
            'fields':       'creative{id,thumbnail_url,image_url,effective_object_story_id,object_story_id}',
            'access_token': TOKEN,
        }
        url = f'https://graph.facebook.com/{API_VERSION}/{ad_id}?{urllib.parse.urlencode(params)}'
        data = api_get(url, timeout=20)
        creative = data.get('creative') or {}
        story_id = creative.get('effective_object_story_id') or creative.get('object_story_id') or ''
        page_id, post_id = '', ''
        if '_' in story_id:
            parts = story_id.split('_')
            page_id = parts[0]
            post_id = '_'.join(parts[1:])
        return {
            'creative_id':                 creative.get('id', ''),
            'thumbnail_url':               creative.get('thumbnail_url', ''),
            'image_url':                   creative.get('image_url', ''),
            'effective_object_story_id':   story_id,
            'page_id':                     page_id,
            'post_id':                     post_id,
        }
    except Exception as e:
        print(f'  ! creative detail falhou {ad_id}: {e}', file=sys.stderr)
        return {'creative_id':'','thumbnail_url':'','image_url':'',
                'effective_object_story_id':'','page_id':'','post_id':''}


def parse_purchases(actions):
    if not actions:
        return 0
    p = sum(float(a['value']) for a in actions if a.get('action_type') == 'purchase')
    o = sum(float(a['value']) for a in actions if a.get('action_type') == 'omni_purchase')
    return max(p, o)


def normalize_ad_name(name):
    """Remove prefix [ADx] / [AD] e normaliza pra agrupar variações do mesmo criativo."""
    if not name:
        return ''
    s = re.sub(r'^\s*\[ad\d*\]\s*', '', name, flags=re.IGNORECASE)
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s


def build_creative_key(ad_row, creative_details):
    """Chave de agrupamento de criativos:
    1. effective_object_story_id (ideal — é o post real)
    2. creative.id (se não tiver story_id)
    3. ad_name normalizado (último recurso)
    """
    sid = creative_details.get('effective_object_story_id') or ''
    if sid:
        return ('story', sid)
    cid = creative_details.get('creative_id') or ''
    if cid:
        return ('creative', cid)
    norm = normalize_ad_name(ad_row.get('ad_name', ''))
    return ('name', norm)


def build_library_url(creative):
    """Constrói URL pra Biblioteca de Anúncios do Meta (pública)."""
    page_id = creative.get('page_id') or ''
    name = creative.get('ad_name_display') or ''
    if page_id:
        # Filtro: todos os ads da página + termo de busca
        q = urllib.parse.quote_plus(normalize_ad_name(name)[:80] or name[:80])
        return ('https://www.facebook.com/ads/library/?active_status=all&ad_type=all'
                f'&country=BR&view_all_page_id={page_id}'
                f'&search_type=keyword_unordered&media_type=all'
                f'&q={q}')
    # Sem page_id, busca geral
    q = urllib.parse.quote_plus(name[:80])
    return ('https://www.facebook.com/ads/library/?active_status=all&ad_type=all'
            f'&country=BR&search_type=keyword_unordered&media_type=all'
            f'&q={q}')


def aggregate_by_creative(ads):
    """Agrupa lista de ads pela chave do criativo. Soma métricas, lista campanhas."""
    grouped = {}
    for ad in ads:
        key = ad['_creative_key']
        if key not in grouped:
            grouped[key] = {
                '_creative_key':              key,
                'account':                    ad['account'],
                'account_id':                 ad['account_id'],
                'ad_name_display':            ad['ad_name'],   # nome de exibição (primeiro encontrado)
                'creative_id':                ad.get('creative_id', ''),
                'effective_object_story_id':  ad.get('effective_object_story_id', ''),
                'page_id':                    ad.get('page_id', ''),
                'post_id':                    ad.get('post_id', ''),
                'thumbnail_url':              ad.get('thumbnail_url', ''),
                'image_url':                  ad.get('image_url', ''),
                'impressions':                0,
                'clicks':                     0,
                'spend':                      0.0,
                'purchases':                  0,
                'instances':                  [],  # cada adset/campanha onde rodou
            }
        g = grouped[key]
        g['impressions'] += ad['impressions']
        g['clicks']      += ad['clicks']
        g['spend']       += ad['spend']
        g['purchases']   += ad['purchases']
        # Prioriza thumb não-vazia
        if not g['thumbnail_url'] and ad.get('thumbnail_url'):
            g['thumbnail_url'] = ad['thumbnail_url']
        g['instances'].append({
            'ad_id':           ad['ad_id'],
            'ad_name':         ad['ad_name'],
            'adset_name':      ad['adset_name'],
            'campaign_name':   ad['campaign_name'],
            'campaign_id':     ad.get('campaign_id', ''),
            'impressions':     ad['impressions'],
            'clicks':          ad['clicks'],
            'spend':           ad['spend'],
            'purchases':       ad['purchases'],
        })

    # Derivados pos-agrupamento
    out = []
    for g in grouped.values():
        g['spend']        = round(g['spend'], 2)
        g['ctr']          = round((g['clicks'] / g['impressions'] * 100), 2) if g['impressions'] > 0 else 0
        g['cpp']          = round(g['spend'] / g['purchases'], 2) if g['purchases'] > 0 else None
        g['n_instances']  = len(g['instances'])
        g['library_url']  = build_library_url(g)
        out.append(g)
    return out


def build_ads(acct, account_label):
    rows = fetch_ads_insights(acct)
    out = []
    for r in rows:
        spend = float(r.get('spend', 0))
        pur   = int(parse_purchases(r.get('actions')))
        out.append({
            'account':       account_label,
            'account_id':    acct,
            'ad_id':         r.get('ad_id'),
            'ad_name':       r.get('ad_name', ''),
            'adset_name':    r.get('adset_name', ''),
            'campaign_name': r.get('campaign_name', ''),
            'campaign_id':   r.get('campaign_id', ''),
            'impressions':   int(r.get('impressions', 0)),
            'clicks':        int(r.get('clicks', 0)),
            'ctr':           round(float(r.get('ctr', 0) or 0), 2),
            'spend':         round(spend, 2),
            'purchases':     pur,
        })
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

    # Filtra ads que de fato tiveram >=1 compra (pra reduzir API calls)
    com_compra = [a for a in todos if a['purchases'] > 0]
    print(f'\nAds com pelo menos 1 compra: {len(com_compra)} (de {len(todos)})')

    # Busca creative_id + post_id de TODOS os ads que importam
    # (mesmo os sem compra, pra contabilizar instâncias completas do criativo)
    print(f'\nBuscando creative_id/post_id de {len(todos)} ads...')
    ad_id_to_creative = {}
    for ad in todos:
        det = fetch_creative_details(ad['ad_id'])
        ad_id_to_creative[ad['ad_id']] = det
        ad.update(det)
        ad['_creative_key'] = build_creative_key(ad, det)

    # Agrupa por creative_key
    creativos_principal  = aggregate_by_creative([a for a in todos if a['account_id'] == ACCT_PRINCIPAL])
    creativos_nova_motos = aggregate_by_creative([a for a in todos if a['account_id'] == ACCT_NOVA_MOTOS])
    creativos_todos      = aggregate_by_creative(todos)

    # Filtra criativos com compra
    com_compra_unif = [c for c in creativos_todos if c['purchases'] > 0]
    print(f'\nCriativos unicos (apos agrupar): {len(creativos_todos)} | com compra: {len(com_compra_unif)}')

    # TOP 10 GERAL (todos criativos, 2 contas, agrupados)
    top10 = sorted(com_compra_unif, key=lambda x: -x['purchases'])[:10]

    # REMARKETING (campanha RMKT da conta principal) — agrupado tb
    def has_remktg_instance(c):
        for inst in c['instances']:
            name = (inst.get('campaign_name') or '').upper()
            if 'RMKT' in name or 'REMARKETING' in name or 'REMKT' in name:
                return True
        return False
    remktg_creativos = [c for c in creativos_principal
                        if has_remktg_instance(c) and c['purchases'] > 0]
    # Pros remarketing, soma APENAS as instâncias RMKT (não outras campanhas onde o mesmo criativo rodou)
    def filter_to_remktg_only(c):
        rmktg_insts = [i for i in c['instances']
                       if 'RMKT' in (i.get('campaign_name') or '').upper()
                       or 'REMARKETING' in (i.get('campaign_name') or '').upper()
                       or 'REMKT' in (i.get('campaign_name') or '').upper()]
        if not rmktg_insts:
            return None
        new = dict(c)
        new['instances']   = rmktg_insts
        new['n_instances'] = len(rmktg_insts)
        new['impressions'] = sum(i['impressions'] for i in rmktg_insts)
        new['clicks']      = sum(i['clicks']      for i in rmktg_insts)
        new['spend']       = round(sum(i['spend'] for i in rmktg_insts), 2)
        new['purchases']   = sum(i['purchases']   for i in rmktg_insts)
        new['ctr']         = round((new['clicks'] / new['impressions'] * 100), 2) if new['impressions'] > 0 else 0
        new['cpp']         = round(new['spend'] / new['purchases'], 2) if new['purchases'] > 0 else None
        return new
    remktg_filtered = [filter_to_remktg_only(c) for c in remktg_creativos]
    remktg_filtered = [c for c in remktg_filtered if c and c['purchases'] > 0]
    remktg_sorted = sorted(remktg_filtered, key=lambda x: -x['purchases'])[:10]

    output = {
        'analise_em': UNTIL.isoformat(),
        'periodo': {
            'since': SINCE.isoformat(),
            'until': UNTIL.isoformat(),
            'dias':  LOOKBACK_DAYS,
        },
        'total_ads_analisados':       len(todos),
        'total_criativos_unificados': len(creativos_todos),
        'total_ads_com_compra':       len(com_compra),
        'total_criativos_com_compra': len(com_compra_unif),
        'top10_geral':            top10,
        'top_remarketing_principal': remktg_sorted,
    }

    with open('analise-meta-criativos.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print()
    print('=' * 80)
    print(f'TOP 10 CRIATIVOS GERAL (unificados) - Ultimos {LOOKBACK_DAYS} dias')
    print('=' * 80)
    for i, c in enumerate(top10, 1):
        cpp = f'R$ {c["cpp"]:,.2f}' if c['cpp'] is not None else '   ---'
        print(f'\n#{i}  [{c["account"]}]  {c["purchases"]} compras (em {c["n_instances"]} adsets)')
        print(f'    Ad: {c["ad_name_display"][:70]}')
        print(f'    Spend: R$ {c["spend"]:,.2f}  CPP: {cpp}  CTR: {c["ctr"]}%')

    print()
    print('=' * 80)
    print(f'TOP REMARKETING (unificados, somente instancias [RMKT])')
    print('=' * 80)
    for i, c in enumerate(remktg_sorted, 1):
        cpp = f'R$ {c["cpp"]:,.2f}' if c['cpp'] is not None else '   ---'
        print(f'\n#{i}  {c["purchases"]} compras (em {c["n_instances"]} adsets RMKT)')
        print(f'    Ad: {c["ad_name_display"][:70]}')
        print(f'    Spend: R$ {c["spend"]:,.2f}  CPP: {cpp}  CTR: {c["ctr"]}%')


if __name__ == '__main__':
    main()
