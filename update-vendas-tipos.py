#!/usr/bin/env python3
"""
update-vendas-tipos.py
Festival Interlagos 2026 — Agência Lime

Agrega vendas por TIPO DE INGRESSO cruzando duas fontes:
1. Sistema Próprio (ingressosmoto + ingressosauto): conta por qrcodes[].ingresso_nome
2. Ticketmaster (data.getcrowder.com): agrupa por product.name

Roda via GitHub Actions a cada 5 minutos. Gera vendas-tipos-data.json.
"""
import json, os, sys, requests
from datetime import datetime, timezone
from collections import defaultdict

# ── PRÓPRIO ────────────────────────────────────────────────
def fetch_proprio_sales(base):
    headers = {'Accept':'application/json','Content-Type':'application/json',
               'Origin':base,'Referer':base+'/',
               'User-Agent':'Mozilla/5.0','X-Requested-With':'XMLHttpRequest'}
    r = requests.get(base+'/apis/token', headers=headers, timeout=30)
    r.raise_for_status()
    headers['Authorization'] = f'Bearer {r.json()["token"]}'
    sales, page = [], 1
    while True:
        url = f'{base}/apis/vendas?data_inicio=2026-01-01&data_fim=2026-12-31&page_size=100&page={page}'
        r = requests.get(url, headers=headers, timeout=30)
        d = r.json()
        if d.get('status') != 'success' or not d.get('data'): break
        sales.extend(d['data'])
        if d.get('pagination', {}).get('isNextPage') != 'Y': break
        page += 1
    return sales

def aggregate_proprio(sales):
    def make():
        return {
            'qtd': 0, 'rec': 0.0,
            'daily': defaultdict(lambda: {'qtd': 0, 'rec': 0.0}),
            'por_dia_evento': defaultdict(lambda: {
                'qtd': 0, 'rec': 0.0,
                'daily': defaultdict(lambda: {'qtd': 0, 'rec': 0.0})
            }),
        }
    agg = defaultdict(make)
    for v in sales:
        if str(v.get('venda_status')) != '3':
            continue
        # Data da venda
        ds = (v.get('created_at') or '').split(' ')[0]
        if not ds or len(ds) < 10:
            continue
        for q in (v.get('qrcodes') or []):
            nome = q.get('ingresso_nome', 'Desconhecido')
            valor = float(q.get('ingresso_valor') or 0)
            # Data do evento (campo "data" no qrcode, formato DD/MM/YYYY)
            data_ev = q.get('data', '') or 'Sem data'
            agg[nome]['qtd'] += 1
            agg[nome]['rec'] += valor
            agg[nome]['daily'][ds]['qtd'] += 1
            agg[nome]['daily'][ds]['rec'] += valor
            # Por dia do evento + dia da venda
            de = agg[nome]['por_dia_evento'][data_ev]
            de['qtd'] += 1
            de['rec'] += valor
            de['daily'][ds]['qtd'] += 1
            de['daily'][ds]['rec'] += valor
    out = {}
    for nome, d in agg.items():
        out[nome] = {
            'qtd': d['qtd'],
            'rec': round(d['rec'], 2),
            'daily': {ds: {'qtd': dd['qtd'], 'rec': round(dd['rec'], 2)}
                      for ds, dd in d['daily'].items()},
            'por_dia_evento': {de: {
                'qtd': dde['qtd'],
                'rec': round(dde['rec'], 2),
                'daily': {ds: {'qtd': sd['qtd'], 'rec': round(sd['rec'], 2)}
                          for ds, sd in dde['daily'].items()}
            } for de, dde in d['por_dia_evento'].items()},
        }
    return out


# ── TICKETMASTER ───────────────────────────────────────────
TM_API_KEY = os.environ.get('TM_API_KEY', '52087b883f65e8d2a684ee680c5beca66e425fdcc46c2b653da0fffd50088734')
CAMPAIGN_START_MS = 1774396800000  # 2026-03-25
MOTO_SHOW_IDS = {195330, 195736, 195737, 195738}
AUTO_SHOW_IDS = {195331, 195739, 195740, 195741}

def fetch_tm_movements():
    all_movs = []
    last_update, last_mov_id = CAMPAIGN_START_MS, 1
    while True:
        url = f'https://data.getcrowder.com/activity/organizer?lastUpdate={last_update}&lastMovementId={last_mov_id}'
        r = requests.get(url, headers={'apiKey': TM_API_KEY}, timeout=30)
        r.raise_for_status()
        d = r.json()
        movs = d.get('movements', [])
        all_movs.extend(movs)
        if not d.get('hasMore') or not movs: break
        last_update = d.get('lastUpdate', last_update)
        last_mov_id = d.get('lastMovementId', last_mov_id)
    return all_movs

def classify_meia_inteira(rate_name, rate_cat):
    """Categoria 'Meia-Entrada' explicita = meia. Resto (incluindo cotacoes e descontos
    promocionais) = inteira."""
    cat = (rate_cat or '').lower()
    nome = (rate_name or '').lower()
    if 'meia' in cat or 'meia' in nome or 'estatuto idoso' in nome:
        return 'meia'
    return 'inteira'


def aggregate_tm(movements):
    # Mapa show.id -> data do evento (DD/MM/YYYY)
    SHOW_TO_DATE = {
        195330: '13/08/2026', 195736: '14/08/2026', 195737: '15/08/2026', 195738: '16/08/2026',
        195331: '27/08/2026', 195739: '28/08/2026', 195740: '29/08/2026', 195741: '30/08/2026',
    }

    issuance_date = {}
    for m in movements:
        if m.get('operation') == 'ISSUANCE':
            pid = (m.get('purchase') or {}).get('id')
            if pid is not None and pid not in issuance_date:
                issuance_date[pid] = (m.get('date') or '')[:10]

    def make_bucket():
        return {
            'qtd': 0, 'rec': 0.0,
            'daily': defaultdict(lambda: {'qtd': 0, 'rec': 0.0}),
            'por_dia_evento': defaultdict(lambda: {
                'qtd': 0, 'rec': 0.0,
                'daily': defaultdict(lambda: {'qtd': 0, 'rec': 0.0})
            }),
            'breakdown': {
                'meia':    {'qtd': 0, 'rec': 0.0, 'daily': defaultdict(lambda: {'qtd': 0, 'rec': 0.0})},
                'inteira': {'qtd': 0, 'rec': 0.0, 'daily': defaultdict(lambda: {'qtd': 0, 'rec': 0.0})},
            },
        }
    moto = defaultdict(make_bucket)
    auto = defaultdict(make_bucket)

    for m in movements:
        edi = None
        for t in m.get('tickets', []):
            sid = t.get('show', {}).get('id')
            if sid in MOTO_SHOW_IDS: edi = 'moto'; break
            if sid in AUTO_SHOW_IDS: edi = 'auto'; break
        if not edi: continue

        prod = (m.get('product') or {}).get('name', '')
        if not prod:
            for t in m.get('tickets', []):
                prod = (t.get('sector') or {}).get('name', '')
                if prod: break
        if not prod: prod = 'Desconhecido'

        op = m.get('operation')
        if op in ('CANCELLATION', 'REFUND'):
            pid = (m.get('purchase') or {}).get('id')
            ds = issuance_date.get(pid) or (m.get('date') or '')[:10]
        else:
            ds = (m.get('date') or '')[:10]
        if not ds or len(ds) < 10:
            continue

        amt = float(m.get('amount', 0))
        qtd = int(m.get('ticketCount', 0))
        rate_name = (m.get('rate') or {}).get('name', '')
        rate_cat  = ((m.get('rate') or {}).get('category') or {}).get('name', '')
        tipo_mi = classify_meia_inteira(rate_name, rate_cat)

        # Data do evento via show.id (cada movimento tem 1 show porque cada ingresso e pra 1 dia)
        data_ev = 'Sem data'
        for t in m.get('tickets', []):
            sid = t.get('show', {}).get('id')
            if sid in SHOW_TO_DATE:
                data_ev = SHOW_TO_DATE[sid]
                break

        bucket = moto if edi == 'moto' else auto
        b = bucket[prod]
        b['qtd'] += qtd
        b['rec'] += amt
        b['daily'][ds]['qtd'] += qtd
        b['daily'][ds]['rec'] += amt
        # Breakdown meia/inteira
        bd = b['breakdown'][tipo_mi]
        bd['qtd'] += qtd
        bd['rec'] += amt
        bd['daily'][ds]['qtd'] += qtd
        bd['daily'][ds]['rec'] += amt
        # Por dia do evento
        de = b['por_dia_evento'][data_ev]
        de['qtd'] += qtd
        de['rec'] += amt
        de['daily'][ds]['qtd'] += qtd
        de['daily'][ds]['rec'] += amt

    def to_dict(agg):
        out = {}
        for nome, d in agg.items():
            out[nome] = {
                'qtd': d['qtd'],
                'rec': round(d['rec'], 2),
                'daily': {ds: {'qtd': dd['qtd'], 'rec': round(dd['rec'], 2)}
                          for ds, dd in d['daily'].items()},
                'por_dia_evento': {de: {
                    'qtd': dde['qtd'],
                    'rec': round(dde['rec'], 2),
                    'daily': {ds: {'qtd': sd['qtd'], 'rec': round(sd['rec'], 2)}
                              for ds, sd in dde['daily'].items()}
                } for de, dde in d['por_dia_evento'].items()},
                'breakdown': {
                    mi: {
                        'qtd': bd['qtd'],
                        'rec': round(bd['rec'], 2),
                        'daily': {ds: {'qtd': dd['qtd'], 'rec': round(dd['rec'], 2)}
                                  for ds, dd in bd['daily'].items()},
                    } for mi, bd in d['breakdown'].items()
                },
            }
        return out
    return to_dict(moto), to_dict(auto)


# ── MAIN ───────────────────────────────────────────────────
def main():
    print('[1/3] Buscando vendas Próprio MOTO...')
    moto_sales = fetch_proprio_sales('https://ingressosmoto.festivalinterlagos.com.br')
    print(f'  {len(moto_sales)} vendas')
    print('[2/3] Buscando vendas Próprio AUTO...')
    auto_sales = fetch_proprio_sales('https://ingressosauto.festivalinterlagos.com.br')
    print(f'  {len(auto_sales)} vendas')

    proprio_moto = aggregate_proprio(moto_sales)
    proprio_auto = aggregate_proprio(auto_sales)

    print('[3/3] Buscando movimentos TM...')
    tm_movs = fetch_tm_movements()
    print(f'  {len(tm_movs)} movs')
    tm_moto, tm_auto = aggregate_tm(tm_movs)

    # Os agregados ja vem com 'daily' incluso pelo aggregate_*
    output = {
        'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source': 'Sistema Proprio + Ticketmaster API',
        'proprio': {
            'moto': proprio_moto,
            'auto': proprio_auto,
        },
        'ticketmaster': {
            'moto': tm_moto,
            'auto': tm_auto,
        },
    }

    with open('vendas-tipos-data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print('\nOK vendas-tipos-data.json gerado!')
    total_moto_qtd = sum(d['qtd'] for d in {**proprio_moto, **tm_moto}.values())
    total_auto_qtd = sum(d['qtd'] for d in {**proprio_auto, **tm_auto}.values())
    print(f'  Total Moto: {total_moto_qtd} ingressos')
    print(f'  Total Auto: {total_auto_qtd} ingressos')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'ERRO: {e}', file=sys.stderr)
        sys.exit(1)
