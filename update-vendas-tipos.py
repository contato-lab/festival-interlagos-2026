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
    agg = defaultdict(lambda: {'qtd': 0, 'rec': 0.0})
    for v in sales:
        if str(v.get('venda_status')) != '3':
            continue
        for q in (v.get('qrcodes') or []):
            nome = q.get('ingresso_nome', 'Desconhecido')
            valor = float(q.get('ingresso_valor') or 0)
            agg[nome]['qtd'] += 1
            agg[nome]['rec'] += valor
    return dict(agg)


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

def aggregate_tm(movements):
    moto = defaultdict(lambda: {'qtd': 0, 'rec': 0.0})
    auto = defaultdict(lambda: {'qtd': 0, 'rec': 0.0})
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

        amt = float(m.get('amount', 0))
        qtd = int(m.get('ticketCount', 0))
        bucket = moto if edi == 'moto' else auto
        bucket[prod]['qtd'] += qtd
        bucket[prod]['rec'] += amt

    return dict(moto), dict(auto)


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

    output = {
        'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source': 'Sistema Proprio + Ticketmaster API',
        'proprio': {
            'moto': {nome: {'qtd': d['qtd'], 'rec': round(d['rec'], 2)}
                     for nome, d in proprio_moto.items()},
            'auto': {nome: {'qtd': d['qtd'], 'rec': round(d['rec'], 2)}
                     for nome, d in proprio_auto.items()},
        },
        'ticketmaster': {
            'moto': {nome: {'qtd': d['qtd'], 'rec': round(d['rec'], 2)}
                     for nome, d in tm_moto.items()},
            'auto': {nome: {'qtd': d['qtd'], 'rec': round(d['rec'], 2)}
                     for nome, d in tm_auto.items()},
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
