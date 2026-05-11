#!/usr/bin/env python3
"""
update-vendas-unificado.py
Festival Interlagos 2026 — Agência Lime

Script ÚNICO que substitui os três anteriores:
  - update-vendas-proprias.py  (vendas-data.json)
  - update-ticketmaster.py     (ticketmaster-data.json)
  - update-vendas-tipos.py     (vendas-tipos-data.json)

Ele faz uma única leitura de cada API (Sistema Próprio + Ticketmaster) e
gera os três arquivos JSON a partir do MESMO snapshot. Isso elimina a
defasagem entre os pipelines, que antes geravam números divergentes na
seção "Tipos de Ingresso" do dashboard.

Roda via GitHub Actions a cada 2 minutos.
"""

import io
import json
import os
import re
import sys
import requests
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

# ─── CONFIGURAÇÃO ──────────────────────────────────────────
API_MOTO_BASE   = 'https://ingressosmoto.festivalinterlagos.com.br'
API_AUTO_BASE   = 'https://ingressosauto.festivalinterlagos.com.br'
DATA_INICIO     = date(2026, 3, 31)
DATA_INICIO_STR = '2026-01-01'
PAGE_SIZE       = 100

TM_API_BASE       = 'https://data.getcrowder.com'
TM_API_ENDPOINT   = '/activity/organizer'
TM_API_KEY        = os.environ.get('TM_API_KEY', '52087b883f65e8d2a684ee680c5beca66e425fdcc46c2b653da0fffd50088734')
CAMPAIGN_START_MS = 1774396800000  # 2026-03-25 00:00:00 UTC
MOTO_SHOW_IDS = {195330, 195736, 195737, 195738}
AUTO_SHOW_IDS = {195331, 195739, 195740, 195741}
SHOW_TO_DATE = {
    195330: '13/08/2026', 195736: '14/08/2026', 195737: '15/08/2026', 195738: '16/08/2026',
    195331: '27/08/2026', 195739: '28/08/2026', 195740: '29/08/2026', 195741: '30/08/2026',
}

OUT_VENDAS = 'vendas-data.json'
OUT_TM     = 'ticketmaster-data.json'
OUT_TIPOS  = 'vendas-tipos-data.json'

# ── PLANILHA (fallback quando a API TM cai) ────────────────
PLANILHA_ID  = os.environ.get('PLANILHA_LINHA_TEMPO_ID', '1jk7jjYB6n-IjkdmBUv54QEerujL_iAAP')
PLANILHA_URL = f'https://docs.google.com/spreadsheets/d/{PLANILHA_ID}/export?format=xlsx'
PLANILHA_YEAR = 2026


# ╔══════════════════════════════════════════════════════════╗
# ║  SISTEMA PRÓPRIO                                        ║
# ╚══════════════════════════════════════════════════════════╝

def _proprio_headers(base, token=None):
    h = {
        'Accept':           'application/json',
        'Content-Type':     'application/json',
        'Origin':           base,
        'Referer':          base + '/',
        'User-Agent':       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                            '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
    }
    if token:
        h['Authorization'] = f'Bearer {token}'
    return h


def fetch_proprio_sales(base):
    """Busca todas as vendas do Sistema Próprio. Uma única vez."""
    r = requests.get(base + '/apis/token', headers=_proprio_headers(base), timeout=30)
    r.raise_for_status()
    token = r.json()['token']

    fim = datetime.now().strftime('%Y-%m-%d')
    sales, page = [], 1
    while True:
        url = (f'{base}/apis/vendas?data_inicio={DATA_INICIO_STR}&data_fim={fim}'
               f'&page_size={PAGE_SIZE}&page={page}')
        r = requests.get(url, headers=_proprio_headers(base, token), timeout=30)
        r.raise_for_status()
        d = r.json()
        if d.get('status') != 'success' or not d.get('data'):
            break
        sales.extend(d['data'])
        if d.get('pagination', {}).get('isNextPage') != 'Y':
            break
        page += 1
    return sales


def aggregate_proprio_totais(sales):
    """Agrega para vendas-data.json (totais diários: receita + qtd ingressos)."""
    by_day = {}
    for v in sales:
        if str(v.get('venda_status')) != '3':
            continue
        ds = (v.get('created_at') or '').split(' ')[0]
        if not ds or len(ds) < 10:
            continue
        try:
            dt = date.fromisoformat(ds)
        except ValueError:
            continue
        d = (dt - DATA_INICIO).days + 1
        if d < 1:
            continue
        if d not in by_day:
            by_day[d] = {'receita': 0.0, 'qtd': 0, 'date': ds}
        by_day[d]['receita'] += float(v.get('venda_valor') or 0)
        by_day[d]['qtd']     += len(v.get('qrcodes') or [])
    return by_day


def aggregate_proprio_tipos(sales):
    """Agrega para vendas-tipos-data.json (breakdown por ingresso_nome)."""
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
        ds = (v.get('created_at') or '').split(' ')[0]
        if not ds or len(ds) < 10:
            continue
        for q in (v.get('qrcodes') or []):
            nome    = q.get('ingresso_nome', 'Desconhecido')
            valor   = float(q.get('ingresso_valor') or 0)
            data_ev = q.get('data', '') or 'Sem data'
            agg[nome]['qtd'] += 1
            agg[nome]['rec'] += valor
            agg[nome]['daily'][ds]['qtd'] += 1
            agg[nome]['daily'][ds]['rec'] += valor
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


# ╔══════════════════════════════════════════════════════════╗
# ║  TICKETMASTER                                            ║
# ╚══════════════════════════════════════════════════════════╝

def fetch_tm_movements():
    """Busca todos os movimentos do Ticketmaster. Uma única vez."""
    all_movs = []
    last_update, last_mov_id = CAMPAIGN_START_MS, 1
    while True:
        url = (f'{TM_API_BASE}{TM_API_ENDPOINT}'
               f'?lastUpdate={last_update}&lastMovementId={last_mov_id}')
        r = requests.get(url, headers={'apiKey': TM_API_KEY,
                                       'Content-Type': 'application/json'},
                         timeout=30)
        r.raise_for_status()
        d = r.json()
        movs = d.get('movements', [])
        all_movs.extend(movs)
        if not d.get('hasMore') or not movs:
            break
        last_update = d.get('lastUpdate', last_update)
        last_mov_id = d.get('lastMovementId', last_mov_id)
    return all_movs


def _classify_show(tickets):
    for t in tickets:
        sid = t.get('show', {}).get('id')
        if sid in MOTO_SHOW_IDS:
            return 'moto'
        if sid in AUTO_SHOW_IDS:
            return 'auto'
    return None


def _classify_meia_inteira(rate_name, rate_cat):
    cat  = (rate_cat or '').lower()
    nome = (rate_name or '').lower()
    if 'meia' in cat or 'meia' in nome or 'estatuto idoso' in nome:
        return 'meia'
    return 'inteira'


def aggregate_tm_totais(movements):
    """Agrega para ticketmaster-data.json (totais diários por edição)."""
    issuance_date = {}
    for m in movements:
        if m.get('operation') == 'ISSUANCE':
            pid = (m.get('purchase') or {}).get('id')
            if pid is not None and pid not in issuance_date:
                issuance_date[pid] = (m.get('date') or '')[:10]

    daily = defaultdict(lambda: {
        'moto_receita': 0.0, 'auto_receita': 0.0,
        'moto_ingressos': 0,  'auto_ingressos': 0,
    })
    totals = {
        'moto_receita': 0.0, 'auto_receita': 0.0,
        'moto_ingressos': 0,  'auto_ingressos': 0,
    }

    for m in movements:
        op   = m.get('operation')
        amt  = float(m.get('amount', 0))
        tc   = int(m.get('ticketCount', 0))
        edi  = _classify_show(m.get('tickets', []))
        if not edi:
            continue
        if op in ('CANCELLATION', 'REFUND'):
            pid = (m.get('purchase') or {}).get('id')
            ds  = issuance_date.get(pid) or (m.get('date') or '')[:10]
        else:
            ds = (m.get('date') or '')[:10]
        if not ds or len(ds) < 10:
            continue
        if edi == 'moto':
            daily[ds]['moto_receita']   += amt
            daily[ds]['moto_ingressos'] += tc
            totals['moto_receita']      += amt
            totals['moto_ingressos']    += tc
        else:
            daily[ds]['auto_receita']   += amt
            daily[ds]['auto_ingressos'] += tc
            totals['auto_receita']      += amt
            totals['auto_ingressos']    += tc

    totals['total_receita']   = round(totals['moto_receita'] + totals['auto_receita'], 2)
    totals['total_ingressos'] = totals['moto_ingressos'] + totals['auto_ingressos']
    totals['moto_receita']    = round(totals['moto_receita'], 2)
    totals['auto_receita']    = round(totals['auto_receita'], 2)

    daily_list = [{
        'date': ds,
        'moto_receita':   round(d['moto_receita'], 2),
        'auto_receita':   round(d['auto_receita'], 2),
        'moto_ingressos': d['moto_ingressos'],
        'auto_ingressos': d['auto_ingressos'],
    } for ds, d in sorted(daily.items())]

    return totals, daily_list


def aggregate_tm_tipos(movements):
    """Agrega para vendas-tipos-data.json (breakdown por product.name + meia/inteira)."""
    issuance_date = {}
    for m in movements:
        if m.get('operation') == 'ISSUANCE':
            pid = (m.get('purchase') or {}).get('id')
            if pid is not None and pid not in issuance_date:
                issuance_date[pid] = (m.get('date') or '')[:10]

    def make():
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
    moto = defaultdict(make)
    auto = defaultdict(make)

    for m in movements:
        edi = _classify_show(m.get('tickets', []))
        if not edi:
            continue
        prod = (m.get('product') or {}).get('name', '')
        if not prod:
            for t in m.get('tickets', []):
                prod = (t.get('sector') or {}).get('name', '')
                if prod:
                    break
        if not prod:
            prod = 'Desconhecido'

        op = m.get('operation')
        if op in ('CANCELLATION', 'REFUND'):
            pid = (m.get('purchase') or {}).get('id')
            ds  = issuance_date.get(pid) or (m.get('date') or '')[:10]
        else:
            ds = (m.get('date') or '')[:10]
        if not ds or len(ds) < 10:
            continue

        amt       = float(m.get('amount', 0))
        qtd       = int(m.get('ticketCount', 0))
        rate_name = (m.get('rate') or {}).get('name', '')
        rate_cat  = ((m.get('rate') or {}).get('category') or {}).get('name', '')
        tipo_mi   = _classify_meia_inteira(rate_name, rate_cat)

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
        bd = b['breakdown'][tipo_mi]
        bd['qtd'] += qtd
        bd['rec'] += amt
        bd['daily'][ds]['qtd'] += qtd
        bd['daily'][ds]['rec'] += amt
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


# ╔══════════════════════════════════════════════════════════╗
# ║  ESCRITA DOS 3 JSONS                                     ║
# ╚══════════════════════════════════════════════════════════╝

def build_vendas_data(moto_by_day, auto_by_day):
    all_days = sorted(set(list(moto_by_day.keys()) + list(auto_by_day.keys())))
    daily, totals = [], {
        'moto_receita': 0.0, 'auto_receita': 0.0,
        'moto_ingressos': 0,  'auto_ingressos': 0,
    }
    for d in all_days:
        m = moto_by_day.get(d, {'receita': 0.0, 'qtd': 0})
        a = auto_by_day.get(d, {'receita': 0.0, 'qtd': 0})
        day_date = (DATA_INICIO + timedelta(days=d - 1)).isoformat()
        daily.append({
            'd': d, 'date': day_date,
            'moto_receita':   round(m['receita'], 2),
            'moto_ingressos': m['qtd'],
            'auto_receita':   round(a['receita'], 2),
            'auto_ingressos': a['qtd'],
        })
        totals['moto_receita']   += m['receita']
        totals['moto_ingressos'] += m['qtd']
        totals['auto_receita']   += a['receita']
        totals['auto_ingressos'] += a['qtd']
    totals['moto_receita']    = round(totals['moto_receita'], 2)
    totals['auto_receita']    = round(totals['auto_receita'], 2)
    totals['total_receita']   = round(totals['moto_receita'] + totals['auto_receita'], 2)
    totals['total_ingressos'] = totals['moto_ingressos'] + totals['auto_ingressos']

    return {
        'updated_at':     datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source':         'Plataforma própria (ingressosmoto/ingressosauto.festivalinterlagos.com.br)',
        'campaign_start': DATA_INICIO.isoformat(),
        'totals':         totals,
        'daily':          daily,
    }


def build_ticketmaster_data(totals, daily, source='api'):
    source_label = {
        'api':      'Ticketmaster API via getcrowder.com',
        'planilha': 'Planilha Linha do Tempo (fallback enquanto API TM está fora)',
    }.get(source, 'Ticketmaster API via getcrowder.com')
    return {
        'updated_at':     datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source':         source_label,
        'source_type':    source,
        'campaign_start': '2026-03-25',
        'moto_show_ids':  sorted(MOTO_SHOW_IDS),
        'auto_show_ids':  sorted(AUTO_SHOW_IDS),
        'totals':         totals,
        'daily':          daily,
    }


def build_tipos_data(proprio_moto, proprio_auto, tm_moto, tm_auto):
    return {
        'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source':     'Sistema Proprio + Ticketmaster API (snapshot unificado)',
        'proprio': {'moto': proprio_moto, 'auto': proprio_auto},
        'ticketmaster': {'moto': tm_moto, 'auto': tm_auto},
    }


def write_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ╔══════════════════════════════════════════════════════════╗
# ║  PLANILHA (fallback quando API TM cai)                  ║
# ╚══════════════════════════════════════════════════════════╝

def _money_to_float(v):
    """Converte 'R$ 1.234,56' ou número direto para float. Vazio -> 0."""
    if v is None: return 0.0
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip()
    if not s or s in ('-', 'R$', 'R$ 0,00', 'R$ 0'): return 0.0
    s = s.replace('R$', '').replace(' ', '').strip()
    # formato BR: 1.234,56 -> 1234.56
    if ',' in s and s.rfind(',') > s.rfind('.'):
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_qtd(v):
    if v is None: return 0
    if isinstance(v, (int, float)): return int(v)
    s = str(v).strip().replace('.', '').replace(',', '')
    if not s or s == '-': return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def _parse_week_header(header_text):
    """Recebe 'DATA: 30 A 05/04' ou 'DATA: 27/04 A 03/05'. Retorna lista
    de 7 datas ISO da semana (segunda a domingo) para o ano da campanha."""
    if not header_text:
        return None
    m = re.search(r'(\d{1,2})(?:/(\d{1,2}))?\s*[Aa]\s*(\d{1,2})/(\d{1,2})', header_text)
    if not m:
        return None
    end_d = int(m.group(3)); end_m = int(m.group(4))
    end_date = date(PLANILHA_YEAR, end_m, end_d)
    return [(end_date - timedelta(days=6 - i)).isoformat() for i in range(7)]


def fetch_planilha_data():
    """Baixa o XLSX e parseia as semanas. Devolve {tm: {moto, auto},
    proprio: {moto, auto}} no formato {date_iso: {qtd, rec}}. Lança
    exceção se download/parse falhar — caller decide o fallback."""
    from openpyxl import load_workbook
    r = requests.get(PLANILHA_URL, timeout=30)
    r.raise_for_status()
    wb = load_workbook(io.BytesIO(r.content), data_only=True, read_only=True)

    out = {
        'tm':      {'moto': {}, 'auto': {}},
        'proprio': {'moto': {}, 'auto': {}},
    }

    for sheet_name in wb.sheetnames:
        if not sheet_name.upper().startswith('SEMANA'):
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue

        # Cabeçalho com as datas (linha 0)
        header = rows[0]
        header_cells = [str(c) if c is not None else '' for c in header]
        header_text = ' '.join(header_cells)
        week_dates = _parse_week_header(header_text)
        if not week_dates:
            print(f'  [planilha] {sheet_name}: cabeçalho não reconhecido: "{header_text[:60]}"', file=sys.stderr)
            continue

        # Encontra cada bloco e suas linhas de VENDAS VALOR / QTD INGRESSOS
        # Estrutura: VENDAS MOTOS -> SISTEMA PRÓPRIO -> VENDAS VALOR -> QTD -> TICKET MASTER -> VENDAS VALOR -> QTD
        # Depois: VENDAS AUTOS -> mesmo pattern
        section = None       # 'moto' | 'auto' | None
        canal = None         # 'proprio' | 'tm' | None
        for row in rows[1:]:
            cells = [str(c).strip() if c is not None else '' for c in row]
            label = cells[0].upper() if cells else ''
            if not label:
                continue
            if 'VENDAS MOTOS' in label:
                section, canal = 'moto', None
                continue
            if 'VENDAS AUTOS' in label or 'VENDAS CARROS' in label:
                section, canal = 'auto', None
                continue
            if 'SISTEMA PR' in label:  # PRÓPRIO/PROPRIO
                canal = 'proprio'; continue
            if 'TICKET MASTER' in label or 'TICKETMASTER' in label:
                canal = 'tm'; continue
            # Para evitar capturar TOTAL VENDAS VALOR / TKT MÉDIO / BALANÇO / etc:
            if label.startswith('TOTAL') or 'TKT' in label or 'BALAN' in label or 'INVEST' in label:
                canal = None; continue

            if section and canal in ('proprio', 'tm'):
                if label.startswith('VENDAS VALOR'):
                    for i, day_iso in enumerate(week_dates):
                        v = _money_to_float(row[i + 1]) if i + 1 < len(row) else 0.0
                        if v == 0 and day_iso not in out[canal][section]:
                            continue
                        out[canal][section].setdefault(day_iso, {'qtd': 0, 'rec': 0.0})
                        out[canal][section][day_iso]['rec'] = v
                elif 'QTD INGRESSOS' in label:
                    for i, day_iso in enumerate(week_dates):
                        q = _parse_qtd(row[i + 1]) if i + 1 < len(row) else 0
                        if q == 0 and day_iso not in out[canal][section]:
                            continue
                        out[canal][section].setdefault(day_iso, {'qtd': 0, 'rec': 0.0})
                        out[canal][section][day_iso]['qtd'] = q
    return out


def build_tm_data_from_planilha(planilha_tm):
    """Constrói ticketmaster-data.json a partir do dict planilha_tm."""
    moto_days = planilha_tm.get('moto', {})
    auto_days = planilha_tm.get('auto', {})
    all_dates = sorted(set(list(moto_days.keys()) + list(auto_days.keys())))

    daily = []
    totals = {'moto_receita': 0.0, 'auto_receita': 0.0, 'moto_ingressos': 0, 'auto_ingressos': 0}
    for ds in all_dates:
        m = moto_days.get(ds, {'qtd': 0, 'rec': 0.0})
        a = auto_days.get(ds, {'qtd': 0, 'rec': 0.0})
        daily.append({
            'date': ds,
            'moto_receita':   round(m['rec'], 2),
            'auto_receita':   round(a['rec'], 2),
            'moto_ingressos': m['qtd'],
            'auto_ingressos': a['qtd'],
        })
        totals['moto_receita']   += m['rec']
        totals['moto_ingressos'] += m['qtd']
        totals['auto_receita']   += a['rec']
        totals['auto_ingressos'] += a['qtd']

    totals['moto_receita']    = round(totals['moto_receita'], 2)
    totals['auto_receita']    = round(totals['auto_receita'], 2)
    totals['total_receita']   = round(totals['moto_receita'] + totals['auto_receita'], 2)
    totals['total_ingressos'] = totals['moto_ingressos'] + totals['auto_ingressos']

    return totals, daily


def build_tm_tipos_from_planilha(planilha_tm):
    """A planilha não tem breakdown por tipo de ingresso (Street Pass, Pista
    Premium, etc.), só Moto vs Auto. Então criamos um único bucket
    'Ticketmaster (planilha)' por canal pra alimentar a seção Tipos.
    Quando a API voltar, granularidade volta automaticamente."""
    def to_bucket(days):
        bucket = {
            'qtd': sum(d['qtd'] for d in days.values()),
            'rec': round(sum(d['rec'] for d in days.values()), 2),
            'daily': {ds: {'qtd': d['qtd'], 'rec': round(d['rec'], 2)}
                      for ds, d in days.items()},
            'por_dia_evento': {},
            'breakdown': {
                'meia':    {'qtd': 0, 'rec': 0.0, 'daily': {}},
                'inteira': {'qtd': 0, 'rec': 0.0, 'daily': {}},
            },
        }
        return bucket

    moto = {'Ticketmaster (planilha)': to_bucket(planilha_tm.get('moto', {}))} if planilha_tm.get('moto') else {}
    auto = {'Ticketmaster (planilha)': to_bucket(planilha_tm.get('auto', {}))} if planilha_tm.get('auto') else {}
    return moto, auto


# ╔══════════════════════════════════════════════════════════╗
# ║  MAIN                                                    ║
# ╚══════════════════════════════════════════════════════════╝

def _load_existing(path):
    """Carrega JSON existente, devolve None se não existir ou inválido."""
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def main():
    print('=== update-vendas-unificado.py ===')
    print(f'Início: {datetime.now().isoformat()}')

    # ── Sistema Próprio ─ uma única busca, dois agregados ───
    print('\n[1/4] Buscando vendas Próprio MOTO...')
    moto_sales = fetch_proprio_sales(API_MOTO_BASE)
    print(f'  {len(moto_sales)} vendas')

    print('[2/4] Buscando vendas Próprio AUTO...')
    auto_sales = fetch_proprio_sales(API_AUTO_BASE)
    print(f'  {len(auto_sales)} vendas')

    moto_by_day  = aggregate_proprio_totais(moto_sales)
    auto_by_day  = aggregate_proprio_totais(auto_sales)
    proprio_moto = aggregate_proprio_tipos(moto_sales)
    proprio_auto = aggregate_proprio_tipos(auto_sales)

    # ── Ticketmaster ─ tenta API; se falhar, tenta planilha; senão, mantém antigo ──
    print('\n[3/4] Buscando movimentos Ticketmaster (API)...')
    tm_movs = None
    tm_error = None
    tm_source = None
    try:
        tm_movs = fetch_tm_movements()
        print(f'  {len(tm_movs)} movimentos')
        tm_source = 'api'
    except Exception as e:
        tm_error = str(e)
        print(f'  ⚠️  TM API falhou: {tm_error}', file=sys.stderr)

    if tm_movs is not None:
        tm_totals, tm_daily = aggregate_tm_totais(tm_movs)
        tm_moto, tm_auto    = aggregate_tm_tipos(tm_movs)
    else:
        # Carrega dados anteriores como baseline (último snapshot bom da API)
        prev_tm    = _load_existing(OUT_TM) or {}
        prev_tipos = _load_existing(OUT_TIPOS) or {}
        prev_daily = {d['date']: d for d in (prev_tm.get('daily') or [])}
        prev_moto  = ((prev_tipos.get('ticketmaster') or {}).get('moto') or {})
        prev_auto  = ((prev_tipos.get('ticketmaster') or {}).get('auto') or {})

        # Fallback 1: planilha. Faz MERGE com o baseline (planilha sobrescreve
        # dias que tem; baseline preserva dias que planilha não cobre — ex.:
        # dias mais recentes que a planilha ainda não foi atualizada manualmente).
        print('  → tentando planilha como fallback...', file=sys.stderr)
        planilha_ok = False
        try:
            planilha = fetch_planilha_data()
            pl_totals, pl_daily = build_tm_data_from_planilha(planilha['tm'])
            # MERGE: parte do prev_daily, sobrescreve dias presentes na planilha
            merged = dict(prev_daily)
            for d in pl_daily:
                merged[d['date']] = d
            tm_daily = sorted(merged.values(), key=lambda x: x['date'])

            # Recalcula totals do merged
            tm_totals = {'moto_receita': 0.0, 'auto_receita': 0.0, 'moto_ingressos': 0, 'auto_ingressos': 0}
            for d in tm_daily:
                tm_totals['moto_receita']   += d.get('moto_receita', 0)
                tm_totals['moto_ingressos'] += d.get('moto_ingressos', 0)
                tm_totals['auto_receita']   += d.get('auto_receita', 0)
                tm_totals['auto_ingressos'] += d.get('auto_ingressos', 0)
            tm_totals['moto_receita']    = round(tm_totals['moto_receita'], 2)
            tm_totals['auto_receita']    = round(tm_totals['auto_receita'], 2)
            tm_totals['total_receita']   = round(tm_totals['moto_receita'] + tm_totals['auto_receita'], 2)
            tm_totals['total_ingressos'] = tm_totals['moto_ingressos'] + tm_totals['auto_ingressos']

            # vendas-tipos: NÃO sobrescreve o breakdown por tipo da API anterior.
            # O front-end já tem reconciliação 'Outros · em sincronização' que
            # absorve a diferença quando o total bate mais alto que a soma das
            # barras. Granularidade volta quando API voltar.
            tm_moto, tm_auto = prev_moto, prev_auto
            print(f'  ✓ Planilha lida: {len(pl_daily)} dias na planilha, '
                  f'merge resultou em {len(tm_daily)} dias totais '
                  f'({tm_totals["moto_ingressos"]} Moto + {tm_totals["auto_ingressos"]} Auto)',
                  file=sys.stderr)
            tm_source = 'planilha'
            planilha_ok = True
        except Exception as pe:
            print(f'  ⚠️  Planilha também falhou: {pe}', file=sys.stderr)

        if not planilha_ok:
            # Fallback 2: só dados anteriores
            tm_totals = prev_tm.get('totals')
            tm_daily  = prev_tm.get('daily', [])
            tm_moto   = prev_moto
            tm_auto   = prev_auto
            tm_source = 'stale'

    tm_ok = tm_source in ('api', 'planilha')

    # ── Escrita dos JSONs ──
    print('\n[4/4] Escrevendo JSONs...')
    write_json(OUT_VENDAS, build_vendas_data(moto_by_day, auto_by_day))
    if tm_ok:
        write_json(OUT_TM, build_ticketmaster_data(tm_totals, tm_daily, source=tm_source))
    else:
        # NÃO sobrescreve OUT_TM: timestamp ficaria mentiroso. Deixa o arquivo
        # antigo no lugar pra dashboard mostrar 'stale' via updated_at antigo.
        print(f'  (mantendo {OUT_TM} antigo intacto — API e planilha falharam)')
    write_json(OUT_TIPOS, build_tipos_data(proprio_moto, proprio_auto, tm_moto, tm_auto))

    print(f'\nOK Sistema Próprio atualizado em {OUT_VENDAS} e {OUT_TIPOS} (parte proprio)')
    print(f'Ticketmaster: source={tm_source}')
    if tm_source == 'stale':
        print(f'  Causa API: {tm_error}')
    print(f'Fim: {datetime.now().isoformat()}')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'\nERRO: {e}', file=sys.stderr)
        sys.exit(1)
