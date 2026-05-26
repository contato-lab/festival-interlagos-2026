#!/usr/bin/env python3
"""
Festival Interlagos 2026 - Audit do Pixel do Meta

Verifica se o Pixel esta capturando 100% das vendas que vem do Meta:
- Compara `purchase` (so Pixel web) com `omni_purchase` (Pixel + CAPI + offline)
- Detecta dias com discrepancia anormal entre eventos
- Compara com vendas reais (Sistema Proprio + Ticketmaster) por dia
- Identifica gaps onde o Pixel pode estar disparando errado

Output:
  audit-meta-pixel.json com diagnostico completo
  Print no stdout com resumo executivo
"""

import os, json, sys, urllib.request, urllib.parse
from datetime import date, datetime, timedelta

ACCT_PRINCIPAL  = os.environ.get('META_ACCT',       'act_2044706169171045')
ACCT_NOVA_MOTOS = os.environ.get('META_ACCT_MOTOS', 'act_1326431289216611')
TOKEN           = os.environ.get('META_TOKEN', '')
SINCE           = '2026-03-31'
UNTIL           = date.today().strftime('%Y-%m-%d')
API_VERSION     = 'v21.0'

# Tipos de evento de compra que existem na API Meta:
ACTION_TYPES_TO_AUDIT = [
    'purchase',                                  # Standard Pixel web
    'offsite_conversion.fb_pixel_purchase',      # Pixel offsite
    'omni_purchase',                             # Pixel + CAPI + offline (deduplicado)
    'onsite_conversion.purchase',                # Onsite (Instant Form, etc)
    'mobile_app_install',                        # Pra detectar se conta mobile tb
    'web_in_store_purchase',                     # Offline conversions
]

FIELDS = ','.join([
    'date_start',
    'spend',
    'impressions',
    'clicks',
    'actions',
    'action_values',
])


def fetch_insights(acct, since, until):
    """Pull daily insights da Meta API."""
    params = {
        'fields':         FIELDS,
        'time_range':     json.dumps({'since': since, 'until': until}),
        'time_increment': '1',
        'level':          'account',
        'access_token':   TOKEN,
        'limit':          '500',
    }
    url = f'https://graph.facebook.com/{API_VERSION}/{acct}/insights?{urllib.parse.urlencode(params)}'
    print(f'  Buscando insights de {acct} ({since} -> {until})...')
    with urllib.request.urlopen(url, timeout=60) as resp:
        data = json.loads(resp.read())
    return data.get('data', [])


def extract_action(row, action_type):
    """Soma o value de um action_type especifico no row."""
    for a in (row.get('actions') or []):
        if a.get('action_type') == action_type:
            return float(a.get('value', 0))
    return 0.0


def extract_action_value(row, action_type):
    """Soma o valor monetario (R$) de um action_type especifico no row."""
    for a in (row.get('action_values') or []):
        if a.get('action_type') == action_type:
            return float(a.get('value', 0))
    return 0.0


def audit_account(acct):
    """Analisa uma conta Meta dia a dia e detecta discrepancias."""
    rows = fetch_insights(acct, SINCE, UNTIL)
    out = []
    for r in rows:
        d = r['date_start']
        spend = float(r.get('spend', 0))
        purchase    = extract_action(r, 'purchase')
        omni        = extract_action(r, 'omni_purchase')
        pixel_off   = extract_action(r, 'offsite_conversion.fb_pixel_purchase')
        onsite      = extract_action(r, 'onsite_conversion.purchase')
        offline_in  = extract_action(r, 'web_in_store_purchase')

        purchase_val = extract_action_value(r, 'purchase')
        omni_val     = extract_action_value(r, 'omni_purchase')

        # Diferenca = potenciais eventos perdidos pelo Pixel
        # Se omni > purchase, sao eventos que vem de CAPI ou offline
        gap = omni - purchase

        out.append({
            'date':           d,
            'spend':          round(spend, 2),
            'purchase':       int(purchase),
            'pixel_offsite':  int(pixel_off),
            'onsite':         int(onsite),
            'offline':        int(offline_in),
            'omni_purchase':  int(omni),
            'gap_omni_minus_pixel': int(gap),
            'purchase_value': round(purchase_val, 2),
            'omni_value':     round(omni_val, 2),
        })
    return out


def load_real_sales():
    """Carrega vendas reais SP + TM dia a dia."""
    real = {}
    try:
        with open('vendas-data.json') as f:
            sp = json.load(f)
        for r in sp.get('daily', []):
            d = r['date']
            real[d] = real.get(d, 0) + int(r.get('moto_ingressos', 0)) + int(r.get('auto_ingressos', 0))
    except Exception as e:
        print(f'  WARN: vendas-data.json: {e}')
    try:
        with open('ticketmaster-data.json') as f:
            tm = json.load(f)
        for r in tm.get('daily', []):
            d = r['date']
            real[d] = real.get(d, 0) + int(r.get('moto_ingressos', 0)) + int(r.get('auto_ingressos', 0))
    except Exception as e:
        print(f'  WARN: ticketmaster-data.json: {e}')
    return real


def main():
    if not TOKEN:
        print('ERRO: META_TOKEN nao configurado')
        sys.exit(1)

    print(f'\n=== Audit Pixel Meta — Festival Interlagos 2026 ===')
    print(f'Periodo: {SINCE} a {UNTIL}\n')

    print('[1/3] Buscando dados conta PRINCIPAL...')
    principal = audit_account(ACCT_PRINCIPAL)

    print('[2/3] Buscando dados conta NOVA MOTOS...')
    motos = audit_account(ACCT_NOVA_MOTOS)

    print('[3/3] Carregando vendas reais...')
    real = load_real_sales()

    # Merge por data
    daily = {}
    for r in principal:
        d = r['date']
        daily[d] = {'date': d, 'principal': r, 'motos': None, 'real_sales': real.get(d, 0)}
    for r in motos:
        d = r['date']
        if d not in daily:
            daily[d] = {'date': d, 'principal': None, 'motos': r, 'real_sales': real.get(d, 0)}
        else:
            daily[d]['motos'] = r

    # Agregados
    total_purchase = 0
    total_omni     = 0
    total_gap      = 0
    total_real     = 0
    total_spend    = 0
    dias_gap_alto  = []
    dias_attr_zero = []

    for d in sorted(daily.keys()):
        day = daily[d]
        pp = day.get('principal') or {}
        mm = day.get('motos') or {}
        purchase_d = pp.get('purchase', 0) + mm.get('purchase', 0)
        omni_d     = pp.get('omni_purchase', 0) + mm.get('omni_purchase', 0)
        gap_d      = omni_d - purchase_d
        spend_d    = pp.get('spend', 0) + mm.get('spend', 0)
        real_d     = day['real_sales']

        total_purchase += purchase_d
        total_omni     += omni_d
        total_gap      += gap_d
        total_real     += real_d
        total_spend    += spend_d

        if gap_d >= 5:
            dias_gap_alto.append({'date': d, 'purchase': purchase_d, 'omni': omni_d, 'gap': gap_d})
        if spend_d >= 200 and omni_d == 0 and real_d > 0:
            dias_attr_zero.append({'date': d, 'spend': spend_d, 'real_sales': real_d})

    print('\n')
    print('═' * 70)
    print('RESUMO EXECUTIVO')
    print('═' * 70)
    print(f'Periodo analisado: {SINCE} a {UNTIL}')
    print(f'')
    print(f'Investimento total Meta:   R$ {total_spend:>11,.2f}')
    print(f'Conversoes via Pixel:      {total_purchase:>11,}    (purchase web)')
    print(f'Conversoes via Omni:       {total_omni:>11,}    (Pixel + CAPI + offline)')
    print(f'GAP (omni - pixel):        {total_gap:>11,}    (potenciais perdidos no Pixel)')
    if total_purchase > 0:
        gap_pct = (total_gap / total_purchase) * 100
        print(f'GAP %:                     {gap_pct:>10.1f}%    (vs Pixel)')
    print(f'')
    print(f'Vendas REAIS (SP + TM):    {total_real:>11,}')
    if total_real > 0:
        attr_omni  = (total_omni     / total_real) * 100
        attr_pixel = (total_purchase / total_real) * 100
        print(f'Atribuicao Omni:           {attr_omni:>10.1f}%    (meta omni / vendas reais)')
        print(f'Atribuicao Pixel:          {attr_pixel:>10.1f}%    (meta pixel / vendas reais)')

    print(f'')
    if dias_gap_alto:
        print(f'═══ DIAS COM GAP ALTO (omni >> pixel) ═══')
        print(f'(quando o numero de "omni_purchase" supera muito o "purchase" do Pixel, indica')
        print(f' que ha eventos vindo via CAPI/offline mas o Pixel nao registrou)')
        for d in dias_gap_alto[:15]:
            print(f"  {d['date']}: Pixel {d['purchase']:>4}  Omni {d['omni']:>4}  GAP +{d['gap']:>3}")
        if len(dias_gap_alto) > 15:
            print(f'  ... e mais {len(dias_gap_alto) - 15} dias')

    if dias_attr_zero:
        print(f'')
        print(f'═══ DIAS COM SPEND >= R$200 MAS ZERO CONVERSAO META ═══')
        print(f'(provavel problema de Pixel ou pagina nao disparando)')
        for d in dias_attr_zero[:10]:
            print(f"  {d['date']}: spend R$ {d['spend']:>8,.2f}   vendas reais: {d['real_sales']}")

    # Salva report completo
    out = {
        'updated_at': datetime.utcnow().isoformat() + 'Z',
        'period': {'since': SINCE, 'until': UNTIL},
        'summary': {
            'total_spend':    round(total_spend, 2),
            'total_purchase': total_purchase,
            'total_omni':     total_omni,
            'total_gap':      total_gap,
            'total_real_sales': total_real,
            'attr_pct_omni':    round((total_omni / total_real * 100) if total_real else 0, 1),
            'attr_pct_pixel':   round((total_purchase / total_real * 100) if total_real else 0, 1),
        },
        'dias_gap_alto': dias_gap_alto,
        'dias_attr_zero': dias_attr_zero,
        'daily_breakdown': [daily[d] for d in sorted(daily.keys())],
    }

    with open('audit-meta-pixel.json', 'w') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f'\nReport completo salvo em: audit-meta-pixel.json')


if __name__ == '__main__':
    main()
