#!/usr/bin/env python3
"""
Test Ride — disponibilidade AO VIVO (Festival Interlagos 2026 / Agencia Lime).

Base  = snapshot da planilha de estoque exportada do sistema proprio (estoque-baseline.json).
Vivo  = base MENOS as vendas confirmadas (venda_status=3) que entraram DEPOIS do snapshot,
        puxadas da API do sistema proprio (/apis/token + /apis/vendas).
Saida = estoque-data.json (lido pelo dashboard).

Sem secret: /apis/token e publico. Roda via GitHub Actions a cada ~15 min.
Granularidade: por edicao -> passe -> marca -> dia (igual o dashboard mostra).
"""
import json, os, sys
from urllib.request import urlopen, Request
from datetime import datetime, timezone, timedelta

HERE      = os.path.dirname(os.path.abspath(__file__))
BASE_FILE = os.path.join(HERE, 'estoque-baseline.json')
OUT_FILE  = os.path.join(HERE, 'estoque-data.json')

BASES = {
    'moto': 'https://ingressosmoto.festivalinterlagos.com.br',
    'auto': 'https://ingressosauto.festivalinterlagos.com.br',
}
# Momento de cada exportacao: a base reflete as vendas ate aqui. So descontamos o que veio depois.
SNAPSHOT_TS = {
    'moto': '2026-06-24 10:22:34',
    'auto': '2026-06-24 10:43:43',
}
SNAPSHOT_DATE = '2026-06-24'   # data_inicio na busca de vendas
BRT = timezone(timedelta(hours=-3))


def _headers(base, token=None):
    h = {'Accept': 'application/json', 'Origin': base, 'Referer': base + '/',
         'X-Requested-With': 'XMLHttpRequest',
         'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}
    if token:
        h['Authorization'] = 'Bearer ' + token
    return h


def _get(url, base, token=None):
    with urlopen(Request(url, headers=_headers(base, token)), timeout=30) as r:
        return json.loads(r.read())


def as_list(x):
    return x if isinstance(x, list) else ([] if x is None else [x])


def norm(s):
    return (s or '').strip().upper()


def norm_pass(nome):
    n = (nome or '').strip()
    return 'Super Sport Pass' if n.lower().startswith('super sport pass') else n


def fetch_vendas(base, token):
    fim = datetime.now(BRT).strftime('%Y-%m-%d')
    out, page = [], 1
    while True:
        url = (f"{base}/apis/vendas?data_inicio={SNAPSHOT_DATE}&data_fim={fim}"
               f"&page_size=100&page={page}")
        d = _get(url, base, token)
        if d.get('status') != 'success' or not d.get('data'):
            break
        out.extend(d['data'])
        if d.get('pagination', {}).get('isNextPage') != 'Y':
            break
        page += 1
    return out


def vendas_pos_snapshot(ed_key):
    """{(passe, MARCA, 'DD'): qtd_vendida_depois_do_snapshot}."""
    base, ts = BASES[ed_key], SNAPSHOT_TS[ed_key]
    token = _get(base + '/apis/token', base)['token']
    sales = {}
    total_q = 0
    for v in fetch_vendas(base, token):
        if str(v.get('venda_status')) != '3':
            continue
        if (v.get('created_at') or '') <= ts:   # ja contabilizado na base
            continue
        for q in (v.get('qrcodes') or []):
            if q.get('ingresso_tipo') != 'testRide':
                continue
            ip = norm_pass(q.get('ingresso_nome'))
            for m in (q.get('modelos') or []):
                marca = norm(m.get('modelo_marca'))
                day = (m.get('data') or '')[:2]
                if ip and marca and day:
                    sales[(ip, marca, day)] = sales.get((ip, marca, day), 0) + 1
                    total_q += 1
    return sales, total_q


def main():
    with open(BASE_FILE, encoding='utf-8') as f:
        data = json.load(f)
    eds = data.get('edicoes') or {}
    resumo = []

    for ed_key in ('moto', 'auto'):
        ed = eds.get(ed_key)
        if not ed:
            continue
        try:
            sales, nq = vendas_pos_snapshot(ed_key)
        except Exception as e:
            print(f'[warn] {ed_key}: {e}', file=sys.stderr)
            continue

        day_keys = [str(d)[:2] for d in as_list(ed.get('dias'))]
        descontado = 0
        subtotal = 0
        for p in as_list(ed.get('passes')):
            pn = p.get('nome')
            ptot = 0
            for m in as_list(p.get('marcas')):
                marca = norm(m.get('marca'))
                novos = []
                for i, val in enumerate(as_list(m.get('dias'))):
                    dk = day_keys[i] if i < len(day_keys) else None
                    sub = sales.get((pn, marca, dk), 0) if dk else 0
                    descontado += min(sub, int(val or 0))
                    novos.append(max(0, int(val or 0) - sub))
                m['dias'] = novos
                m['total'] = sum(novos)
                ptot += m['total']
            p['total'] = ptot
            subtotal += ptot
        ed['total_disponivel'] = subtotal

        # alertas + marcas com vaga (recalculados sobre o disponivel atual)
        bt = {}
        for p in as_list(ed.get('passes')):
            for m in as_list(p.get('marcas')):
                bt[m.get('marca')] = bt.get(m.get('marca'), 0) + int(m.get('total') or 0)
        ed['marcas_com_vaga'] = sum(1 for v in bt.values() if v > 0)
        ed['quase_esgotando'] = [{'marca': k, 'total': v}
                                 for k, v in sorted(bt.items(), key=lambda kv: kv[1])[:4]]
        ed['mais_folga'] = [{'marca': k, 'total': v}
                            for k, v in sorted(bt.items(), key=lambda kv: -kv[1])[:4]]
        resumo.append(f"{ed_key}: {subtotal} disp ({descontado} descontados de {nq} vendas pos-snapshot)")

    data['updated_at'] = datetime.now(BRT).strftime('%Y-%m-%d %H:%M')
    data['source'] = 'Base da planilha (24/06) menos vendas ao vivo da API do sistema proprio'

    tmp = OUT_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, OUT_FILE)
    print('OK estoque-data.json | ' + ' | '.join(resumo))


if __name__ == '__main__':
    main()
