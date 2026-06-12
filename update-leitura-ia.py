#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Leitura do dia com IA (Claude) para os dashboards do Festival Interlagos.
Le os dados ja coletados pelas outras Actions, monta um resumo de FATOS reais e
pede ao Claude (Haiku) uma leitura curta para cada dashboard. Grava leitura-ia.json.
A chave NUNCA vai pro navegador: o dashboard so le o JSON pronto.
So roda se ANTHROPIC_API_KEY existir.
"""
import json, os, re, sys, urllib.request
from datetime import datetime, timezone, date

OUT = 'leitura-ia.json'
HOJE = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
CAMP_INI = date(2026, 3, 31)
EV_MOTO = date(2026, 8, 13)
EV_AUTO = date(2026, 8, 27)
TODAY = datetime.now(timezone.utc).date()


def load(f):
    try:
        with open(f, encoding='utf-8') as fh:
            return json.load(fh)
    except Exception:
        return None


def sum_daily(daily, dset, fields):
    tot = 0.0
    for r in daily or []:
        if r.get('date') in dset:
            for fld in fields:
                tot += r.get(fld, 0) or 0
    return tot


def monta_fatos():
    vd = load('vendas-data.json') or {}
    tm = load('ticketmaster-data.json') or {}
    meta = load('meta-data.json') or {}
    gads = load('google-ads-data.json') or {}
    tt = load('tiktok-data.json') or {}
    ga = load('ga4-data.json') or {}

    rec = (vd.get('totals', {}).get('total_receita', 0)) + (tm.get('totals', {}).get('total_receita', 0))
    ing = (vd.get('totals', {}).get('total_ingressos', 0)) + (tm.get('totals', {}).get('total_ingressos', 0))
    tkt = rec / ing if ing else 0

    # dia mais recente com dados
    datas = sorted(set([r['date'] for r in vd.get('daily', []) if r.get('date')] +
                       [r['date'] for r in tm.get('daily', []) if r.get('date')]))
    hoje = datas[-1] if datas else None
    d7 = set(datas[-7:]) if datas else set()
    d7prev = set(datas[-14:-7]) if len(datas) >= 8 else set()

    rec_hoje = (sum_daily(vd.get('daily'), {hoje}, ['moto_receita', 'auto_receita']) +
                sum_daily(tm.get('daily'), {hoje}, ['moto_receita', 'auto_receita'])) if hoje else 0
    ing_hoje = (sum_daily(vd.get('daily'), {hoje}, ['moto_ingressos', 'auto_ingressos']) +
                sum_daily(tm.get('daily'), {hoje}, ['moto_ingressos', 'auto_ingressos'])) if hoje else 0
    rec7 = sum_daily(vd.get('daily'), d7, ['moto_receita', 'auto_receita']) + sum_daily(tm.get('daily'), d7, ['moto_receita', 'auto_receita'])
    rec7prev = sum_daily(vd.get('daily'), d7prev, ['moto_receita', 'auto_receita']) + sum_daily(tm.get('daily'), d7prev, ['moto_receita', 'auto_receita'])
    delta7 = round((rec7 / rec7prev - 1) * 100) if rec7prev else None

    # mix pago vs proprio (GA4, mesma regua do brand monitor: self-referral fora, social/email proprios contam)
    pago = proprio = outros = 0
    for r in ga.get('source_medium_daily', [])[-2000:]:
        s, m = (r.get('source') or '').lower(), (r.get('medium') or '').lower()
        sess = r.get('sessions', 0) or 0
        if 'festivalinterlagos.com.br' in s:
            continue
        if re.search(r'paid|cpc|ppc|ads|cpm|awareness|affiliate|display|tiktok', m):
            pago += sess
        elif ((s == '(direct)' and m == '(none)') or 'organic' in m or s == 'organico'
              or re.search(r'social|story|email', m) or s == 'ig' or 'instagram' in s
              or re.search(r'salesforce|crm|abertura|^eml', s)):
            proprio += sess
        else:
            outros += sess
    tot_sess = pago + proprio + outros
    share_prop = round(proprio / tot_sess * 100) if tot_sess else None

    # canal que mais converte (GA4 ultimo clique)
    ts = sorted(ga.get('traffic_sources', []), key=lambda x: x.get('conversions', 0), reverse=True)
    canal_top = ts[0]['channel'] if ts else None

    custo = (meta.get('totals', {}).get('cost', 0)) + (gads.get('totals', {}).get('cost', 0)) + (tt.get('totals', {}).get('cost', 0))
    conv_pix = (meta.get('totals', {}).get('purchases', 0)) + (gads.get('totals', {}).get('conversions', 0)) + (tt.get('totals', {}).get('purchases', 0))
    cpa = custo / conv_pix if conv_pix else 0

    return {
        'data_dados': hoje,
        'dias_de_campanha': (TODAY - CAMP_INI).days + 1,
        'dias_para_evento_moto': (EV_MOTO - TODAY).days,
        'dias_para_evento_auto': (EV_AUTO - TODAY).days,
        'receita_total': round(rec, 2),
        'ingressos_total': int(ing),
        'ticket_medio': round(tkt, 2),
        'receita_hoje': round(rec_hoje, 2),
        'ingressos_hoje': int(ing_hoje),
        'receita_ult_7_dias': round(rec7, 2),
        'variacao_receita_semana_pct': delta7,
        'share_trafego_proprio_pct': share_prop,
        'canal_que_mais_fecha_venda': canal_top,
        'investimento_performance': round(custo, 2),
        'cpa_medio': round(cpa, 2),
        'retorno_sobre_performance': round(rec / custo, 1) if custo else None,
        'vendas_pixel_meta': meta.get('totals', {}).get('purchases', 0),
        'vendas_pixel_google': round(gads.get('totals', {}).get('conversions', 0)),
        'vendas_pixel_tiktok': tt.get('totals', {}).get('purchases', 0),
    }


def claude(fatos, papel, instrucao):
    key = os.environ.get('ANTHROPIC_API_KEY')
    if not key:
        return None
    prompt = (
        "Voce e um analista de midia do Suhai Festival Interlagos 2026 (festival de motos 13-16/08 e "
        "carros 27-30/08). Abaixo, os FATOS reais de hoje (numeros internos de vendas e midia). "
        + instrucao +
        " Regras: portugues do Brasil, tom " + papel + ", NUNCA use travessao, seja direto, nada de "
        "encher linguica. Baseie-se SO nos fatos dados, nao invente numero. "
        'Responda APENAS JSON valido: {"titulo": "frase curta de ate 8 palavras", '
        '"paragrafo": "no maximo 2 frases", "destaques": ["3 bullets curtos"], '
        '"alerta": "1 frase de atencao ou string vazia"}.\n\nFATOS:\n' + json.dumps(fatos, ensure_ascii=False))
    try:
        body = json.dumps({'model': 'claude-haiku-4-5-20251001', 'max_tokens': 700,
                           'messages': [{'role': 'user', 'content': prompt}]}).encode()
        req = urllib.request.Request('https://api.anthropic.com/v1/messages', data=body,
                                     headers={'x-api-key': key, 'anthropic-version': '2023-06-01',
                                              'content-type': 'application/json'})
        with urllib.request.urlopen(req, timeout=120) as r:
            out = json.loads(r.read())
        txt = out['content'][0]['text']
        m = re.search(r'\{.*\}', txt, re.S)
        d = json.loads(m.group(0))
        d['updated_at'] = HOJE
        return d
    except Exception as e:
        print(f'[claude:{papel}] {e}', file=sys.stderr)
        return None


def main():
    fatos = monta_fatos()
    print('FATOS:', json.dumps(fatos, ensure_ascii=False))
    resultado = load(OUT) or {}
    resultado['updated_at'] = HOJE
    resultado['fatos'] = fatos

    leituras = {
        'geral': ('panorama para o time de marketing',
                  'Escreva um panorama da campanha hoje: como estao as vendas, qual canal puxa e o que observar.'),
        'executivo': ('executivo direto para a chefia, sem jargao tecnico',
                      'Escreva um resumo executivo para a diretoria entender o dia em 10 segundos: vendas, ritmo e o ponto de atencao mais importante.'),
        'projecao': ('analitico sobre ritmo de vendas',
                     'Comente o RITMO de vendas e se a campanha esta no caminho para a reta final, sem cravar um numero final de projecao (o grafico ja mostra). Foque em aceleracao ou desaceleracao e dias restantes.'),
    }
    for chave, (papel, instr) in leituras.items():
        r = claude(fatos, papel, instr)
        if r:
            resultado[chave] = r
            print(f'[ok] leitura {chave}: {r.get("titulo")}')

    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, indent=1)
    print('gravado', OUT)


if __name__ == '__main__':
    main()
