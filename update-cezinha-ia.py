#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnóstico das redes do Deputado Cezinha de Madureira, com IA (Claude).
Lê cezinha-data.json (dados reais das campanhas Meta + crescimento de seguidores),
monta um resumo de FATOS e pede ao Claude um diagnóstico direto e acionável.
Grava cezinha-ia.json. A chave NUNCA vai pro navegador (o dashboard só lê o JSON).
Roda 1x por dia. Só executa se ANTHROPIC_API_KEY existir.
"""
import json, os, re, sys, urllib.request
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(HERE, 'cezinha-data.json')
OUT = os.path.join(HERE, 'cezinha-ia.json')
BRT = timezone(timedelta(hours=-3))
AGORA = datetime.now(BRT).isoformat(timespec='seconds')


def load(f):
    try:
        with open(f, encoding='utf-8') as fh:
            return json.load(fh)
    except Exception:
        return None


def monta_fatos():
    d = load(DATA_FILE) or {}
    camps = d.get('campanhas', [])
    seg = d.get('seguidores', {})

    por_camp = []
    tot_spend = tot_reach = tot_impr = tot_eng = 0
    for c in camps:
        t = c.get('totais', {}) or {}
        por_camp.append({
            'nome': c.get('nome'),
            'objetivo': c.get('objetivo_label'),
            'investido_reais': t.get('spend'),
            'alcance': t.get('reach'),
            'impressoes': t.get('impressions'),
            'frequencia': t.get('frequency'),
            'cpm_reais': t.get('cpm'),
            'ctr_pct': t.get('ctr'),
            'cliques_no_link': t.get('link_clicks'),
            'engajamentos': t.get('engajamentos'),
            'reacoes': t.get('reacoes'),
            'comentarios': t.get('comentarios'),
            'reproducoes_video': t.get('video_plays'),
            'custo_por_engajamento_reais': t.get('custo_engajamento'),
        })
        tot_spend += t.get('spend', 0) or 0
        tot_reach += t.get('reach', 0) or 0
        tot_impr += t.get('impressions', 0) or 0
        tot_eng += t.get('engajamentos', 0) or 0

    serie = seg.get('serie', []) or []
    cresc_fb = cresc_ig = None
    if len(serie) >= 2:
        cresc_fb = int((serie[-1].get('fb', 0) or 0) - (serie[0].get('fb', 0) or 0))
        cresc_ig = int((serie[-1].get('ig', 0) or 0) - (serie[0].get('ig', 0) or 0))

    return {
        'campanhas': por_camp,
        'total_investido_reais': round(tot_spend, 2),
        'alcance_somado': int(tot_reach),
        'impressoes_total': int(tot_impr),
        'engajamentos_total': int(tot_eng),
        'seguidores_facebook': seg.get('fb_atual'),
        'seguidores_instagram': seg.get('ig_atual'),
        'instagram_contas_que_segue': seg.get('ig_follows'),
        'instagram_total_posts': seg.get('ig_posts'),
        'crescimento_facebook_no_periodo_medido': cresc_fb,
        'crescimento_instagram_no_periodo_medido': cresc_ig,
        'dias_de_medicao_de_seguidores': len(serie),
    }


def claude(fatos):
    key = os.environ.get('ANTHROPIC_API_KEY')
    if not key:
        print('ANTHROPIC_API_KEY ausente, abortando', file=sys.stderr)
        return None
    prompt = (
        "Você é um estrategista sênior de redes sociais e mídia paga, analisando as redes do "
        "Deputado Federal Cezinha de Madureira (político de São Paulo). Abaixo estão os FATOS reais "
        "de hoje: desempenho das campanhas no Meta (Instagram e Facebook) e crescimento das redes. "
        "Faça um diagnóstico DIRETO e RETO, de alto valor, que dê ao time uma visão clara de onde a "
        "operação está forte, onde está fraca e o que fazer a seguir. Pense como quem vai apresentar "
        "isso pro deputado e pro time em 30 segundos. Seja concreto e use os números reais (cite valores "
        "quando ajudar). Nada de encher linguiça nem elogio vazio. "
        "Regras: português do Brasil, NUNCA use travessão, nada de emoji, baseie-se SÓ nos fatos dados, "
        "não invente número. "
        'Responda APENAS um JSON válido neste formato: '
        '{"titulo": "frase curta de ate 9 palavras", '
        '"panorama": "2 a 3 frases diretas sobre o estado geral das redes e campanhas hoje", '
        '"destaques": ["2 a 3 bullets curtos do que esta indo bem, com numero quando der"], '
        '"atencao": ["2 a 3 bullets curtos de onde melhorar ou o que esta fraco"], '
        '"proximos_passos": ["2 a 3 acoes concretas e priorizadas pra fazer a seguir"]}.\n\n'
        'FATOS:\n' + json.dumps(fatos, ensure_ascii=False))
    try:
        body = json.dumps({'model': 'claude-haiku-4-5-20251001', 'max_tokens': 1100,
                           'messages': [{'role': 'user', 'content': prompt}]}).encode()
        req = urllib.request.Request('https://api.anthropic.com/v1/messages', data=body,
                                     headers={'x-api-key': key, 'anthropic-version': '2023-06-01',
                                              'content-type': 'application/json'})
        with urllib.request.urlopen(req, timeout=120) as r:
            out = json.loads(r.read())
        txt = out['content'][0]['text']
        m = re.search(r'\{.*\}', txt, re.S)
        return json.loads(m.group(0))
    except Exception as e:
        print(f'[claude] {e}', file=sys.stderr)
        return None


def main():
    fatos = monta_fatos()
    print('FATOS:', json.dumps(fatos, ensure_ascii=False))
    diag = claude(fatos)
    resultado = load(OUT) or {}
    resultado['gerado_em'] = AGORA
    resultado['fatos'] = fatos
    if diag:
        resultado['diagnostico'] = diag
        print('[ok] diagnostico:', diag.get('titulo'))
    else:
        print('[warn] diagnostico nao gerado, mantendo o anterior se houver')

    tmp = OUT + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, indent=1)
    os.replace(tmp, OUT)
    print('gravado', OUT)


if __name__ == '__main__':
    main()
