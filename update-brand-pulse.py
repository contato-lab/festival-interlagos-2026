#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Varredura automatica diaria de mencoes da marca "Suhai Festival Interlagos".
Fontes gratuitas: Google News RSS + Reddit search JSON.
Atualiza a secao feed_auto do brand-pulse-data.json (dedupe por URL).
A varredura profunda (sentimento, veredito, scores) continua sendo feita
pela analise multi-agente e preservada neste JSON.
"""
import json, re, sys, urllib.request, urllib.parse
from datetime import datetime, timezone
from xml.etree import ElementTree

PULSE_FILE = 'brand-pulse-data.json'
QUERIES = ['"festival interlagos"', '"suhai festival"']
UA = {'User-Agent': 'Mozilla/5.0 (brand-monitor-festival-interlagos)'}
NEG_HINTS = re.compile(r'reclama|problema|golpe|cancel|reembolso|processo|nao compre|não compre|frustra|confus', re.I)
POS_HINTS = re.compile(r'confirma|anuncia|lanca|lança|line-?up|recorde|sucesso|imperd|chega|estreia|novidade', re.I)


def fetch(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def sentimento_basico(txt):
    if NEG_HINTS.search(txt or ''):
        return 'negativo'
    if POS_HINTS.search(txt or ''):
        return 'positivo'
    return 'neutro'


def google_news():
    out = []
    for q in QUERIES:
        url = ('https://news.google.com/rss/search?q=' + urllib.parse.quote(q)
               + '&hl=pt-BR&gl=BR&ceid=BR:pt-150')
        try:
            root = ElementTree.fromstring(fetch(url))
        except Exception as e:
            print(f'[news] erro {q}: {e}', file=sys.stderr)
            continue
        for item in root.iter('item'):
            t = (item.findtext('title') or '').strip()
            l = (item.findtext('link') or '').strip()
            d = (item.findtext('pubDate') or '').strip()
            src = item.find('{https://news.google.com/rss}source')
            fonte = src.text.strip() if src is not None and src.text else 'Google News'
            if t and l:
                out.append({'canal': 'Imprensa', 'fonte': fonte, 'titulo': t, 'url': l,
                            'data': d[:16], 'sentimento': sentimento_basico(t)})
    return out


def reddit():
    out = []
    for q in QUERIES:
        url = ('https://www.reddit.com/search.json?q=' + urllib.parse.quote(q)
               + '&sort=new&t=month&limit=25')
        try:
            data = json.loads(fetch(url))
        except Exception as e:
            print(f'[reddit] erro {q}: {e}', file=sys.stderr)
            continue
        for ch in (data.get('data', {}).get('children', []) or []):
            p = ch.get('data', {})
            t = (p.get('title') or '').strip()
            if not t:
                continue
            out.append({'canal': 'Comunidades', 'fonte': 'r/' + (p.get('subreddit') or '?'),
                        'titulo': t, 'url': 'https://reddit.com' + (p.get('permalink') or ''),
                        'data': datetime.fromtimestamp(p.get('created_utc', 0), tz=timezone.utc).strftime('%Y-%m-%d'),
                        'engajamento': f"{p.get('score',0)} pts / {p.get('num_comments',0)} comentários",
                        'sentimento': sentimento_basico(t + ' ' + (p.get('selftext') or '')[:300])})
    return out


def main():
    with open(PULSE_FILE, encoding='utf-8') as f:
        pulse = json.load(f)

    novas = google_news() + reddit()
    feed = pulse.get('feed_auto', [])
    vistos = {m.get('url') for m in feed}
    adicionadas = 0
    for m in novas:
        if m['url'] not in vistos:
            feed.append(m)
            vistos.add(m['url'])
            adicionadas += 1
    feed = feed[-200:]  # mantem as 200 mais recentes

    pulse['feed_auto'] = feed
    pulse['feed_auto_updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    pulse['feed_auto_total'] = len(feed)
    neg = [m for m in feed if m.get('sentimento') == 'negativo']
    pulse['feed_auto_negativas'] = len(neg)

    with open(PULSE_FILE, 'w', encoding='utf-8') as f:
        json.dump(pulse, f, ensure_ascii=False, indent=1)
    print(f'feed_auto: +{adicionadas} novas, {len(feed)} no total, {len(neg)} com sinal negativo')


if __name__ == '__main__':
    main()
