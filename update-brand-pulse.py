#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Varredura automatica de mencoes e sinais de marca "Suhai Festival Interlagos".
Roda 2x ao dia no GitHub Actions. Cada integracao e independente e falha de
forma silenciosa (try/except), entao o robo nunca quebra por causa de uma fonte.

Fontes SEM chave (sempre ativas):
  - Google News RSS        -> feed_auto (mencoes de imprensa)
  - Reddit search JSON     -> feed_auto (comunidades)
  - Google Autocomplete    -> busca (saude da busca da marca) + historico_busca

Fontes COM chave (ativam sozinhas quando o secret existir no repo):
  - GOOGLE_PSE_KEY + GOOGLE_PSE_CX -> serp (quem rankeia na busca do Google) + historico_serp
  - BRAVE_API_KEY                  -> feed_auto (mencoes na web via Brave Search)
  - APIFY_TOKEN                    -> social_proprio (seguidores IG/TikTok) + historico_social
"""
import json, os, re, sys, urllib.request, urllib.parse
from datetime import datetime, timezone
from xml.etree import ElementTree

PULSE_FILE = 'brand-pulse-data.json'
QUERIES = ['"festival interlagos"', '"suhai festival"']
UA = {'User-Agent': 'Mozilla/5.0 (brand-monitor-festival-interlagos)'}
NEG = re.compile(r'reclama|problema|golpe|cancel|reembolso|processo|nao compre|não compre|frustra|confus', re.I)
POS = re.compile(r'confirma|anuncia|lanca|lança|line-?up|recorde|sucesso|imperd|chega|estreia|novidade', re.I)
HOJE = datetime.now(timezone.utc).strftime('%Y-%m-%d')


def fetch(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def sentimento(txt):
    if NEG.search(txt or ''):
        return 'negativo'
    if POS.search(txt or ''):
        return 'positivo'
    return 'neutro'


def hist_append(pulse, key, registro):
    """Anexa ao historico mantendo 1 registro por dia (substitui o do mesmo dia)."""
    h = [x for x in pulse.get(key, []) if x.get('data') != HOJE]
    h.append({'data': HOJE, **registro})
    pulse[key] = h[-120:]


# ---------- fontes sem chave ----------

def google_news(feed, vistos):
    add = 0
    for q in QUERIES:
        try:
            url = ('https://news.google.com/rss/search?q=' + urllib.parse.quote(q)
                   + '&hl=pt-BR&gl=BR&ceid=BR:pt-150')
            root = ElementTree.fromstring(fetch(url))
            for item in root.iter('item'):
                t = (item.findtext('title') or '').strip()
                l = (item.findtext('link') or '').strip()
                src = item.find('{https://news.google.com/rss}source')
                fonte = src.text.strip() if src is not None and src.text else 'Google News'
                if t and l and l not in vistos:
                    feed.append({'canal': 'Imprensa', 'fonte': fonte, 'titulo': t, 'url': l,
                                 'data': (item.findtext('pubDate') or '')[:16], 'sentimento': sentimento(t)})
                    vistos.add(l)
                    add += 1
        except Exception as e:
            print(f'[news] {e}', file=sys.stderr)
    return add


def reddit(feed, vistos):
    add = 0
    for q in QUERIES:
        try:
            url = 'https://www.reddit.com/search.json?q=' + urllib.parse.quote(q) + '&sort=new&t=month&limit=25'
            data = json.loads(fetch(url))
            for ch in (data.get('data', {}).get('children', []) or []):
                p = ch.get('data', {})
                t = (p.get('title') or '').strip()
                l = 'https://reddit.com' + (p.get('permalink') or '')
                if t and l not in vistos:
                    feed.append({'canal': 'Comunidades', 'fonte': 'r/' + (p.get('subreddit') or '?'),
                                 'titulo': t, 'url': l,
                                 'data': datetime.fromtimestamp(p.get('created_utc', 0), tz=timezone.utc).strftime('%Y-%m-%d'),
                                 'engajamento': f"{p.get('score', 0)} pts / {p.get('num_comments', 0)} comentários",
                                 'sentimento': sentimento(t + ' ' + (p.get('selftext') or '')[:300])})
                    vistos.add(l)
                    add += 1
        except Exception as e:
            print(f'[reddit] {e}', file=sys.stderr)
    return add


TRANS = re.compile(r'ingresso|valor|preço|preco|cupom|desconto|data|2026|comprar', re.I)

def autocomplete(pulse):
    try:
        sug = {}
        total = trans = 0
        for q in ['festival interlagos', 'suhai festival']:
            url = ('https://suggestqueries.google.com/complete/search?client=firefox&hl=pt-BR&gl=br&q='
                   + urllib.parse.quote(q))
            data = json.loads(fetch(url).decode('utf-8', 'ignore'))
            lst = data[1] if len(data) > 1 else []
            sug[q] = lst
            total += len(lst)
            trans += sum(1 for s in lst if TRANS.search(s))
        share = round(trans / total * 100) if total else 0
        pulse['busca'] = {'updated_at': HOJE, 'sugestoes': sug, 'share_transacional': share,
                          'nota': 'Percentual das sugestões do Google que mostram intenção de compra (ingressos, datas, valores).'}
        hist_append(pulse, 'historico_busca', {'share_transacional': share, 'total_sugestoes': total})
        print(f'[autocomplete] {total} sugestões, {share}% transacionais')
    except Exception as e:
        print(f'[autocomplete] {e}', file=sys.stderr)


# ---------- fontes com chave ----------

OWNED = re.compile(r'festivalinterlagos\.com\.br|instagram\.com/festival|tiktok\.com/@festivalinterlagos|youtube\.com/@festivalinterlagos|ticketmaster\.com', re.I)
RISCO = re.compile(r'reclameaqui|consumidor\.gov', re.I)

def google_pse(pulse):
    key, cx = os.environ.get('GOOGLE_PSE_KEY'), os.environ.get('GOOGLE_PSE_CX')
    if not key or not cx:
        return
    try:
        top, riscos = [], []
        owned = 0
        for q in ['festival interlagos', 'suhai festival interlagos']:
            url = ('https://www.googleapis.com/customsearch/v1?key=' + key + '&cx=' + cx
                   + '&gl=br&hl=pt-BR&num=10&q=' + urllib.parse.quote(q))
            data = json.loads(fetch(url))
            for it in (data.get('items') or []):
                link, title = it.get('link', ''), it.get('title', '')
                tipo = 'própria' if OWNED.search(link) else ('risco' if RISCO.search(link) else 'imprensa/terceiros')
                if tipo == 'própria':
                    owned += 1
                if tipo == 'risco':
                    riscos.append({'titulo': title, 'url': link})
                top.append({'q': q, 'titulo': title, 'url': link, 'tipo': tipo})
        health = round((len([t for t in top if t['tipo'] != 'risco']) / len(top)) * 100) if top else None
        pulse['serp'] = {'updated_at': HOJE, 'top': top[:20], 'health': health,
                         'owned_top': owned, 'riscos': riscos}
        hist_append(pulse, 'historico_serp', {'health': health, 'owned': owned, 'riscos': len(riscos)})
        print(f'[serp] saúde {health}%, {owned} resultados próprios, {len(riscos)} de risco')
    except Exception as e:
        print(f'[serp] {e}', file=sys.stderr)


def brave(feed, vistos, pulse):
    key = os.environ.get('BRAVE_API_KEY')
    if not key:
        return 0
    add = 0
    try:
        # 1) descoberta de mencoes novas (ultima semana)
        url = ('https://api.search.brave.com/res/v1/web/search?count=20&freshness=pw&q='
               + urllib.parse.quote('"festival interlagos"'))
        data = json.loads(fetch(url, headers={'X-Subscription-Token': key, 'Accept': 'application/json'}))
        for r in (data.get('web', {}).get('results') or []):
            l, t = r.get('url', ''), (r.get('title') or '').strip()
            if t and l and l not in vistos:
                dom = urllib.parse.urlparse(l).netloc.replace('www.', '')
                feed.append({'canal': 'Web', 'fonte': dom, 'titulo': t, 'url': l,
                             'data': HOJE, 'sentimento': sentimento(t + ' ' + (r.get('description') or ''))})
                vistos.add(l)
                add += 1
        print(f'[brave] +{add}')
        # 2) saude da busca: quem rankeia pelas buscas da marca (web inteira, sem filtro de data)
        top, riscos = [], []
        owned = 0
        for q in ['festival interlagos', 'suhai festival interlagos']:
            u2 = 'https://api.search.brave.com/res/v1/web/search?count=10&country=br&q=' + urllib.parse.quote(q)
            d2 = json.loads(fetch(u2, headers={'X-Subscription-Token': key, 'Accept': 'application/json'}))
            for r in (d2.get('web', {}).get('results') or [])[:10]:
                link, title = r.get('url', ''), (r.get('title') or '').strip()
                tipo = 'própria' if OWNED.search(link) else ('risco' if RISCO.search(link) else 'imprensa/terceiros')
                if tipo == 'própria':
                    owned += 1
                if tipo == 'risco':
                    riscos.append({'titulo': title, 'url': link})
                top.append({'q': q, 'titulo': title, 'url': link, 'tipo': tipo})
        if top:
            health = round((len([t for t in top if t['tipo'] != 'risco']) / len(top)) * 100)
            pulse['serp'] = {'updated_at': HOJE, 'fonte': 'Brave Search', 'top': top[:20],
                             'health': health, 'owned_top': owned, 'riscos': riscos}
            hist_append(pulse, 'historico_serp', {'health': health, 'owned': owned, 'riscos': len(riscos)})
            print(f'[brave-serp] saúde {health}%, {owned} próprios, {len(riscos)} de risco')
    except Exception as e:
        print(f'[brave] {e}', file=sys.stderr)
    return add


def apify(pulse):
    token = os.environ.get('APIFY_TOKEN')
    if not token:
        return
    social = pulse.get('social_proprio', {})
    # Instagram
    try:
        url = f'https://api.apify.com/v2/acts/apify~instagram-profile-scraper/run-sync-get-dataset-items?token={token}'
        body = json.dumps({'usernames': ['festival.interlagos']}).encode()
        req = urllib.request.Request(url, data=body, headers={**UA, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=300) as r:
            items = json.loads(r.read())
        if items:
            it = items[0]
            social['instagram'] = {'followers': it.get('followersCount'), 'posts': it.get('postsCount'),
                                   'updated_at': HOJE}
            print(f"[apify-ig] {it.get('followersCount')} seguidores")
    except Exception as e:
        print(f'[apify-ig] {e}', file=sys.stderr)
    # TikTok
    try:
        url = f'https://api.apify.com/v2/acts/clockworks~tiktok-profile-scraper/run-sync-get-dataset-items?token={token}'
        body = json.dumps({'profiles': ['festivalinterlagos'], 'resultsPerPage': 1,
                           'shouldDownloadVideos': False, 'shouldDownloadCovers': False}).encode()
        req = urllib.request.Request(url, data=body, headers={**UA, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=300) as r:
            items = json.loads(r.read())
        fans = None
        for it in items:
            meta = it.get('authorMeta') or {}
            fans = meta.get('fans') or meta.get('followers') or fans
        if fans:
            social['tiktok'] = {'followers': fans, 'updated_at': HOJE}
            print(f'[apify-tt] {fans} seguidores')
    except Exception as e:
        print(f'[apify-tt] {e}', file=sys.stderr)
    if social:
        pulse['social_proprio'] = social
        hist_append(pulse, 'historico_social', {
            'ig': (social.get('instagram') or {}).get('followers'),
            'tt': (social.get('tiktok') or {}).get('followers')})


def main():
    with open(PULSE_FILE, encoding='utf-8') as f:
        pulse = json.load(f)

    feed = pulse.get('feed_auto', [])
    vistos = {m.get('url') for m in feed}
    n = google_news(feed, vistos) + reddit(feed, vistos) + brave(feed, vistos, pulse)
    feed = feed[-300:]
    pulse['feed_auto'] = feed
    pulse['feed_auto_updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    pulse['feed_auto_total'] = len(feed)
    pulse['feed_auto_negativas'] = len([m for m in feed if m.get('sentimento') == 'negativo'])

    autocomplete(pulse)
    google_pse(pulse)
    apify(pulse)

    pulse['integracoes'] = {
        'google_news': True, 'reddit': True, 'autocomplete': 'busca' in pulse,
        'busca_google_pse': bool(os.environ.get('GOOGLE_PSE_KEY')),
        'brave': bool(os.environ.get('BRAVE_API_KEY')),
        'apify_social': bool(os.environ.get('APIFY_TOKEN')),
    }

    with open(PULSE_FILE, 'w', encoding='utf-8') as f:
        json.dump(pulse, f, ensure_ascii=False, indent=1)
    print(f'ok: +{n} mencoes novas, {len(feed)} no feed, {pulse["feed_auto_negativas"]} negativas')


if __name__ == '__main__':
    main()
