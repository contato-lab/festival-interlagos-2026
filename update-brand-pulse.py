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


NEGC = re.compile(r'porcaria|merda|m&\$|m\$|lixo|contram[aã]o|p[ée]ssim|horr[ií]vel|vergonha|descaso|decep|nada a ver|odiei|palha[çc]ada|absurdo|petista', re.I)
POSC = re.compile(r'\bbora+\b|simbora+|j[áa] comprei|\btop\b|incr[íi]vel|sonho|maravilh|perfeito|amei|ansios|n[ãa]o vejo a hora|da hora|show de bola|parab[ée]ns|🔥|❤|🥰|😍|👏', re.I)

def sentimento_comentario(txt):
    t = txt or ''
    if NEGC.search(t):
        return 'negativo'
    if POSC.search(t):
        return 'positivo'
    return 'neutro'


def claude_classifica(comentarios):
    """Classifica sentimento e gera resumo via Claude API (Haiku). Retorna (labels, resumo) ou (None, None).
    So roda se ANTHROPIC_API_KEY existir; custo estimado: centavos por dia."""
    key = os.environ.get('ANTHROPIC_API_KEY')
    if not key or not comentarios:
        return None, None
    try:
        prompt = ("Comentários públicos em posts/vídeos sobre o Suhai Festival Interlagos "
                  "(festival de motos e carros com test rides e shows). "
                  "Tarefa 1: classifique o sentimento de CADA comentário em relação ao festival: "
                  "'positivo', 'negativo' ou 'neutro'. Regras: ironia, deboche e palavrões censurados "
                  "(ex: m&$#@) são negativos; críticas ao lineup/shows são negativas; reclamações da "
                  "seguradora Suhai são negativas; dúvidas são neutras; empolgação (bora, já comprei, "
                  "emojis de amor/fogo) é positiva. "
                  "Tarefa 2: um resumo de no máximo 2 frases dos temas dominantes, em português, sem travessão. "
                  'Responda APENAS JSON válido: {"sentimentos": ["..."], "resumo": "..."}.\n\nComentários:\n'
                  + json.dumps([c.get('texto', '') for c in comentarios], ensure_ascii=False))
        body = json.dumps({'model': 'claude-haiku-4-5-20251001', 'max_tokens': 1200,
                           'messages': [{'role': 'user', 'content': prompt}]}).encode()
        req = urllib.request.Request('https://api.anthropic.com/v1/messages', data=body,
                                     headers={'x-api-key': key, 'anthropic-version': '2023-06-01',
                                              'content-type': 'application/json'})
        with urllib.request.urlopen(req, timeout=120) as r:
            out = json.loads(r.read())
        txt = out['content'][0]['text']
        m = re.search(r'\{.*\}', txt, re.S)
        data = json.loads(m.group(0))
        labs = data.get('sentimentos')
        if labs and len(labs) == len(comentarios):
            labs = [l if l in ('positivo', 'negativo', 'neutro') else 'neutro' for l in labs]
            return labs, data.get('resumo')
    except Exception as e:
        print(f'[claude] {e}', file=sys.stderr)
    return None, None


def claude_leitura_marca(pulse):
    """Leitura interpretativa da marca via Claude (Haiku). Grava pulse['leitura_ia']."""
    key = os.environ.get('ANTHROPIC_API_KEY')
    if not key:
        return
    ver = pulse.get('veredito', {})
    resumo_coment = (pulse.get('social_comentarios', {}) or {}).get('resumo', {})
    fatos = {
        'forca_da_marca_0a100': pulse.get('voz_score'),
        'total_mencoes_periodo': pulse.get('total_mencoes'),
        'mencoes_negativas': pulse.get('feed_auto_negativas'),
        'furou_a_bolha': ver.get('furou_bolha'),
        'justificativa': ver.get('justificativa'),
        'pontos_fortes': ver.get('pontos_fortes'),
        'pontos_fracos': ver.get('pontos_fracos'),
        'riscos': ver.get('riscos'),
        'frentes': [{'nome': f.get('nome'), 'score': f.get('score')} for f in pulse.get('frentes', [])][:8],
        'seguidores_instagram': (pulse.get('social_proprio', {}) or {}).get('instagram', {}).get('followers'),
        'seguidores_tiktok': (pulse.get('social_proprio', {}) or {}).get('tiktok', {}).get('followers'),
        'resumo_comentarios': resumo_coment,
    }
    prompt = (
        "Voce e analista de marca do Suhai Festival Interlagos 2026 (festival de motos 13-16/08 e carros "
        "27-30/08). Abaixo, os sinais reais da marca hoje (forca da marca 0 a 100, mencoes, veredito sobre "
        "furar a bolha, frentes monitoradas e resumo dos comentarios). Escreva uma leitura executiva da "
        "MARCA: como ela esta hoje, se esta furando a bolha e qual a prioridade numero um para agir. "
        "Regras: portugues do Brasil, direto, NUNCA use travessao, nao invente numero, baseie-se so nos "
        'fatos. Responda APENAS JSON valido: {"titulo": "ate 8 palavras", "paragrafo": "no maximo 2 frases", '
        '"destaques": ["3 bullets curtos"], "alerta": "1 frase de atencao ou string vazia"}.\n\nSINAIS:\n'
        + json.dumps(fatos, ensure_ascii=False))
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
        d['updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        pulse['leitura_ia'] = d
        print(f'[claude] leitura da marca: {d.get("titulo")}')
    except Exception as e:
        print(f'[claude-leitura] {e}', file=sys.stderr)


def apify_call(actor, payload, token, timeout=300):
    url = f'https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items?token={token}'
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={**UA, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def social_comments(pulse):
    """Coleta comentarios publicos (IG oficial, TikTok oficial, videos de criadores no YouTube).
    Roda 1x ao dia (execucao da manha) para caber nos creditos gratis do Apify."""
    token = os.environ.get('APIFY_TOKEN')
    if not token:
        return
    hour = datetime.now(timezone.utc).hour
    if not (hour < 15 or os.environ.get('FORCE_COMMENTS')):
        return
    sc = pulse.get('social_comentarios', {})
    plats = sc.setdefault('plataformas', {})

    def push(plat, items):
        old = (plats.get(plat) or {}).get('comentarios', [])
        seen = {(c.get('autor'), c.get('texto')) for c in old}
        for it in items:
            k = (it.get('autor'), it.get('texto'))
            if k not in seen:
                old.append(it)
                seen.add(k)
        old = old[-30:]
        # sentimento fino + resumo automatico via Claude API (quando a chave existir)
        labs, resumo = claude_classifica(old)
        if labs:
            for c, l in zip(old, labs):
                c['sentimento'] = l
            print(f'[claude-{plat}] sentimento e resumo via API')
        if resumo:
            sc.setdefault('resumo', {})
            if isinstance(sc['resumo'], dict):
                sc['resumo'][plat] = resumo
        pos = len([c for c in old if c.get('sentimento') == 'positivo'])
        neg = len([c for c in old if c.get('sentimento') == 'negativo'])
        plats[plat] = {'coletados': len(old), 'positivos': pos, 'negativos': neg,
                       'comentarios': old, 'updated_at': HOJE}

    # Instagram: comentarios dos 3 posts mais recentes do perfil oficial
    try:
        posts = apify_call('apify~instagram-post-scraper',
                           {'username': ['festival.interlagos'], 'resultsLimit': 3}, token)
        urls = [p.get('url') for p in posts if p.get('url')][:3]
        if urls:
            coms = apify_call('apify~instagram-comment-scraper',
                              {'directUrls': urls, 'resultsLimit': 30}, token)
            items = [{'texto': (c.get('text') or '')[:220], 'autor': c.get('ownerUsername') or '?',
                      'likes': c.get('likesCount') or 0, 'sentimento': sentimento_comentario(c.get('text')),
                      'origem': 'post oficial'} for c in coms if c.get('text')]
            push('instagram', items)
            print(f'[coment-ig] {len(items)} comentarios')
    except Exception as e:
        print(f'[coment-ig] {e}', file=sys.stderr)

    # TikTok: comentarios dos 3 videos mais recentes do perfil oficial
    try:
        vids = apify_call('clockworks~tiktok-profile-scraper',
                          {'profiles': ['festivalinterlagos'], 'resultsPerPage': 3,
                           'shouldDownloadVideos': False, 'shouldDownloadCovers': False,
                           'shouldDownloadSubtitles': False}, token)
        vurls = [v.get('webVideoUrl') for v in vids if v.get('webVideoUrl')][:3]
        if vurls:
            coms = apify_call('clockworks~tiktok-comments-scraper',
                              {'postURLs': vurls, 'commentsPerPost': 10}, token)
            items = [{'texto': (c.get('text') or '')[:220], 'autor': c.get('uniqueId') or '?',
                      'likes': c.get('diggCount') or 0, 'sentimento': sentimento_comentario(c.get('text')),
                      'origem': 'vídeo oficial'} for c in coms if c.get('text')]
            push('tiktok', items)
            print(f'[coment-tt] {len(items)} comentarios')
    except Exception as e:
        print(f'[coment-tt] {e}', file=sys.stderr)

    # YouTube: comentarios dos videos de criadores monitorados (lista atualizada nas varreduras)
    try:
        vlist = pulse.get('videos_monitorados') or []
        if vlist:
            coms = apify_call('streamers~youtube-comments-scraper',
                              {'startUrls': [{'url': u} for u in vlist[:3]], 'maxComments': 10}, token)
            items = [{'texto': (c.get('comment') or c.get('text') or '')[:220],
                      'autor': c.get('author') or c.get('authorName') or '?',
                      'likes': c.get('voteCount') or c.get('likes') or 0,
                      'sentimento': sentimento_comentario(c.get('comment') or c.get('text')),
                      'origem': 'vídeo de criador'} for c in coms if (c.get('comment') or c.get('text'))]
            push('youtube', items)
            print(f'[coment-yt] {len(items)} comentarios')
    except Exception as e:
        print(f'[coment-yt] {e}', file=sys.stderr)

    sc['updated_at'] = HOJE
    pulse['social_comentarios'] = sc


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
    social_comments(pulse)
    claude_leitura_marca(pulse)

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
