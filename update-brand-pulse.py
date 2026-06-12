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
from datetime import datetime, timezone, date
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
            return labs, _limpa(data.get('resumo'))
    except Exception as e:
        print(f'[claude] {e}', file=sys.stderr)
    return None, None


def _limpa(x):
    """Remove travessao e tags de textos gerados (regra da casa: nunca travessao)."""
    if isinstance(x, str):
        x = re.sub(r'(?<=\d)\s*[–—]\s*(?=\d)', ' a ', x)   # intervalos: 13–16 vira 13 a 16
        x = re.sub(r'\s*[–—]\s*', ', ', x).replace('<', '').replace('>', '')
        return re.sub(r'^[,\s]+', '', x).strip()
    if isinstance(x, list):
        return [_limpa(i) for i in x]
    if isinstance(x, dict):
        return {k: _limpa(v) for k, v in x.items()}
    return x


def _ga4_share_proprio():
    """Share de trafego proprio (organico + direto) em duas janelas: lancamento e ultimos 14 dias."""
    try:
        with open('ga4-data.json', encoding='utf-8') as f:
            rows = [r for r in (json.load(f).get('source_medium_daily') or []) if isinstance(r, dict)]
        datas = sorted({r.get('date') for r in rows if r.get('date')})
        if len(datas) < 7:
            return None

        def proprio(r):
            s, m = (r.get('source') or '').lower(), (r.get('medium') or '').lower()
            return (s == '(direct)' and m == '(none)') or 'organic' in m

        def share(dset):
            tot = own = 0
            for r in rows:
                if r.get('date') in dset:
                    try:
                        sess = int(r.get('sessions') or 0)
                    except (TypeError, ValueError):
                        continue
                    tot += sess
                    own += sess if proprio(r) else 0
            return round(own / tot * 100) if tot else None

        return {'lancamento_pct': share(set(datas[:14])), 'ultimos_14_dias_pct': share(set(datas[-14:]))}
    except Exception as e:
        print(f'[ga4-share] {e}', file=sys.stderr)
        return None


def _conta_sentimentos(pulse):
    cont = {'positivo': 0, 'negativo': 0, 'neutro': 0}
    for m in (pulse.get('mencoes_destaque') or []) + (pulse.get('feed_auto') or []):
        if isinstance(m, dict) and m.get('sentimento') in cont:
            cont[m['sentimento']] += 1
    for p in ((pulse.get('social_comentarios') or {}).get('plataformas') or {}).values():
        if not isinstance(p, dict):
            continue
        for c in p.get('comentarios') or []:
            if isinstance(c, dict) and c.get('sentimento') in cont:
                cont[c['sentimento']] += 1
    return cont


def _lista_str(v, vazio_ok=False):
    return isinstance(v, list) and (vazio_ok or v) and all(isinstance(i, str) and i.strip() for i in v)


def _interpreta_valida(d):
    ver = d.get('veredito') or {}
    if ver.get('furou_bolha') not in ('sim', 'parcialmente', 'nao'):
        return False
    if not isinstance(ver.get('resumo_selo'), str) or not ver['resumo_selo'].strip():
        return False
    if 'justificativa' in ver and not isinstance(ver['justificativa'], str):
        return False
    chips = d.get('chips') or {}
    if not all(_lista_str(chips.get(k)) for k in ('fortes', 'fracos', 'riscos')):
        return False
    acoes = d.get('acoes')
    if not isinstance(acoes, list) or not acoes or not all(
            isinstance(a, dict) and all(isinstance(a.get(k), str) and a[k].strip() for k in ('t', 'd', 'prazo'))
            for a in acoes):
        return False
    fr = d.get('frentes')
    if not isinstance(fr, list) or not fr or not all(
            isinstance(f, dict) and f.get('nome') and isinstance(f.get('conclusao'), str) for f in fr):
        return False
    lei = d.get('leitura') or {}
    if not lei.get('titulo') or not lei.get('paragrafo') or not _lista_str(lei.get('destaques')):
        return False
    return True


def claude_interpreta_marca(pulse):
    """Interpretacao completa do Brand Monitor via Claude: voz_score/voz_why, veredito da bolha,
    chips, proximas acoes, conclusao por frente e leitura da marca. Uma unica chamada por varredura
    para manter o custo baixo. Em qualquer falha, os textos anteriores permanecem intactos."""
    key = os.environ.get('ANTHROPIC_API_KEY')
    if not key:
        return
    try:
        hoje = datetime.now(timezone.utc).date()
        sent = _conta_sentimentos(pulse)
        ig = (pulse.get('social_proprio') or {}).get('instagram') or {}
        tk = (pulse.get('social_proprio') or {}).get('tiktok') or {}
        hist_soc = [h for h in pulse.get('historico_social') or [] if isinstance(h, dict)]
        plats = (pulse.get('social_comentarios') or {}).get('plataformas') or {}
        sinais = {
            'data_de_hoje': hoje.isoformat(),
            'dias_para_festival_motos_13_08': (date(2026, 8, 13) - hoje).days,
            'dias_para_festival_carros_27_08': (date(2026, 8, 27) - hoje).days,
            'mencoes_catalogadas_varredura_profunda': pulse.get('total_mencoes'),
            'feed_automatico_total': pulse.get('feed_auto_total'),
            'feed_automatico_negativas': pulse.get('feed_auto_negativas'),
            'contagem_sentimento_mencoes_e_comentarios': sent,
            'mencoes_recentes_feed': [
                {'fonte': m.get('fonte'), 'titulo': m.get('titulo'), 'sentimento': m.get('sentimento')}
                for m in (pulse.get('feed_auto') or [])[-25:] if isinstance(m, dict)],
            'busca_share_transacional_pct': (pulse.get('busca') or {}).get('share_transacional'),
            'serp_saude_pct': (pulse.get('serp') or {}).get('health'),
            'instagram_oficial': {'seguidores': ig.get('followers'), 'posts': ig.get('posts')},
            'tiktok_oficial': {'seguidores': tk.get('followers')},
            'evolucao_seguidores': {'inicio': hist_soc[0] if hist_soc else None,
                                    'agora': hist_soc[-1] if hist_soc else None},
            'comentarios_por_plataforma': {
                k: {kk: v.get(kk) for kk in ('coletados', 'positivos', 'negativos')}
                for k, v in plats.items() if isinstance(v, dict)},
            'resumo_comentarios': (pulse.get('social_comentarios') or {}).get('resumo'),
            'share_trafego_proprio_site': _ga4_share_proprio(),
            'analise_anterior': {
                'voz_score': pulse.get('voz_score'),
                'voz_why': pulse.get('voz_why'),
                'veredito': pulse.get('veredito'),
                'chips': pulse.get('chips'),
                'acoes': pulse.get('acoes'),
                'frentes': [{'nome': f.get('nome'), 'score': f.get('score'), 'conclusao': f.get('conclusao')}
                            for f in pulse.get('frentes') or [] if isinstance(f, dict)],
            },
        }
        prompt = (
            "Voce e o analista de marca do Suhai Festival Interlagos 2026 (festival de motos 13 a 16/08 e de "
            "carros 27 a 30/08, no Autodromo de Interlagos). Abaixo estao os SINAIS reais coletados hoje pela "
            "varredura automatica (mencoes, busca, redes, comentarios, trafego do site) e a ANALISE ANTERIOR. "
            "Sua tarefa: ATUALIZAR a analise completa do Brand Monitor. Mantenha o que segue valido da analise "
            "anterior, atualize o que os sinais novos mudaram e corrija numeros citados nos textos para os "
            "valores atuais. Traga informacao pertinente e acionavel, nada generico.\n"
            "REGRAS OBRIGATORIAS: portugues do Brasil; NUNCA use travessao em nenhum texto; nao use emojis; "
            "nao invente numero nenhum, use somente os numeros presentes nos SINAIS; nunca cite estimativa de "
            "publico do evento; tom direto de analista, sem encher linguica. PRECISAO sobre reclamacoes: o que "
            "esta zerado em 2026 e o Reclame Aqui; EXISTEM reclamacoes nos comentarios das redes (SAC da "
            "seguradora Suhai herdado pelo naming rights, criticas ao lineup), entao ao falar de reclamacao "
            "diga sempre 'zero no Reclame Aqui' e nao 'zero reclamacao' generico.\n"
            "FORMATO: responda APENAS um JSON valido com exatamente esta estrutura:\n"
            '{"voz_score": inteiro 0 a 100 (forca da voz da marca hoje),\n'
            '"voz_why": "1 a 2 frases justificando o voz_score",\n'
            '"veredito": {"furou_bolha": "sim" ou "parcialmente" ou "nao" (sem acento),\n'
            ' "resumo_selo": "frase de ate 28 palavras explicando o veredito, vai ao lado do selo",\n'
            ' "justificativa": "paragrafo de ate 150 palavras com A favor: e Contra:"},\n'
            '"chips": {"fortes": ["4 a 5 chips de 4 a 8 palavras"], "fracos": ["4 a 5 chips"], "riscos": ["3 a 4 chips"]},\n'
            '"acoes": [3 a 4 itens {"t": "titulo de ate 7 palavras", "d": "justificativa de ate 25 palavras '
            'baseada nos sinais", "prazo": "Esta semana" ou "Antes de julho" ou "Preparar agora" ou similar}],\n'
            '"frentes": [para cada frente da analise anterior, manter o MESMO nome exato com acentos: '
            '{"nome": "...", "score": inteiro 0 a 100, "conclusao": "1 a 2 frases atualizadas"}],\n'
            '"leitura": {"titulo": "ate 8 palavras", "paragrafo": "no maximo 2 frases", '
            '"destaques": ["3 bullets de ate 18 palavras"], "alerta": "1 frase com a prioridade numero 1"}}\n\n'
            'SINAIS:\n' + json.dumps(sinais, ensure_ascii=False))
        body = json.dumps({'model': 'claude-haiku-4-5-20251001', 'max_tokens': 6000,
                           'messages': [{'role': 'user', 'content': prompt}]}).encode()
        req = urllib.request.Request('https://api.anthropic.com/v1/messages', data=body,
                                     headers={'x-api-key': key, 'anthropic-version': '2023-06-01',
                                              'content-type': 'application/json'})
        with urllib.request.urlopen(req, timeout=180) as r:
            out = json.loads(r.read())
        if out.get('stop_reason') == 'max_tokens':
            print('[claude-interpreta] resposta truncada (max_tokens), mantendo textos anteriores', file=sys.stderr)
            return
        txt = out['content'][0]['text']
        m = re.search(r'\{.*\}', txt, re.S)
        if not m:
            print('[claude-interpreta] sem JSON na resposta, mantendo textos anteriores', file=sys.stderr)
            return
        d = _limpa(json.loads(m.group(0)))
        ver_novo = d.get('veredito') or {}
        fb = (ver_novo.get('furou_bolha') or '').strip().lower().replace('não', 'nao')
        if fb in ('sim', 'parcialmente', 'nao'):
            ver_novo['furou_bolha'] = fb
        if not _interpreta_valida(d):
            print('[claude-interpreta] resposta invalida, mantendo textos anteriores', file=sys.stderr)
            return

        # tudo validado: monta as mudancas e aplica de uma vez (atomico)
        agora = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        ver_atual = pulse.get('veredito')
        ver_atual = dict(ver_atual) if isinstance(ver_atual, dict) else {}
        ver_atual.update({k: v for k, v in ver_novo.items() if v})
        frentes_atuais = [dict(f) for f in pulse.get('frentes') or [] if isinstance(f, dict)]
        por_nome = {f.get('nome'): f for f in d.get('frentes') or [] if isinstance(f, dict)}
        for f in frentes_atuais:
            novo = por_nome.pop(f.get('nome'), None)
            if novo:
                try:
                    f['score'] = max(0, min(100, int(novo.get('score', f.get('score')))))
                except (TypeError, ValueError):
                    pass
                if novo.get('conclusao'):
                    f['conclusao'] = novo['conclusao']
        if por_nome:
            print(f'[claude-interpreta] frentes sem par (nome divergente): {list(por_nome)}', file=sys.stderr)
        lei = d['leitura']
        lei['updated_at'] = agora
        updates = {'veredito': ver_atual, 'chips': d['chips'], 'acoes': d['acoes'][:4],
                   'frentes': frentes_atuais, 'leitura_ia': lei, 'interpretacao_updated_at': agora}
        try:
            updates['voz_score'] = max(0, min(100, int(d['voz_score'])))
        except (TypeError, ValueError):
            pass
        if isinstance(d.get('voz_why'), str) and d['voz_why'].strip():
            updates['voz_why'] = d['voz_why']
        pulse.update(updates)
        s = _conta_sentimentos(pulse)
        nao_neutros = s['positivo'] + s['negativo']
        hist_append(pulse, 'historico', {
            'voz': pulse.get('voz_score'),
            'mencoes': pulse.get('total_mencoes'),
            'sent_pos': round(s['positivo'] / nao_neutros * 100) if nao_neutros else None})
        print(f'[claude] interpretacao completa: {lei.get("titulo")} | voz {pulse.get("voz_score")} | '
              f'bolha {ver_atual.get("furou_bolha")}')
    except Exception as e:
        print(f'[claude-interpreta] {e}', file=sys.stderr)


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
    # sanitiza texto vindo de fontes externas (travessao e tags nunca chegam ao dashboard)
    for m in feed:
        if isinstance(m, dict):
            for k in ('titulo', 'fonte'):
                if isinstance(m.get(k), str):
                    m[k] = _limpa(m[k])
    pulse['feed_auto'] = feed
    pulse['feed_auto_updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    pulse['feed_auto_total'] = len(feed)
    pulse['feed_auto_negativas'] = len([m for m in feed if m.get('sentimento') == 'negativo'])

    autocomplete(pulse)
    google_pse(pulse)
    apify(pulse)
    social_comments(pulse)
    claude_interpreta_marca(pulse)

    pulse['integracoes'] = {
        'google_news': True, 'reddit': True, 'autocomplete': 'busca' in pulse,
        'busca_google_pse': bool(os.environ.get('GOOGLE_PSE_KEY')),
        'brave': bool(os.environ.get('BRAVE_API_KEY')),
        'apify_social': bool(os.environ.get('APIFY_TOKEN')),
    }

    # serp pode ter sido reescrito por google_pse/brave depois do feed: sanitiza por ultimo
    for t in (pulse.get('serp') or {}).get('top') or []:
        if isinstance(t, dict):
            for k in ('titulo', 'q'):
                if isinstance(t.get(k), str):
                    t[k] = _limpa(t[k])

    # escrita atomica: nunca deixa o JSON truncado se o processo morrer no meio
    tmp = PULSE_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(pulse, f, ensure_ascii=False, indent=1)
    os.replace(tmp, PULSE_FILE)
    print(f'ok: +{n} mencoes novas, {len(feed)} no feed, {pulse["feed_auto_negativas"]} negativas')


if __name__ == '__main__':
    main()
