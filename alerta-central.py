#!/usr/bin/env python3
"""
Avisa no Telegram quando entra coisa nova na Central do Evento.

Le o Firestore pela API REST publica (as regras do projeto ja permitem leitura
sem login, igual o proprio site faz) e compara com o ultimo estado conhecido,
guardado em alerta-central-state.json neste mesmo repo.

Manda UM resumo por rodada, nao uma mensagem por item: durante o evento entram
varios itens juntos e uma mensagem por item faria a equipe silenciar o grupo no
primeiro dia. Grupo silenciado nao avisa ninguem.

Precisa de TELEGRAM_TOKEN e TELEGRAM_CHAT_ID (ja existem no repo).
Roda via GitHub Actions.
"""
import json, os, sys, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta

HERE       = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, 'alerta-central-state.json')

PROJECT = 'central-evento-fi'
# Mesma chave que esta no HTML publico da Central. Nao e segredo: da acesso ao
# que as regras do Firestore ja liberam pra qualquer visitante do site.
API_KEY = os.environ.get('FIREBASE_WEB_KEY', 'AIzaSyD1spoy847dEtccSGJflKtG4-EYZMe23OQ')
BASE    = f'https://firestore.googleapis.com/v1/projects/{PROJECT}/databases/(default)/documents'

TELEGRAM_TOKEN   = os.environ.get('TELEGRAM_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

CENTRAL_URL  = 'https://central.festivalinterlagos.com.br/'
CONTEUDO_URL = 'https://central.festivalinterlagos.com.br/conteudo/'
BRT = timezone(timedelta(hours=-3))


def _get(url):
    req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def val(v):
    """Desembrulha o formato do Firestore REST ({'stringValue': 'x'}) pro valor cru."""
    if not isinstance(v, dict):
        return v
    for k in ('stringValue', 'booleanValue'):
        if k in v:
            return v[k]
    if 'integerValue' in v:
        return int(v['integerValue'])
    if 'doubleValue' in v:
        return float(v['doubleValue'])
    if 'nullValue' in v:
        return None
    if 'mapValue' in v:
        return {k: val(x) for k, x in (v['mapValue'].get('fields') or {}).items()}
    if 'arrayValue' in v:
        return [val(x) for x in (v['arrayValue'].get('values') or [])]
    return None


def ler_doc(caminho):
    """Documento unico que guarda uma lista em 'items' (os murais)."""
    try:
        d = _get(f'{BASE}/{caminho}?key={API_KEY}')
    except Exception as e:
        print(f'[warn] {caminho}: {e}', file=sys.stderr)
        return []
    return val((d.get('fields') or {}).get('items')) or []


def ler_colecao(nome):
    """Colecao com um documento por item, paginada."""
    itens, token = [], None
    for _ in range(20):                       # teto de paginas: 20 x 300 = 6000
        url = f'{BASE}/{nome}?key={API_KEY}&pageSize=300'
        if token:
            url += '&pageToken=' + urllib.parse.quote(token)
        try:
            d = _get(url)
        except Exception as e:
            print(f'[warn] {nome}: {e}', file=sys.stderr)
            break
        for doc in (d.get('documents') or []):
            o = {k: val(v) for k, v in (doc.get('fields') or {}).items()}
            o['_id'] = doc['name'].rsplit('/', 1)[-1]
            itens.append(o)
        token = d.get('nextPageToken')
        if not token:
            break
    return itens


def carimbo(item):
    """Momento do item. Os murais usam 'ts', o modulo de conteudo usa 'criadoEm'."""
    for campo in ('ts', 'criadoEm', 'em'):
        v = item.get(campo)
        try:
            n = int(v)
            if n > 0:
                return n
        except (TypeError, ValueError):
            continue
    return 0


def novos(itens, desde):
    return [i for i in itens if carimbo(i) > desde]


def maior_carimbo(itens, atual):
    for i in itens:
        c = carimbo(i)
        if c > atual:
            atual = c
    return atual


def escapar(s):
    return (str(s or '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'))


def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print('[warn] TELEGRAM_TOKEN ou TELEGRAM_CHAT_ID nao configurados', file=sys.stderr)
        return False
    url  = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    body = json.dumps({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': msg,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True,
    }).encode('utf-8')
    req = urllib.request.Request(url, data=body, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=20):
            return True
    except Exception as e:
        print(f'[erro] telegram: {e}', file=sys.stderr)
        return False


def main():
    teste = os.environ.get('MODO_TESTE', '').lower() in ('1', 'true', 'sim')

    try:
        with open(STATE_FILE, encoding='utf-8') as f:
            state = json.load(f)
    except Exception:
        state = {}

    # ── o que a gente vigia ────────────────────────────────────────────────
    fontes = [
        # (chave de estado, rotulo, leitor, nome do item, url)
        ('avisos',   'Avisos',                lambda: ler_doc('board/avisos'),  'tema',   CENTRAL_URL),
        ('mural',    'Mural do Evento',       lambda: ler_doc('board/mural'),   'titulo', CENTRAL_URL),
        ('interno',  'Mural interno',         lambda: ler_doc('board/interno'), 'titulo', CENTRAL_URL),
        ('story',    'checklist do StoryMaker', lambda: ler_colecao('cont_story'), 'grupo', CONTEUDO_URL),
        ('pecas',    'peças de conteúdo',     lambda: ler_colecao('cont_pecas'), 'marca', CONTEUDO_URL),
    ]

    linhas, urls, novo_state = [], set(), dict(state)
    primeira_vez = not state

    for chave, rotulo, ler, campo, url in fontes:
        itens = ler()
        desde = int(state.get(chave, 0))
        novo_state[chave] = maior_carimbo(itens, desde)

        # Primeira execucao: so grava o marco e nao avisa nada. Sem isso, o
        # primeiro disparo mandaria o historico inteiro de uma vez.
        if primeira_vez:
            continue

        n = novos(itens, desde)
        if not n:
            continue

        urls.add(url)
        # ate 3 exemplos por fonte: o suficiente pra saber do que se trata sem
        # transformar o alerta num despejo de conteudo
        nomes = []
        for i in sorted(n, key=carimbo, reverse=True)[:3]:
            t = str(i.get(campo) or '').strip()
            if t:
                nomes.append(escapar(t[:60]))
        detalhe = (': ' + ', '.join(nomes)) if nomes else ''
        resto = f' e mais {len(n) - len(nomes)}' if len(n) > len(nomes) else ''
        linhas.append(f'• <b>{len(n)}</b> em {escapar(rotulo)}{detalhe}{resto}')

    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(novo_state, f, ensure_ascii=False, indent=2, sort_keys=True)

    # O teste vem ANTES da saida da primeira execucao de proposito: na estreia
    # o script so grava o marco e nao alerta nada, e quem acabou de configurar
    # ficaria sem nenhum sinal de que o canal funciona. Sem confirmacao, a
    # pessoa fica sem saber se o silencio e "nao ha novidade" ou "esta quebrado".
    if teste:
        send_telegram('🔧 <b>Teste do alerta da Central</b>\n\nO robô está de pé e '
                      'ligado neste grupo. Quando entrar publicação nova na Central '
                      'ou no módulo de Conteúdo, o aviso chega aqui.')

    if primeira_vez:
        print('primeira execucao: marco gravado, nada enviado')
        return

    if not linhas:
        print('nada novo')
        return

    agora = datetime.now(BRT).strftime('%H:%M')
    corpo = '\n'.join(linhas)
    link  = '\n'.join(f'<a href="{u}">{u}</a>' for u in sorted(urls))
    msg = (f'📣 <b>Novidade na Central do Evento</b>  <i>{agora}</i>\n\n'
           f'{corpo}\n\n{link}')
    ok = send_telegram(msg)
    print(('enviado: ' if ok else 'FALHOU: ') + ' | '.join(linhas))


if __name__ == '__main__':
    main()
