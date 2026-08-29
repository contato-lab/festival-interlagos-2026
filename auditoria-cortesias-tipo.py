#!/usr/bin/env python3
"""
Festival Interlagos 2026 — CORTESIA POR TIPO DE INGRESSO (Ticketmaster).

POR QUE ESTE ARQUIVO EXISTE:
o update-vendas-tipos.py descarta cortesia de proposito ("Cortesia fora dos
Tipos (so venda)"), e o ticketmaster-data.json conta cortesia mas SEM tipo de
ingresso. Resultado: o sistema sabe quantas cortesias sairam por dia e sabe
quantos VIP Lounge foram vendidos por dia de evento, mas nunca cruza os dois.
A pergunta "quantas cortesias de VIP Lounge para hoje" nao tinha resposta.

O dado sempre esteve na API: o mesmo movimento traz o valor (que identifica a
cortesia) e os tickets (que identificam tipo e dia do evento). Quem separou os
dois foi a nossa regra, nao a Ticketmaster. Este script inverte a regra: fica
SO com a cortesia e mantem o tipo.

NAO ESCREVE em vendas-tipos-data.json nem em ticketmaster-data.json. Grava
auditoria-cortesias.json, relatorio de leitura, pra nao mexer em numero que o
painel ja mostra no meio do evento.

Responde nos DOIS recortes, porque sao perguntas diferentes:
  por_dia_evento  -> cortesia VALIDA para aquele dia   (o que a portaria quer)
  por_dia_emissao -> cortesia EMITIDA naquele dia      (o que o financeiro quer)
"""

import os, sys, json, urllib.request
from datetime import datetime, timezone
from collections import defaultdict

TM_API_KEY = os.environ.get('TM_API_KEY', '')
CAMPAIGN_START_MS = 1774396800000          # 2026-03-25, mesmo marco do coletor
MOTO_SHOW_IDS = {195330, 195736, 195737, 195738}
AUTO_SHOW_IDS = {195331, 195739, 195740, 195741}
SHOW_TO_DATE = {
    195330: '13/08/2026', 195736: '14/08/2026', 195737: '15/08/2026', 195738: '16/08/2026',
    195331: '27/08/2026', 195739: '28/08/2026', 195740: '29/08/2026', 195741: '30/08/2026',
}


def fetch_movements():
    """Mesma paginacao do coletor de producao. Parar na primeira pagina
    subestimaria a contagem, que e justamente o erro que isto vem corrigir."""
    todos, last_update, last_mov, paginas = [], CAMPAIGN_START_MS, 1, 0
    while paginas < 200:
        url = (f'https://data.getcrowder.com/activity/organizer'
               f'?lastUpdate={last_update}&lastMovementId={last_mov}')
        req = urllib.request.Request(url, headers={'apiKey': TM_API_KEY})
        with urllib.request.urlopen(req, timeout=60) as r:
            d = json.loads(r.read())
        movs = d.get('movements', [])
        todos.extend(movs)
        paginas += 1
        if not d.get('hasMore') or not movs:
            break
        last_update = d.get('lastUpdate', last_update)
        last_mov    = d.get('lastMovementId', last_mov)
    return todos, paginas


def nome_do_produto(m):
    prod = (m.get('product') or {}).get('name', '')
    if not prod:
        for t in m.get('tickets', []):
            prod = (t.get('sector') or {}).get('name', '')
            if prod:
                break
    return prod or 'Desconhecido'


def main():
    if not TM_API_KEY:
        print('TM_API_KEY nao definida'); sys.exit(1)

    movs, paginas = fetch_movements()
    print(f'{len(movs)} movimentos lidos em {paginas} pagina(s)', flush=True)

    # Compras que NASCERAM cortesia: o cancelamento de uma cortesia nao traz
    # valor zero, entao sem este mapa o cancelamento nao seria reconhecido e a
    # contagem ficaria alta.
    nasceu_cortesia = set()
    for m in movs:
        if m.get('operation') == 'ISSUANCE':
            pid = (m.get('purchase') or {}).get('id')
            if pid is not None and float(m.get('amount', 0)) == 0 and int(m.get('ticketCount', 0)) > 0:
                nasceu_cortesia.add(pid)

    ed_evento = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # edicao > dia evento > tipo
    ed_emissao = defaultdict(lambda: defaultdict(lambda: defaultdict(int))) # edicao > dia emissao > tipo
    total_por_edicao = defaultdict(int)
    cancelados = 0

    for m in movs:
        edi = None
        for t in m.get('tickets', []):
            sid = (t.get('show') or {}).get('id')
            if sid in MOTO_SHOW_IDS: edi = 'moto'; break
            if sid in AUTO_SHOW_IDS: edi = 'auto'; break
        if not edi:
            continue

        op  = m.get('operation')
        amt = float(m.get('amount', 0))
        qtd = int(m.get('ticketCount', 0))
        pid = (m.get('purchase') or {}).get('id')

        if op == 'ISSUANCE':
            if not (amt == 0 and qtd > 0):
                continue           # venda, nao interessa aqui
            sinal = 1
        elif op in ('CANCELLATION', 'REFUND'):
            if pid not in nasceu_cortesia:
                continue           # cancelamento de venda, nao interessa
            sinal = -1
            cancelados += qtd
        else:
            continue

        tipo = nome_do_produto(m)
        dia_ev = 'Sem data'
        for t in m.get('tickets', []):
            sid = (t.get('show') or {}).get('id')
            if sid in SHOW_TO_DATE:
                dia_ev = SHOW_TO_DATE[sid]; break
        dia_em = (m.get('date') or '')[:10] or 'Sem data'

        ed_evento[edi][dia_ev][tipo]  += sinal * qtd
        ed_emissao[edi][dia_em][tipo] += sinal * qtd
        total_por_edicao[edi]         += sinal * qtd

    def limpa(d):
        return {ed: {dia: {t: q for t, q in sorted(tipos.items()) if q != 0}
                     for dia, tipos in sorted(dias.items())}
                for ed, dias in d.items()}

    saida = {
        'gerado_em': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fonte': 'Ticketmaster API (data.getcrowder.com), movimentos brutos',
        'observacao': ('Relatorio de leitura. NAO altera vendas-tipos-data.json nem '
                       'ticketmaster-data.json. Cortesia = emissao com valor R$ 0. '
                       'Cancelamento de cortesia entra como negativo.'),
        'movimentos_lidos': len(movs),
        'cortesias_canceladas': cancelados,
        'total_por_edicao': dict(total_por_edicao),
        'por_dia_evento':  limpa(ed_evento),
        'por_dia_emissao': limpa(ed_emissao),
    }
    with open('auditoria-cortesias.json', 'w', encoding='utf-8') as f:
        json.dump(saida, f, ensure_ascii=False, indent=2)

    print()
    print('===== CORTESIAS POR DIA DE EVENTO =====')
    for edi in ('auto', 'moto'):
        if edi not in ed_evento: continue
        print(f'-- edicao {edi}')
        for dia, tipos in sorted(saida['por_dia_evento'][edi].items()):
            soma = sum(tipos.values())
            print(f'   {dia}   total {soma}')
            for t, q in sorted(tipos.items(), key=lambda x: -x[1]):
                print(f'       {q:6d}  {t}')
    print()
    print('total por edicao:', dict(total_por_edicao))
    print('auditoria-cortesias.json gerado')


if __name__ == '__main__':
    main()
