"""
update-meta-ads.py (VOGE Brasil)
Busca o dia a dia da conta de anúncios VOGE BRASIL e grava voge/meta-data.json.
Roda no repositório do Festival porque as chaves das APIs já estão lá.

Lead do painel = envio de formulário (test ride e "tenho interesse", evento Lead)
               + contato (formulário de contato e clique no WhatsApp, evento Contact).
O evento CandidaturaConcessionario fica de fora: não é comprador.
"""
import json, os, urllib.request, urllib.parse
from datetime import datetime, timedelta, timezone

BRT      = timezone(timedelta(hours=-3))
TOKEN    = os.environ['META_TOKEN']
ACCT     = os.environ.get('META_ACCT', 'act_1397071468729208')
OUT_FILE = 'meta-data.json'
FIELDS   = 'date_start,spend,impressions,reach,clicks,actions'
since    = os.environ.get('META_SINCE', '2026-11-01')   # início da campanha VOGE Sales Speed
until    = datetime.now(BRT).strftime('%Y-%m-%d')

# O nome do evento muda conforme a origem. Vale o primeiro que existir, para não contar duas vezes.
TIPOS_LEAD    = ('lead', 'offsite_conversion.fb_pixel_lead')
TIPOS_CONTATO = ('contact_total', 'contact_website', 'contact', 'offsite_conversion.fb_pixel_contact')
TIPOS_VC      = ('view_content', 'omni_view_content', 'offsite_conversion.fb_pixel_view_content')


def contar(actions, tipos):
    valores = {a.get('action_type'): a.get('value', 0) for a in (actions or [])}
    for t in tipos:
        if t in valores:
            return int(float(valores[t]))
    return 0


query = {
    'fields':         FIELDS,
    'time_increment': '1',
    'time_range':     json.dumps({'since': since, 'until': until}),
    'level':          'account',
    'limit':          '90',
    'access_token':   TOKEN,
}
url = f'https://graph.facebook.com/v20.0/{ACCT}/insights?{urllib.parse.urlencode(query)}'

all_data = []
while url:
    with urllib.request.urlopen(url) as resp:
        result = json.loads(resp.read())
    all_data.extend(result.get('data', []))
    url = result.get('paging', {}).get('next')

parsed, tipos_vistos = [], set()
for day in all_data:
    actions = day.get('actions')
    tipos_vistos.update(a.get('action_type') for a in (actions or []))
    form, contatos = contar(actions, TIPOS_LEAD), contar(actions, TIPOS_CONTATO)
    leads = form + contatos
    gasto = round(float(day.get('spend', 0)), 2)
    iso = day['date_start']
    parsed.append({
        'data':        f'{iso[8:10]}/{iso[5:7]}',
        'dataISO':     iso,
        'leads':       leads,
        'leadsForm':   form,
        'contatos':    contatos,
        'gasto':       gasto,
        'cpl':         round(gasto / leads, 2) if leads else None,
        'impressoes':  int(day.get('impressions', 0)),
        'alcance':     int(day.get('reach', 0)),
        'cliques':     int(day.get('clicks', 0)),
        'lpViews':     contar(actions, ('landing_page_view',)),
        'viewContent': contar(actions, TIPOS_VC),
    })

output = {
    'updated': datetime.now(BRT).strftime('%d/%m/%Y %H:%M'),
    'account': 'VOGE Brasil | ' + ACCT,
    'since':   since,
    'data':    parsed,
}
with open(OUT_FILE, 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"OK: {len(parsed)} dias desde {since} | leads {sum(d['leads'] for d in parsed)} | gasto R$ {sum(d['gasto'] for d in parsed):,.2f}")
print('Tipos de acao vistos:', sorted(t for t in tipos_vistos if t))
