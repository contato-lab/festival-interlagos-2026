#!/usr/bin/env python3
"""
Plataforma Própria API → vendas-data.json
Festival Interlagos 2026 — Agência Lime

Busca vendas das plataformas ingressosmoto e ingressosauto,
agrega por dia de venda (status=3 = confirmada) e salva
como vendas-data.json para consumo pelo dashboard (GitHub Pages).

Roda via GitHub Actions a cada 5 minutos.
"""

import json
import sys
from datetime import date, datetime, timedelta, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# ─── CONFIGURAÇÃO ─────────────────────────────────────────
API_MOTO_BASE    = "https://ingressosmoto.festivalinterlagos.com.br"
API_AUTO_BASE    = "https://ingressosauto.festivalinterlagos.com.br"
DATA_INICIO      = date(2026, 3, 31)   # D+1 = abertura das vendas
DATA_INICIO_STR  = "2026-01-01"        # data_inicio na query (busca tudo)
PAGE_SIZE        = 100
OUTPUT_FILE      = "vendas-data.json"

# Headers para simular requisição do browser (mesmo origin que a plataforma)
def _headers(base, token=None):
    h = {
        "Accept":          "application/json",
        "Content-Type":    "application/json",
        "Origin":          base,
        "Referer":         base + "/",
        "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

# ─── FUNÇÕES ──────────────────────────────────────────────

def get_token(base):
    """Obtém Bearer token do endpoint /apis/token."""
    url = base + "/apis/token"
    req = Request(url, headers=_headers(base), method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except HTTPError as e:
        raise RuntimeError(f"Token HTTP {e.code} para {base}") from e
    except URLError as e:
        raise RuntimeError(f"Token URLError para {base}: {e.reason}") from e
    if data.get("status") != "success":
        raise RuntimeError(f"Token status inválido: {data.get('status')}")
    return data["token"]


def get_vendas_all(base, token):
    """Busca todas as vendas com paginação completa."""
    fim  = datetime.now().strftime("%Y-%m-%d")
    sales = []
    page  = 1
    while True:
        url = (
            f"{base}/apis/vendas"
            f"?data_inicio={DATA_INICIO_STR}"
            f"&data_fim={fim}"
            f"&page_size={PAGE_SIZE}"
            f"&page={page}"
        )
        req = Request(url, headers=_headers(base, token), method="GET")
        try:
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
        except HTTPError as e:
            raise RuntimeError(f"Vendas HTTP {e.code} para {base} página {page}") from e
        except URLError as e:
            raise RuntimeError(f"Vendas URLError para {base}: {e.reason}") from e

        if data.get("status") != "success" or not data.get("data"):
            break
        sales.extend(data["data"])
        print(f"    página {page}: {len(data['data'])} vendas", flush=True)
        if data.get("pagination", {}).get("isNextPage") != "Y":
            break
        page += 1
    return sales


def vendas_por_dia(sales, tipo):
    """
    Agrega vendas por dia (d = dias desde DATA_INICIO + 1).
    Filtra apenas status=3 (confirmado).
    Retorna dict: {d: {"receita": float, "qtd": int, "date": str}}
    """
    by_day = {}
    for v in sales:
        if str(v.get("venda_status")) != "3":
            continue
        ds = (v.get("created_at") or "").split(" ")[0]
        if not ds or len(ds) < 10:
            continue
        try:
            dt = date.fromisoformat(ds)
        except ValueError:
            continue
        d = (dt - DATA_INICIO).days + 1
        if d < 1:
            continue
        if d not in by_day:
            by_day[d] = {"receita": 0.0, "qtd": 0, "date": ds}
        by_day[d]["receita"] += float(v.get("venda_valor") or 0)
        by_day[d]["qtd"]     += len(v.get("qrcodes") or [])
    return by_day


def main():
    print("=== update-vendas-proprias.py ===")
    print(f"Início: {datetime.now().isoformat()}")

    # Tokens
    print("\n[1/4] Obtendo tokens...")
    moto_token = get_token(API_MOTO_BASE)
    auto_token = get_token(API_AUTO_BASE)
    print("  Tokens OK")

    # Vendas Moto
    print(f"\n[2/4] Buscando vendas Moto ({API_MOTO_BASE})...")
    moto_sales = get_vendas_all(API_MOTO_BASE, moto_token)
    print(f"  Total: {len(moto_sales)} registros Moto")

    # Vendas Auto
    print(f"\n[3/4] Buscando vendas Auto ({API_AUTO_BASE})...")
    auto_sales = get_vendas_all(API_AUTO_BASE, auto_token)
    print(f"  Total: {len(auto_sales)} registros Auto")

    # Agregação
    print("\n[4/4] Agregando por dia...")
    moto_by_day = vendas_por_dia(moto_sales, "moto")
    auto_by_day = vendas_por_dia(auto_sales, "auto")

    all_days = sorted(set(list(moto_by_day.keys()) + list(auto_by_day.keys())))

    daily  = []
    totals = {"moto_receita": 0.0, "auto_receita": 0.0,
              "moto_ingressos": 0,  "auto_ingressos": 0}

    for d in all_days:
        m        = moto_by_day.get(d, {"receita": 0.0, "qtd": 0})
        a        = auto_by_day.get(d, {"receita": 0.0, "qtd": 0})
        day_date = (DATA_INICIO + timedelta(days=d - 1)).isoformat()
        daily.append({
            "d":              d,
            "date":           day_date,
            "moto_receita":   round(m["receita"], 2),
            "moto_ingressos": m["qtd"],
            "auto_receita":   round(a["receita"], 2),
            "auto_ingressos": a["qtd"],
        })
        totals["moto_receita"]   += m["receita"]
        totals["moto_ingressos"] += m["qtd"]
        totals["auto_receita"]   += a["receita"]
        totals["auto_ingressos"] += a["qtd"]

    totals["moto_receita"]   = round(totals["moto_receita"],  2)
    totals["auto_receita"]   = round(totals["auto_receita"],  2)
    totals["total_receita"]  = round(totals["moto_receita"] + totals["auto_receita"], 2)
    totals["total_ingressos"] = totals["moto_ingressos"] + totals["auto_ingressos"]

    output = {
        "updated_at":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":          "Plataforma própria (ingressosmoto/ingressosauto.festivalinterlagos.com.br)",
        "campaign_start":  DATA_INICIO.isoformat(),
        "totals":          totals,
        "daily":           daily,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nOK  {OUTPUT_FILE} salvo com sucesso!")
    print(f"    Receita Moto:   R$ {totals['moto_receita']:>12,.2f}  ({totals['moto_ingressos']} ingressos)")
    print(f"    Receita Auto:   R$ {totals['auto_receita']:>12,.2f}  ({totals['auto_ingressos']} ingressos)")
    print(f"    Receita Total:  R$ {totals['total_receita']:>12,.2f}  ({totals['total_ingressos']} ingressos)")
    print(f"    Dias com dados: {len(daily)}")
    print(f"Fim: {datetime.now().isoformat()}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERRO: {e}", file=sys.stderr)
        sys.exit(1)
