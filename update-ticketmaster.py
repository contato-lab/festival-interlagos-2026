#!/usr/bin/env python3
"""
Ticketmaster API → ticketmaster-data.json
Festival Interlagos 2026
Agência Lime

Busca todos os movimentos desde o início da campanha (2026-03-25),
separa por Edição Moto e Edição Auto, agrega por data de compra.
"""

import json
import os
import sys
import requests
from datetime import datetime, timezone
from collections import defaultdict

# ─── CONFIGURAÇÃO ─────────────────────────────────────────
API_BASE      = "https://data.getcrowder.com"
API_ENDPOINT  = "/activity/organizer"
API_KEY       = os.environ.get("TM_API_KEY", "52087b883f65e8d2a684ee680c5beca66e425fdcc46c2b653da0fffd50088734")

# Timestamp de início da campanha: 2026-03-25 00:00:00 UTC em milissegundos
CAMPAIGN_START_MS = 1774396800000

# Show IDs por edição (Festival Interlagos 2026)
MOTO_SHOW_IDS = {195330, 195736, 195737, 195738}
AUTO_SHOW_IDS = {195331, 195739, 195740, 195741}

OUTPUT_FILE = "ticketmaster-data.json"

# ─── FUNÇÕES ──────────────────────────────────────────────

def fetch_all_movements():
    """Busca todos os movimentos com paginação completa."""
    all_movements = []
    last_update      = CAMPAIGN_START_MS
    last_movement_id = 1
    page = 0

    print(f"Iniciando busca a partir de lastUpdate={last_update}...")

    while True:
        page += 1
        url = f"{API_BASE}{API_ENDPOINT}?lastUpdate={last_update}&lastMovementId={last_movement_id}"

        try:
            r = requests.get(url, headers={
                "apiKey": API_KEY,
                "Content-Type": "application/json"
            }, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"Erro na página {page}: {e}", file=sys.stderr)
            break

        data = r.json()
        movements = data.get("movements", [])
        has_more  = data.get("hasMore", False)

        all_movements.extend(movements)
        print(f"  Página {page}: {len(movements)} movimentos | hasMore={has_more}")

        if not has_more or not movements:
            break

        # Atualiza cursores para próxima página
        last_update      = data.get("lastUpdate", last_update)
        last_movement_id = data.get("lastMovementId", last_movement_id)

    print(f"Total de movimentos: {len(all_movements)}")
    return all_movements


def classify_show(tickets):
    """Retorna 'moto', 'auto' ou None conforme os show IDs dos tickets."""
    for t in tickets:
        show_id = t.get("show", {}).get("id")
        if show_id in MOTO_SHOW_IDS:
            return "moto"
        if show_id in AUTO_SHOW_IDS:
            return "auto"
    return None


def aggregate(movements):
    """Agrega movimentos por data de compra, separado por Moto e Auto."""
    # daily[date_str] = {moto_receita, auto_receita, moto_ingressos, auto_ingressos}
    daily = defaultdict(lambda: {
        "moto_receita": 0.0,
        "auto_receita": 0.0,
        "moto_ingressos": 0,
        "auto_ingressos": 0
    })

    totals = {
        "moto_receita":   0.0,
        "auto_receita":   0.0,
        "moto_ingressos": 0,
        "auto_ingressos": 0,
        "total_receita":  0.0,
        "total_ingressos": 0
    }

    for mv in movements:
        date_str = mv.get("date", "")[:10]   # "2026-04-01"
        amount   = float(mv.get("amount", 0))
        tc       = int(mv.get("ticketCount", 0))
        edition  = classify_show(mv.get("tickets", []))

        if edition == "moto":
            daily[date_str]["moto_receita"]   += amount
            daily[date_str]["moto_ingressos"] += tc
            totals["moto_receita"]            += amount
            totals["moto_ingressos"]          += tc
        elif edition == "auto":
            daily[date_str]["auto_receita"]   += amount
            daily[date_str]["auto_ingressos"] += tc
            totals["auto_receita"]            += amount
            totals["auto_ingressos"]          += tc

    totals["total_receita"]   = totals["moto_receita"] + totals["auto_receita"]
    totals["total_ingressos"] = totals["moto_ingressos"] + totals["auto_ingressos"]

    # Arredonda
    for k in totals:
        totals[k] = round(totals[k], 2)

    # Converte daily para lista ordenada
    daily_list = []
    for date_str in sorted(daily.keys()):
        d = daily[date_str]
        daily_list.append({
            "date":           date_str,
            "moto_receita":   round(d["moto_receita"], 2),
            "auto_receita":   round(d["auto_receita"], 2),
            "moto_ingressos": d["moto_ingressos"],
            "auto_ingressos": d["auto_ingressos"]
        })

    return totals, daily_list


def main():
    movements = fetch_all_movements()

    if not movements:
        print("Nenhum movimento encontrado. Abortando.", file=sys.stderr)
        sys.exit(1)

    totals, daily = aggregate(movements)

    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "Ticketmaster API via getcrowder.com",
        "campaign_start": "2026-03-25",
        "moto_show_ids": sorted(MOTO_SHOW_IDS),
        "auto_show_ids": sorted(AUTO_SHOW_IDS),
        "totals": totals,
        "daily": daily
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ {OUTPUT_FILE} gerado com sucesso!")
    print(f"   Receita Moto:  R$ {totals['moto_receita']:,.2f} ({totals['moto_ingressos']} ingressos)")
    print(f"   Receita Auto:  R$ {totals['auto_receita']:,.2f} ({totals['auto_ingressos']} ingressos)")
    print(f"   Receita Total: R$ {totals['total_receita']:,.2f} ({totals['total_ingressos']} ingressos)")
    print(f"   Dias com dados: {len(daily)}")


if __name__ == "__main__":
    main()
