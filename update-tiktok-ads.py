#!/usr/bin/env python3
"""
TikTok Ads API → tiktok-data.json
Festival Interlagos 2026 — Agência Lime

Busca dados de investimento, impressões, cliques e compras
da conta TikTok Ads, agrega por data desde o início da campanha.

Variáveis de ambiente necessárias (GitHub Secrets):
  TT_ACCESS_TOKEN  — Long-term access token do TikTok for Business
  TT_ADVERTISER_ID — ID da conta de anúncios (Advertiser ID)
"""

import json
import os
import sys
import requests
from datetime import datetime, timezone, timedelta

# ─── CONFIGURAÇÃO ─────────────────────────────────────────
API_BASE      = "https://business-api.tiktok.com/open_api/v1.3"
ACCESS_TOKEN  = os.environ.get("TT_ACCESS_TOKEN", "")
ADVERTISER_ID = os.environ.get("TT_ADVERTISER_ID", "")
CAMPAIGN_START = "2026-03-25"   # início da campanha
OUTPUT_FILE    = "tiktok-data.json"

# Métricas que queremos
METRICS = [
    "spend",
    "impressions",
    "clicks",
    "purchase",
    "cost_per_purchase",
    "ctr",
    "cpm",
]


def fetch_daily_report() -> list:
    """Busca relatório diário de nível de advertiser."""
    if not ACCESS_TOKEN or not ADVERTISER_ID:
        print("ERRO: TT_ACCESS_TOKEN e TT_ADVERTISER_ID são obrigatórios.", file=sys.stderr)
        sys.exit(1)

    end_date   = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = CAMPAIGN_START

    url = f"{API_BASE}/report/integrated/get/"
    params = {
        "advertiser_id": ADVERTISER_ID,
        "report_type":   "BASIC",
        "data_level":    "AUCTION_ADVERTISER",
        "dimensions":    json.dumps(["stat_time_day"]),
        "metrics":       json.dumps(METRICS),
        "start_date":    start_date,
        "end_date":      end_date,
        "page_size":     1000,
        "page":          1,
    }

    all_rows = []
    page = 0

    while True:
        page += 1
        params["page"] = page

        try:
            r = requests.get(url, headers={"Access-Token": ACCESS_TOKEN}, params=params, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"Erro na página {page}: {e}", file=sys.stderr)
            break

        body = r.json()
        code = body.get("code", -1)

        if code != 0:
            print(f"TikTok API erro code={code}: {body.get('message','')}", file=sys.stderr)
            sys.exit(1)

        rows      = body.get("data", {}).get("list", [])
        page_info = body.get("data", {}).get("page_info", {})
        total_pages = page_info.get("total_page", 1)

        all_rows.extend(rows)
        print(f"  Página {page}/{total_pages}: {len(rows)} linhas")

        if page >= total_pages or not rows:
            break

    print(f"Total de linhas: {len(all_rows)}")
    return all_rows


def aggregate(rows: list) -> tuple:
    """Agrega por data e calcula totais."""
    daily = {}

    for row in rows:
        dims    = row.get("dimensions", {})
        metrics = row.get("metrics", {})

        date_str   = dims.get("stat_time_day", "")[:10]  # "2026-04-01"
        spend      = float(metrics.get("spend", 0))
        impressions = int(float(metrics.get("impressions", 0)))
        clicks     = int(float(metrics.get("clicks", 0)))
        purchases  = float(metrics.get("purchase", 0))

        if date_str not in daily:
            daily[date_str] = {
                "date":        date_str,
                "cost":        0.0,
                "impressions": 0,
                "clicks":      0,
                "purchases":   0.0,
            }

        daily[date_str]["cost"]        += spend
        daily[date_str]["impressions"] += impressions
        daily[date_str]["clicks"]      += clicks
        daily[date_str]["purchases"]   += purchases

    # Arredonda valores
    for d in daily.values():
        d["cost"]      = round(d["cost"], 2)
        d["purchases"] = round(d["purchases"])

    daily_list = sorted(daily.values(), key=lambda x: x["date"])

    totals = {
        "cost":        round(sum(d["cost"]        for d in daily_list), 2),
        "impressions": sum(d["impressions"] for d in daily_list),
        "clicks":      sum(d["clicks"]      for d in daily_list),
        "purchases":   round(sum(d["purchases"]   for d in daily_list)),
    }
    if totals["purchases"] > 0:
        totals["cost_per_purchase"] = round(totals["cost"] / totals["purchases"], 2)
    else:
        totals["cost_per_purchase"] = 0.0

    return totals, daily_list


def main():
    rows = fetch_daily_report()

    if not rows:
        print("Nenhuma linha retornada. Abortando.", file=sys.stderr)
        sys.exit(1)

    totals, daily = aggregate(rows)

    output = {
        "updated_at":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":         "TikTok Ads API v1.3",
        "campaign_start": CAMPAIGN_START,
        "advertiser_id":  ADVERTISER_ID,
        "totals":         totals,
        "daily_series":   daily,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nOK: {OUTPUT_FILE} gerado com sucesso!")
    print(f"   Investimento: R$ {totals['cost']:,.2f}")
    print(f"   Impressoes:   {totals['impressions']:,}")
    print(f"   Cliques:      {totals['clicks']:,}")
    print(f"   Compras:      {totals['purchases']:,}")
    print(f"   Dias com dados: {len(daily)}")


if __name__ == "__main__":
    main()
