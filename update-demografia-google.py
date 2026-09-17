"""
update-demografia-google.py
Perfil de idade e genero declarado no Google Ads, periodo cheio de 2026.
Gera demografia-google.json.

Serve de conferencia cruzada do Meta. Campanhas PMax e Demand Gen nao
publicam esses recortes, entao o total daqui e menor que o total da conta.
"""

import json
import os
import sys
from datetime import datetime, timezone

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

DEVELOPER_TOKEN   = os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"]
CLIENT_ID         = os.environ["GOOGLE_ADS_CLIENT_ID"]
CLIENT_SECRET     = os.environ["GOOGLE_ADS_CLIENT_SECRET"]
REFRESH_TOKEN     = os.environ["GOOGLE_ADS_REFRESH_TOKEN"]
CUSTOMER_ID       = os.environ["GOOGLE_ADS_CUSTOMER_ID"]
LOGIN_CUSTOMER_ID = os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"]

OUTPUT_FILE = "demografia-google.json"
INICIO = os.environ.get("DEMO_SINCE", "2026-03-30")
FIM    = os.environ.get("DEMO_UNTIL", datetime.now(timezone.utc).strftime("%Y-%m-%d"))


def get_client():
    return GoogleAdsClient.load_from_dict({
        "developer_token": DEVELOPER_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "login_customer_id": LOGIN_CUSTOMER_ID,
        "use_proto_plus": True,
    })


def recorte(client, recurso, campo):
    query = f"""
        SELECT
            {campo},
            campaign.advertising_channel_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions
        FROM {recurso}
        WHERE segments.date BETWEEN '{INICIO}' AND '{FIM}'
    """
    service = client.get_service("GoogleAdsService")
    acumulado = {}
    try:
        for batch in service.search_stream(customer_id=CUSTOMER_ID, query=query):
            for row in batch.results:
                obj = row.ad_group_criterion
                for parte in campo.split(".")[1:]:
                    obj = getattr(obj, parte)
                chave = obj.name if hasattr(obj, "name") else str(obj)
                d = acumulado.setdefault(chave, {
                    "chave": chave,
                    "impressoes": 0,
                    "cliques": 0,
                    "investimento": 0.0,
                    "conversoes": 0.0,
                })
                d["impressoes"]   += row.metrics.impressions
                d["cliques"]      += row.metrics.clicks
                d["investimento"] += row.metrics.cost_micros / 1_000_000
                d["conversoes"]   += row.metrics.conversions
    except GoogleAdsException as e:
        print(f"ERRO em {recurso}: {e}", file=sys.stderr)
        return {"total_impressoes": 0, "itens": [], "erro": str(e.error.code().name)}

    itens = sorted(acumulado.values(), key=lambda x: -x["impressoes"])
    total = sum(i["impressoes"] for i in itens)
    for i in itens:
        i["pct_impressoes"] = round(i["impressoes"] / total * 100, 2) if total else 0
        i["ctr"] = round(i["cliques"] / i["impressoes"] * 100, 2) if i["impressoes"] else 0
    return {"total_impressoes": total, "itens": itens}


def main():
    client = get_client()
    dados = {
        "atualizado_em": datetime.now(timezone.utc).isoformat(),
        "periodo": {"inicio": INICIO, "fim": FIM},
        "observacao": "PMax e Demand Gen nao publicam idade e genero, o total aqui e parcial",
        "recortes": {
            "idade":  recorte(client, "age_range_view", "ad_group_criterion.age_range.type"),
            "genero": recorte(client, "gender_view",    "ad_group_criterion.gender.type"),
        },
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    print(f"gravado {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
