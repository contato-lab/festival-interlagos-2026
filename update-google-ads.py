"""
update-google-ads.py
Busca dados de campanhas do Google Ads e grava google-ads-data.json.
Roda via GitHub Actions a cada 30 minutos.
"""

import json
import os
from datetime import datetime, timedelta, timezone

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# ── Configurações via GitHub Secrets ─────────────────────────────────────────
DEVELOPER_TOKEN     = os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"]
CLIENT_ID           = os.environ["GOOGLE_ADS_CLIENT_ID"]
CLIENT_SECRET       = os.environ["GOOGLE_ADS_CLIENT_SECRET"]
REFRESH_TOKEN       = os.environ["GOOGLE_ADS_REFRESH_TOKEN"]
CUSTOMER_ID         = os.environ["GOOGLE_ADS_CUSTOMER_ID"]          # 5879952911
LOGIN_CUSTOMER_ID   = os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"]    # 4736269396 (MCC)

OUTPUT_FILE = "google-ads-data.json"
LOOKBACK_DAYS = 90  # últimos 90 dias de histórico


def get_client():
    config = {
        "developer_token": DEVELOPER_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "login_customer_id": LOGIN_CUSTOMER_ID,
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


def fetch_campaign_data(client):
    ga_service = client.get_service("GoogleAdsService")

    today = datetime.now(timezone.utc).date()
    start_date = today - timedelta(days=LOOKBACK_DAYS)

    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.ctr,
            metrics.average_cpc
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{today}'
            AND campaign.status != 'REMOVED'
            AND campaign.name LIKE '%FESTIVAL INTERLAGOS%'
        ORDER BY segments.date DESC
    """

    response = ga_service.search_stream(
        customer_id=CUSTOMER_ID,
        query=query,
    )

    daily_data = {}
    campaigns = {}
    totals = {
        "impressions": 0,
        "clicks": 0,
        "cost": 0.0,
        "conversions": 0.0,
    }

    for batch in response:
        for row in batch.results:
            campaign_id   = str(row.campaign.id)
            campaign_name = row.campaign.name
            date_str      = row.segments.date
            impressions   = row.metrics.impressions
            clicks        = row.metrics.clicks
            cost          = row.metrics.cost_micros / 1_000_000
            conversions   = row.metrics.conversions
            ctr           = row.metrics.ctr * 100  # percentual
            avg_cpc       = row.metrics.average_cpc / 1_000_000

            # Acumula totais
            totals["impressions"] += impressions
            totals["clicks"]      += clicks
            totals["cost"]        += cost
            totals["conversions"] += conversions

            # Mapa de campanhas
            if campaign_id not in campaigns:
                campaigns[campaign_id] = {
                    "id": campaign_id,
                    "name": campaign_name,
                    "status": row.campaign.status.name,
                    "impressions": 0,
                    "clicks": 0,
                    "cost": 0.0,
                    "conversions": 0.0,
                }
            campaigns[campaign_id]["impressions"] += impressions
            campaigns[campaign_id]["clicks"]      += clicks
            campaigns[campaign_id]["cost"]        += cost
            campaigns[campaign_id]["conversions"] += conversions

            # Série temporal diária (totais de todas as campanhas)
            if date_str not in daily_data:
                daily_data[date_str] = {
                    "date": date_str,
                    "impressions": 0,
                    "clicks": 0,
                    "cost": 0.0,
                    "conversions": 0.0,
                }
            daily_data[date_str]["impressions"] += impressions
            daily_data[date_str]["clicks"]      += clicks
            daily_data[date_str]["cost"]        += cost
            daily_data[date_str]["conversions"] += conversions

    # Métricas derivadas dos totais
    totals["ctr"]              = (totals["clicks"] / totals["impressions"] * 100) if totals["impressions"] > 0 else 0
    totals["cpc"]              = (totals["cost"] / totals["clicks"]) if totals["clicks"] > 0 else 0
    totals["cost_per_conv"]    = (totals["cost"] / totals["conversions"]) if totals["conversions"] > 0 else 0

    # Métricas derivadas por campanha
    for c in campaigns.values():
        c["ctr"]           = (c["clicks"] / c["impressions"] * 100) if c["impressions"] > 0 else 0
        c["cpc"]           = (c["cost"] / c["clicks"]) if c["clicks"] > 0 else 0
        c["cost_per_conv"] = (c["cost"] / c["conversions"]) if c["conversions"] > 0 else 0

    # Série diária ordenada
    daily_series = sorted(daily_data.values(), key=lambda x: x["date"])

    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "customer_id": CUSTOMER_ID,
        "period": {
            "start": str(start_date),
            "end": str(today),
        },
        "totals": totals,
        "campaigns": list(campaigns.values()),
        "daily_series": daily_series,
    }


def main():
    print("Conectando ao Google Ads API...")
    client = get_client()

    print(f"Buscando dados da conta {CUSTOMER_ID}...")
    data = fetch_campaign_data(client)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✓ {OUTPUT_FILE} atualizado — {data['updated_at']}")
    print(f"  Impressões: {data['totals']['impressions']:,}")
    print(f"  Cliques:    {data['totals']['clicks']:,}")
    print(f"  Custo:      R$ {data['totals']['cost']:,.2f}")
    print(f"  Conversões: {data['totals']['conversions']:,.1f}")


if __name__ == "__main__":
    main()
