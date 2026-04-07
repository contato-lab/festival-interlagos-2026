"""
update-google-analytics.py
Busca dados do GA4 (Festival Duas Rodas – Property 378336436)
e grava ga4-data.json para o dashboard.

Autenticação (em ordem de prioridade):
  1. OAuth2 via GOOGLE_ADS_REFRESH_TOKEN (funciona imediatamente — o refresh
     token já inclui o escopo analytics.readonly)
  2. Application Default Credentials / Workload Identity Federation (GHA WIF)
     — ativo quando o service account estiver adicionado como Viewer no GA4

Roda via GitHub Actions a cada hora.
"""

import json
import os
from datetime import datetime, timedelta, timezone

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    OrderBy,
    RunReportRequest,
)

# ── Configurações ─────────────────────────────────────────────────────────────
PROPERTY_ID   = os.environ.get("GA4_PROPERTY_ID", "378336436")
OUTPUT_FILE   = "ga4-data.json"
LOOKBACK_DAYS = 90   # histórico para séries diárias
CHANNEL_DAYS  = 45   # histórico para menções por canal (≈6 semanas)

# Mapeamento: canal GA4 → nome no dashboard (Menções Gerais)
CHANNEL_MAP = {
    "Organic Search":  "Busca Orgânica",
    "Organic Social":  "Social Orgânico",
    "Direct":          "Direto pelo Link",
    "Referral":        "Direto pelo Link",   # agrega com Direto
}

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


def get_client():
    """
    Retorna BetaAnalyticsDataClient.
    Tenta OAuth2 primeiro (refresh token no env); cai em ADC se não existir.
    """
    refresh_token  = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN")
    client_id      = os.environ.get("GOOGLE_ADS_CLIENT_ID")
    client_secret  = os.environ.get("GOOGLE_ADS_CLIENT_SECRET")

    if refresh_token and client_id and client_secret:
        print("Autenticando via OAuth2 (refresh token)…")
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=SCOPES,
        )
        creds.refresh(Request())
        return BetaAnalyticsDataClient(credentials=creds)

    print("Autenticando via ADC / Workload Identity Federation…")
    return BetaAnalyticsDataClient()   # usa GOOGLE_APPLICATION_CREDENTIALS


# ─────────────────────────────────────────────────────────────────────────────
def fetch_daily_series(client, start_date, end_date):
    """Sessões, usuários, pageviews, eventos por dia."""
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="screenPageViews"),
            Metric(name="eventCount"),
            Metric(name="conversions"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
        ],
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
        limit=500,
    )
    response = client.run_report(request)

    series = []
    for row in response.rows:
        raw_date = row.dimension_values[0].value   # "YYYYMMDD"
        iso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        v = [m.value for m in row.metric_values]
        series.append({
            "date":          iso_date,
            "sessions":      int(v[0]),
            "users":         int(v[1]),
            "new_users":     int(v[2]),
            "pageviews":     int(v[3]),
            "events":        int(v[4]),
            "conversions":   float(v[5]),
            "bounce_rate":   round(float(v[6]) * 100, 2),
            "avg_session_s": round(float(v[7]), 1),
        })
    return series


def fetch_traffic_sources(client, start_date, end_date):
    """Sessões por canal (totais do período)."""
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="sessionDefaultChannelGrouping")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="conversions"),
        ],
        limit=20,
    )
    response = client.run_report(request)

    sources = []
    for row in response.rows:
        v = [m.value for m in row.metric_values]
        sources.append({
            "channel":     row.dimension_values[0].value,
            "sessions":    int(v[0]),
            "conversions": float(v[1]),
        })
    sources.sort(key=lambda x: x["sessions"], reverse=True)
    return sources


def fetch_sessions_by_date_channel(client) -> dict:
    """
    Sessões por data × canal — usado pela tabela Menções Gerais no dashboard.
    Retorna: { "YYYY-MM-DD": { "Busca Orgânica": N, "Social Orgânico": N, … } }
    """
    end   = datetime.now(timezone.utc) - timedelta(days=1)
    start = end - timedelta(days=CHANNEL_DAYS)

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="sessionDefaultChannelGrouping"),
        ],
        metrics=[Metric(name="sessions")],
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
        limit=2000,
    )
    response = client.run_report(request)

    result: dict = {}
    for row in response.rows:
        raw_date = row.dimension_values[0].value    # "YYYYMMDD"
        channel  = row.dimension_values[1].value
        sessions = int(row.metric_values[0].value)
        iso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        mapped   = CHANNEL_MAP.get(channel)
        if mapped:
            result.setdefault(iso_date, {})
            result[iso_date][mapped] = result[iso_date].get(mapped, 0) + sessions

    return result


def fetch_influencer_breakdown(client) -> list:
    """
    Por influenciador via utm_campaign (sessionCampaignName).
    Os links usam utm_medium=story/instagram, nao influencer.
    Duas queries: (1) sessoes+conversoes por campanha, (2) sessoes por campanha+pagePath.
    """
    INFLUENCER_CAMPAIGNS = {
        "FelipeTitto", "KarinaSimoes", "LeandroMello",
        "AmandaP", "DaianeGaia", "DanielD2", "DavidJensen", "DayMiguel",
        "DurvalCareca", "EduardoBernasconi", "ElianaMalizia", "FANYRAINHA",
        "GiseleFavaro", "RafaelTogni", "RodolfinhoZ", "RodrigoRateiro",
        "SekuMello", "Vans", "lucasxaparral",
        "dinamize", "perfilfestival", "fullpower", "duasrodas",
    }

    TICKET_PAGES = {
        "ride-pass":   "Ride Pass",
        "sport-pass":  "Sport Pass",
        "drive-pass":  "Drive Pass",
        "street-pass": "Street Pass",
        "fan-pass":    "Fan Pass",
        "vip-pass":    "VIP Pass",
        "pit-pass":    "Pit Pass",
    }

    end   = datetime.now(timezone.utc) - timedelta(days=1)
    start = end - timedelta(days=CHANNEL_DAYS)
    date_range = DateRange(
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
    )

    # Query 1: sessoes + conversoes por sessionCampaignName
    r1 = client.run_report(RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[date_range],
        dimensions=[Dimension(name="sessionCampaignName")],
        metrics=[Metric(name="sessions"), Metric(name="conversions")],
        limit=500,
    ))
    sources: dict = {}
    for row in r1.rows:
        campaign = row.dimension_values[0].value
        if campaign not in INFLUENCER_CAMPAIGNS:
            continue
        sources[campaign] = {
            "source":      campaign,
            "sessions":    int(row.metric_values[0].value),
            "conversions": round(float(row.metric_values[1].value)),
            "tickets":     {},
        }

    if not sources:
        return []

    # Query 2: sessoes por sessionCampaignName + pagePath
    r2 = client.run_report(RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[date_range],
        dimensions=[Dimension(name="sessionCampaignName"), Dimension(name="pagePath")],
        metrics=[Metric(name="sessions")],
        limit=5000,
    ))
    for row in r2.rows:
        campaign = row.dimension_values[0].value
        page     = row.dimension_values[1].value.lower()
        sess     = int(row.metric_values[0].value)
        if campaign not in sources:
            continue
        for slug, name in TICKET_PAGES.items():
            if slug in page:
                sources[campaign]["tickets"][name] = sources[campaign]["tickets"].get(name, 0) + sess
                break

    return sorted(sources.values(), key=lambda x: x["conversions"], reverse=True)
def fetch_influencer_sessions(client) -> dict:
    """
    Sessões com utm_medium=influencer por data.
    Retorna: { "YYYY-MM-DD": N }
    """
    end   = datetime.now(timezone.utc) - timedelta(days=1)
    start = end - timedelta(days=CHANNEL_DAYS)

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )],
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="sessions")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="sessionMedium",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.EXACT,
                    value="influencer",
                    case_sensitive=False,
                ),
            )
        ),
        limit=500,
    )
    response = client.run_report(request)

    result: dict = {}
    for row in response.rows:
        raw_date = row.dimension_values[0].value
        sessions = int(row.metric_values[0].value)
        iso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        result[iso_date] = result.get(iso_date, 0) + sessions

    return result


def fetch_top_pages(client, start_date, end_date):
    """Top 10 páginas por visualizações."""
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="pagePath")],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="sessions"),
        ],
        order_bys=[OrderBy(
            metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"),
            desc=True,
        )],
        limit=10,
    )
    response = client.run_report(request)

    pages = []
    for row in response.rows:
        v = [m.value for m in row.metric_values]
        pages.append({
            "page":      row.dimension_values[0].value,
            "pageviews": int(v[0]),
            "sessions":  int(v[1]),
        })
    return pages


def compute_totals(series):
    t = dict(sessions=0, users=0, new_users=0, pageviews=0, events=0, conversions=0.0)
    for d in series:
        t["sessions"]    += d["sessions"]
        t["users"]       += d["users"]
        t["new_users"]   += d["new_users"]
        t["pageviews"]   += d["pageviews"]
        t["events"]      += d["events"]
        t["conversions"] += d["conversions"]
    t["pages_per_session"] = round(t["pageviews"] / t["sessions"], 2) if t["sessions"] else 0
    t["avg_bounce_rate"]   = round(
        sum(d["bounce_rate"] for d in series) / len(series), 2
    ) if series else 0
    return t


def main():
    print(f"=== GA4 Update — property {PROPERTY_ID} ===")
    client = get_client()

    today      = datetime.now(timezone.utc).date()
    start_date = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    end_date   = (today - timedelta(days=1)).strftime("%Y-%m-%d")  # ontem

    print(f"Buscando série diária de {start_date} a {end_date}…")
    daily_series    = fetch_daily_series(client, start_date, end_date)
    traffic_sources = fetch_traffic_sources(client, start_date, end_date)
    top_pages       = fetch_top_pages(client, start_date, end_date)
    totals          = compute_totals(daily_series)

    print(f"Buscando sessões por canal (últimos {CHANNEL_DAYS} dias)…")
    sessions_by_channel  = fetch_sessions_by_date_channel(client)
    influencer_sessions  = fetch_influencer_sessions(client)
    influencer_breakdown = fetch_influencer_breakdown(client)

    # Injeta Influenciadores no mapa de canais
    for date, count in influencer_sessions.items():
        sessions_by_channel.setdefault(date, {})
        sessions_by_channel[date]["Influenciadores"] = (
            sessions_by_channel[date].get("Influenciadores", 0) + count
        )

    data = {
        "updated_at":      datetime.now(timezone.utc).isoformat(),
        "property_id":     PROPERTY_ID,
        "period":          {"start": start_date, "end": end_date},
        "totals":          totals,
        "daily_series":    daily_series,
        "traffic_sources": traffic_sources,
        "top_pages":       top_pages,
        # ↓ Consumido pela tabela Menções Gerais no dashboard
        "sessions_by_date":    sessions_by_channel,
        # ↓ Top influenciadores com breakdown moto/auto
        "influencer_breakdown": influencer_breakdown,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✓ {OUTPUT_FILE} atualizado — {data['updated_at']}")
    print(f"  Sessões:         {totals['sessions']:,}")
    print(f"  Usuários:        {totals['users']:,}")
    print(f"  Pageviews:       {totals['pageviews']:,}")
    print(f"  Dias com canais: {len(sessions_by_channel)}")


if __name__ == "__main__":
    main()
