"""
update-google-analytics.py
Busca dados do GA4 (Festival Duas Rodas – Property 378336436)
e grava ga4-data.json para o dashboard.
Roda via GitHub Actions a cada 30 minutos.
"""

import json
import os
from datetime import datetime, timedelta, timezone

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    OrderBy,
)
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# ── Configurações via GitHub Secrets ─────────────────────────────────────────
PROPERTY_ID   = os.environ["GA4_PROPERTY_ID"]       # 378336436
CLIENT_ID     = os.environ["GOOGLE_ADS_CLIENT_ID"]
CLIENT_SECRET = os.environ["GOOGLE_ADS_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["GOOGLE_ADS_REFRESH_TOKEN"]

OUTPUT_FILE   = "ga4-data.json"
LOOKBACK_DAYS = 90  # últimos 90 dias de histórico

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


def get_client():
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return BetaAnalyticsDataClient(credentials=creds)


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
        raw_date = row.dimension_values[0].value  # "YYYYMMDD"
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
    """Sessões por canal (Organic Search, Paid Search, Social, etc.)."""
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="sessionDefaultChannelGroup")],
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
    print("Conectando ao GA4 API…")
    client = get_client()

    today      = datetime.now(timezone.utc).date()
    start_date = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    end_date   = today.strftime("%Y-%m-%d")

    print(f"Buscando dados de {start_date} a {end_date} — property {PROPERTY_ID}…")

    daily_series    = fetch_daily_series(client, start_date, end_date)
    traffic_sources = fetch_traffic_sources(client, start_date, end_date)
    top_pages       = fetch_top_pages(client, start_date, end_date)
    totals          = compute_totals(daily_series)

    data = {
        "updated_at":      datetime.now(timezone.utc).isoformat(),
        "property_id":     PROPERTY_ID,
        "period":          {"start": start_date, "end": end_date},
        "totals":          totals,
        "daily_series":    daily_series,
        "traffic_sources": traffic_sources,
        "top_pages":       top_pages,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✓ {OUTPUT_FILE} atualizado — {data['updated_at']}")
    print(f"  Sessões:    {totals['sessions']:,}")
    print(f"  Usuários:   {totals['users']:,}")
    print(f"  Pageviews:  {totals['pageviews']:,}")
    print(f"  Conversões: {totals['conversions']:,.1f}")


if __name__ == "__main__":
    main()
