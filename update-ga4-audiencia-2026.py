"""
update-ga4-audiencia-2026.py

Perfil demografico, geografico e de tecnologia do publico de 2026, MACRO:
as duas edicoes juntas, moto e auto, medido por ACESSO AO SITE e nao por venda.

DIFERENCA PARA O SCRIPT DE 2025: aquele filtra por hostname/pagePath da Moto.
Aqui NAO existe filtro. A propriedade 378336436 cobre o site principal e os dois
sites de ingresso, entao sem filtro o retrato e o do publico inteiro do festival,
que e exatamente o recorte macro pedido.

Saida: ga4-audiencia-2026.json
Roda via GitHub Actions, disparo manual.
"""

import json, os
from datetime import datetime, timedelta, timezone
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Filter, FilterExpression, FilterExpressionList,
    Metric, OrderBy, RunReportRequest,
)

PROPERTY_ID  = os.environ.get("GA4_PROPERTY_ID", "378336436")
OUTPUT_FILE  = "ga4-audiencia-2026.json"
SCOPES       = ["https://www.googleapis.com/auth/analytics.readonly"]
# Campanha 2026: da abertura das vendas ate hoje. Cobre Moto (13 a 16/08) e
# Auto (27 a 30/08) na mesma janela.
PERIODO_INICIO = "2026-03-30"
PERIODO_FIM    = os.environ.get("GA4_FIM", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# SEM FILTRO DE EDICAO. O recorte macro quer o site inteiro, as duas edicoes.
SEM_FILTRO = None

def get_client():
    refresh = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN")
    cid = os.environ.get("GOOGLE_ADS_CLIENT_ID")
    csec = os.environ.get("GOOGLE_ADS_CLIENT_SECRET")
    if refresh and cid and csec:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        creds = Credentials(token=None, refresh_token=refresh,
                            token_uri="https://oauth2.googleapis.com/token",
                            client_id=cid, client_secret=csec, scopes=SCOPES)
        creds.refresh(Request())
        return BetaAnalyticsDataClient(credentials=creds)
    return BetaAnalyticsDataClient()


def query(client, dim, start, end, limit=50, order_by_metric=True, extra_filter=True):
    dims = [Dimension(name=d) for d in (dim if isinstance(dim, list) else [dim])]
    req = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimensions=dims,
        metrics=[Metric(name="sessions"), Metric(name="totalUsers"),
                 Metric(name="averageSessionDuration"), Metric(name="bounceRate")],
        dimension_filter=SEM_FILTRO,
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)] if order_by_metric else None,
        limit=limit,
    )
    r = client.run_report(req)
    out = []
    for row in r.rows:
        d_vals = [v.value for v in row.dimension_values]
        m_vals = [v.value for v in row.metric_values]
        out.append({
            "dim": d_vals[0] if len(d_vals)==1 else d_vals,
            "sessions":   int(m_vals[0]),
            "users":      int(m_vals[1]),
            "avg_sess_s": round(float(m_vals[2]), 1),
            "bounce":     round(float(m_vals[3])*100, 2),
        })
    return out


def main():
    s, e = PERIODO_INICIO, PERIODO_FIM
    print(f"Periodo CAMPANHA 2026 (macro, as duas edicoes): {s} -> {e}")

    client = get_client()
    print("Cliente GA4 OK. Property:", PROPERTY_ID)

    out = {
        "updated_at":  datetime.now(timezone.utc).isoformat(),
        "property_id": PROPERTY_ID,
        "edition":     "Moto e Auto (macro)",
        "year":        2026,
        "recorte":     "site inteiro, sem filtro de edicao",
        "period":      {"start": s, "end": e},
    }

    print("\n--- Idade ---")
    out["age"] = query(client, "userAgeBracket", s, e, limit=20)
    for r in out["age"]: print(f"  {r['dim']:10s}: {r['sessions']:>6} sess | {r['users']:>5} users | bounce {r['bounce']}%")

    print("\n--- Genero ---")
    out["gender"] = query(client, "userGender", s, e, limit=10)
    for r in out["gender"]: print(f"  {r['dim']:10s}: {r['sessions']:>6} sess | {r['users']:>5} users")

    print("\n--- Top 20 cidades ---")
    out["cities"] = query(client, "city", s, e, limit=20)
    for r in out["cities"][:10]: print(f"  {r['dim']:30s}: {r['sessions']:>6} sess")

    print("\n--- Estados (region) ---")
    out["regions"] = query(client, "region", s, e, limit=30)
    for r in out["regions"][:15]: print(f"  {r['dim']:25s}: {r['sessions']:>6} sess")

    print("\n--- Pais ---")
    out["countries"] = query(client, "country", s, e, limit=10)
    for r in out["countries"][:5]: print(f"  {r['dim']:20s}: {r['sessions']:>6} sess")

    print("\n--- Device Category ---")
    out["devices"] = query(client, "deviceCategory", s, e, limit=10)
    for r in out["devices"]: print(f"  {r['dim']:10s}: {r['sessions']:>6} sess ({r['bounce']}% bounce)")

    print("\n--- OS ---")
    out["os"] = query(client, "operatingSystem", s, e, limit=10)
    for r in out["os"][:5]: print(f"  {r['dim']:15s}: {r['sessions']:>6} sess")

    print("\n--- Browser ---")
    out["browsers"] = query(client, "browser", s, e, limit=10)
    for r in out["browsers"][:5]: print(f"  {r['dim']:15s}: {r['sessions']:>6} sess")

    print("\n--- Canais (sessionDefaultChannelGrouping) ---")
    out["channels"] = query(client, "sessionDefaultChannelGrouping", s, e, limit=15)
    for r in out["channels"]: print(f"  {r['dim']:25s}: {r['sessions']:>6} sess")

    print("\n--- Idioma (language) ---")
    out["languages"] = query(client, "language", s, e, limit=10)
    for r in out["languages"][:5]: print(f"  {r['dim']:15s}: {r['sessions']:>6} sess")

    # Idade x Genero combinado
    print("\n--- Idade x Genero ---")
    out["age_gender"] = query(client, ["userAgeBracket","userGender"], s, e, limit=50)
    for r in out["age_gender"][:15]: print(f"  {r['dim'][0]:10s} {r['dim'][1]:8s}: {r['sessions']:>6} sess")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n>>> Salvo: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
