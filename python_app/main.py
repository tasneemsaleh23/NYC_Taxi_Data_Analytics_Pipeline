import os
import json
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from flask import Flask, render_template, jsonify
from pyhive import hive
from tenacity import retry, stop_after_attempt, wait_fixed, before_log
import time

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
HIVE_HOST     = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT     = int(os.environ.get("HIVE_PORT", 10000))
HIVE_DATABASE = os.environ.get("HIVE_DATABASE", "nyc_taxi")
FLASK_PORT    = int(os.environ.get("FLASK_PORT", 8501))

CHART_TEMPLATE = "plotly_dark"
ACCENT        = ["#a78bfa", "#60a5fa", "#34d399", "#f472b6", "#fbbf24", "#f87171"]

app = Flask(__name__)

# ---------------------------------------------------------------------------
# HIVE HELPERS
# ---------------------------------------------------------------------------
@retry(stop=stop_after_attempt(10), wait=wait_fixed(5), before=before_log(log, logging.INFO), reraise=True)
def get_hive_connection():
    return hive.connect(
        host=HIVE_HOST,
        port=HIVE_PORT,
        database=HIVE_DATABASE,
        auth="NOSASL",
        configuration={"hive.execution.engine": "mr"},
    )


def run_query(sql: str) -> pd.DataFrame:
    """Execute a SQL query against HiveServer2 and return a DataFrame."""
    try:
        conn = get_hive_connection()
        df = pd.read_sql(sql, conn)
        conn.close()
        df.columns = [c.split(".")[-1] for c in df.columns]
        return df
    except Exception as exc:
        log.error(f"Hive query failed: {exc}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# CHART HELPERS  (returns Plotly JSON string consumed by Plotly.js on client)
# ---------------------------------------------------------------------------
def _fig_json(fig) -> str:
    """Apply dark/transparent theme and serialise to JSON for the template."""
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#e2e8f0"),
        margin=dict(l=20, r=20, t=48, b=20),
        legend=dict(bgcolor="rgba(255,255,255,0.05)", bordercolor="rgba(255,255,255,0.1)", borderwidth=1),
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.07)", showgrid=True)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.07)", showgrid=True)
    return fig.to_json()


# ---------------------------------------------------------------------------
# DATA FETCH  (called on every request so the dashboard is always live)
# ---------------------------------------------------------------------------
def fetch_dashboard_data() -> dict:
    """Query Hive for all KPIs and return a data dict for the template."""
    data = {}

    # ── 1. KPI header cards ──────────────────────────────────────────────
    rev_df = run_query("SELECT * FROM revenue_summary")
    if not rev_df.empty:
        data["total_trips"]    = f"{int(rev_df['total_trips'].sum()):,}"
        data["total_revenue"]  = f"${float(rev_df['total_revenue'].sum()):,.2f}"
        data["avg_fare"]       = f"${float(rev_df['avg_fare'].mean()):.2f}"
        data["avg_duration"]   = f"{float(rev_df['avg_duration_min'].mean()):.1f} min"
    else:
        data["total_trips"] = data["total_revenue"] = data["avg_fare"] = data["avg_duration"] = "N/A"

    # ── 2. Trips per hour ────────────────────────────────────────────────
    trips_hour_raw = run_query("SELECT * FROM trips_per_hour")
    if not trips_hour_raw.empty:
        th = (
            trips_hour_raw
            .groupby("trip_hour", as_index=False)["total_trips"].sum()
            .sort_values("trip_hour")
        )
        th["hour_label"] = th["trip_hour"].apply(
            lambda h: f"{h % 12 or 12} {'AM' if h < 12 else 'PM'}"
        )
        fig = px.area(
            th, x="hour_label", y="total_trips",
            title="Trips by Hour of Day",
            template=CHART_TEMPLATE,
            color_discrete_sequence=["#a78bfa"],
        )
        fig.update_traces(fill="tozeroy", line_color="#a78bfa")
        data["chart_trips_hour"] = _fig_json(fig)
    else:
        data["chart_trips_hour"] = None

    # ── 3. Daily revenue ────────────────────────────────────────────────
    if not rev_df.empty:
        dr = rev_df.copy()
        dr["date_label"] = (
            dr["trip_year"].astype(str) + "-"
            + dr["trip_month"].astype(str).str.zfill(2) + "-"
            + dr["trip_day"].astype(str).str.zfill(2)
        )
        dr = dr.sort_values(["trip_year", "trip_month", "trip_day"])
        fig = px.bar(
            dr, x="date_label", y="total_revenue",
            title="Daily Revenue",
            template=CHART_TEMPLATE,
            color="total_revenue",
            color_continuous_scale="Purples",
        )
        data["chart_daily_rev"] = _fig_json(fig)
    else:
        data["chart_daily_rev"] = None

    # ── 4. Top pickup zones ──────────────────────────────────────────────
    zones_df = run_query("SELECT * FROM top_pickup_zones")
    if not zones_df.empty:
        zones = zones_df.sort_values("total_trips", ascending=False).head(20).copy()
        zones["zone_label"] = "Zone " + zones["pickup_location_id"].astype(str)
        fig = px.bar(
            zones.sort_values("total_trips"),
            x="total_trips", y="zone_label", orientation="h",
            title="Top 20 Pickup Zones",
            template=CHART_TEMPLATE,
            color="total_trips",
            color_continuous_scale="Viridis",
        )
        data["chart_zones"] = _fig_json(fig)
    else:
        data["chart_zones"] = None

    # ── 5. Payment split + revenue ───────────────────────────────────────
    pay_df = run_query("SELECT * FROM payment_summary")
    if not pay_df.empty:
        pay = pay_df.sort_values("total_trips", ascending=False)
        fig_pie = px.pie(
            pay, names="payment_label", values="total_trips",
            title="Trip Share by Payment Method",
            template=CHART_TEMPLATE,
            color_discrete_sequence=ACCENT,
            hole=0.42,
        )
        data["chart_payment_pie"] = _fig_json(fig_pie)

        fig_rev = px.bar(
            pay, x="payment_label", y="total_revenue",
            title="Revenue by Payment Method",
            template=CHART_TEMPLATE,
            color="payment_label",
            color_discrete_sequence=ACCENT,
        )
        data["chart_payment_rev"] = _fig_json(fig_rev)
    else:
        data["chart_payment_pie"] = data["chart_payment_rev"] = None

    # ── 6. Monthly summary table ─────────────────────────────────────────
    if not rev_df.empty:
        monthly = (
            rev_df
            .groupby(["trip_year", "trip_month"], as_index=False)
            .agg(
                total_trips=("total_trips", "sum"),
                total_revenue=("total_revenue", "sum"),
                avg_fare=("avg_fare", "mean"),
                avg_duration_min=("avg_duration_min", "mean"),
            )
            .sort_values(["trip_year", "trip_month"])
        )
        monthly["total_revenue"] = monthly["total_revenue"].map("${:,.2f}".format)
        monthly["avg_fare"]      = monthly["avg_fare"].map("${:.2f}".format)
        monthly["avg_duration_min"] = monthly["avg_duration_min"].map("{:.1f} min".format)
        monthly["total_trips"]   = monthly["total_trips"].map("{:,}".format)
        data["monthly_table"] = monthly.to_dict(orient="records")
        data["monthly_columns"] = list(monthly.columns)
    else:
        data["monthly_table"]   = []
        data["monthly_columns"] = []

    return data


# ---------------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    data = fetch_dashboard_data()
    return render_template("dashboard.html", **data)


@app.route("/health")
def health():
    """Lightweight health check endpoint for Docker."""
    return jsonify(status="ok"), 200


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Starting NYC Taxi Flask dashboard on port %s …", FLASK_PORT)
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False)
