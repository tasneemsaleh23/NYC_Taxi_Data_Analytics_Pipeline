import os
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyhive import hive
from tenacity import retry, stop_after_attempt, wait_fixed, before_log
import time

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

HIVE_HOST     = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT     = int(os.environ.get("HIVE_PORT", 10000))
HIVE_DATABASE = os.environ.get("HIVE_DATABASE", "nyc_taxi")

@retry(
    stop=stop_after_attempt(10),
    wait=wait_fixed(5),
    before=before_log(log, logging.INFO),
    reraise=True
)
def get_hive_connection():
    conn = hive.connect(
        host=HIVE_HOST,
        port=HIVE_PORT,
        database=HIVE_DATABASE,
        auth="NOSASL",
        configuration={"hive.execution.engine": "mr"}
    )
    return conn

def run_query(sql: str) -> pd.DataFrame:
    try:
        conn = get_hive_connection()
        df = pd.read_sql(sql, conn)
        conn.close()
        df.columns = [c.split('.')[-1] for c in df.columns]
        return df
    except Exception as exc:
        log.error(f"Hive query failed: {exc}")
        return pd.DataFrame()

CHART_TEMPLATE = "plotly_dark"
ACCENT_COLORS  = ["#a78bfa", "#60a5fa", "#34d399", "#f472b6", "#fbbf24", "#f87171"]

def style_chart(fig) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color="#e8e8f0"),
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(
            bgcolor="rgba(255,255,255,0.05)",
            bordercolor="rgba(255,255,255,0.1)",
            borderwidth=1,
        )
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.07)", showgrid=True)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.07)", showgrid=True)
    return fig

def wait_for_hive():
    log.info("Waiting for HiveServer2 to become ready...")
    for _ in range(60):
        try:
            conn = get_hive_connection()
            conn.close()
            log.info("HiveServer2 is ready!")
            return True
        except Exception as e:
            log.warning(f"Hive not ready yet: {e}")
            time.sleep(10)
    log.error("Timed out waiting for Hive.")
    return False

def main():
    if not wait_for_hive():
        log.error("Cannot proceed, Hive is down.")
        return
        
    log.info("Connecting to Hive and running queries...")
    
    html_content = ["<html><head><title>NYC Taxi Analytics Report</title></head><body style='background-color:#1e1e2e; color:#cdd6f4; font-family:sans-serif;'>"]
    html_content.append("<h1>NYC Taxi Analytics Report</h1>")

    # 1. Revenue
    revenue_df_raw = run_query("SELECT * FROM revenue_summary")
    if not revenue_df_raw.empty:
        revenue_df = pd.DataFrame([{
            "total_trips": revenue_df_raw["total_trips"].sum(),
            "total_revenue": revenue_df_raw["total_revenue"].sum(),
            "avg_fare": revenue_df_raw["avg_fare"].mean(),
            "avg_duration_min": revenue_df_raw["avg_duration_min"].mean()
        }])
    else:
        revenue_df = pd.DataFrame()

    if not revenue_df.empty:
        total_trips   = int(revenue_df["total_trips"].iloc[0])    if not revenue_df.empty and pd.notnull(revenue_df["total_trips"].iloc[0]) else 0
        total_revenue = float(revenue_df["total_revenue"].iloc[0]) if not revenue_df.empty and pd.notnull(revenue_df["total_revenue"].iloc[0]) else 0.0
        avg_fare      = float(revenue_df["avg_fare"].iloc[0])      if not revenue_df.empty and pd.notnull(revenue_df["avg_fare"].iloc[0]) else 0.0
        avg_duration  = float(revenue_df["avg_duration_min"].iloc[0]) if not revenue_df.empty and pd.notnull(revenue_df["avg_duration_min"].iloc[0]) else 0.0

        html_content.append(f"<h2>Key Performance Indicators</h2>")
        html_content.append(f"<ul><li>Total Trips: {total_trips:,}</li>")
        html_content.append(f"<li>Total Revenue: ${total_revenue:,.2f}</li>")
        html_content.append(f"<li>Avg Fare: ${avg_fare:.2f}</li>")
        html_content.append(f"<li>Avg Trip Duration: {avg_duration:.1f} min</li></ul>")

    # 2. Trips per hour
    trips_hour_raw = run_query("SELECT * FROM trips_per_hour")
    if not trips_hour_raw.empty:
        trips_hour_df = trips_hour_raw.groupby("trip_hour", as_index=False)["total_trips"].sum().sort_values("trip_hour")
        trips_hour_df["hour_label"] = trips_hour_df["trip_hour"].apply(
            lambda h: f"{h % 12 or 12} {'AM' if h < 12 else 'PM'}"
        )
        fig_hour = px.area(
            trips_hour_df, x="hour_label", y="total_trips", title="Total Trips by Hour of Day",
            template=CHART_TEMPLATE, color_discrete_sequence=["#a78bfa"]
        )
        html_content.append(style_chart(fig_hour).to_html(full_html=False, include_plotlyjs='cdn'))
    
    # 3. Daily Revenue
    daily_rev_df = run_query("SELECT * FROM revenue_summary")
    if not daily_rev_df.empty:
        daily_rev_df["date_label"] = daily_rev_df["trip_year"].astype(str) + "-" + \
                                     daily_rev_df["trip_month"].astype(str).str.zfill(2) + "-" + \
                                     daily_rev_df["trip_day"].astype(str).str.zfill(2)
        daily_rev_df = daily_rev_df.sort_values(["trip_year", "trip_month", "trip_day"])
        fig_rev = px.bar(
            daily_rev_df, x="date_label", y="total_revenue", title="Daily Total Revenue",
            template=CHART_TEMPLATE, color="total_revenue", color_continuous_scale="Purples"
        )
        html_content.append(style_chart(fig_rev).to_html(full_html=False, include_plotlyjs=False))

    # 4. Top Pickup Zones
    zones_df = run_query("SELECT * FROM top_pickup_zones")
    if not zones_df.empty:
        zones_df = zones_df.sort_values("total_trips", ascending=False).head(20)
        zones_df["zone_label"] = "Zone " + zones_df["pickup_location_id"].astype(str)
        fig_zones = px.bar(
            zones_df.sort_values("total_trips"), x="total_trips", y="zone_label", orientation="h",
            title="Top 20 Pickup Zones", template=CHART_TEMPLATE, color="total_trips", color_continuous_scale="Viridis"
        )
        html_content.append(style_chart(fig_zones).to_html(full_html=False, include_plotlyjs=False))
        
    # 5. Payment Methods
    payment_df = run_query("SELECT * FROM payment_summary")
    if not payment_df.empty:
        payment_df = payment_df.sort_values("total_trips", ascending=False)
        fig_pie = px.pie(
            payment_df, names="payment_label", values="total_trips", title="Trip Share by Payment Method",
            template=CHART_TEMPLATE, color_discrete_sequence=ACCENT_COLORS, hole=0.42
        )
        html_content.append(style_chart(fig_pie).to_html(full_html=False, include_plotlyjs=False))
        
        fig_rev_pay = px.bar(
            payment_df, x="payment_label", y="total_revenue", title="Revenue by Payment Method",
            template=CHART_TEMPLATE, color="payment_label", color_discrete_sequence=ACCENT_COLORS
        )
        html_content.append(style_chart(fig_rev_pay).to_html(full_html=False, include_plotlyjs=False))

    # 6. Monthly summary
    monthly_raw = run_query("SELECT * FROM revenue_summary")
    if not monthly_raw.empty:
        monthly_df = monthly_raw.groupby(["trip_year", "trip_month"], as_index=False).agg(
            total_trips=("total_trips", "sum"),
            total_revenue=("total_revenue", "sum"),
            avg_fare=("avg_fare", "mean"),
            avg_duration_min=("avg_duration_min", "mean")
        ).sort_values(["trip_year", "trip_month"])
        html_content.append("<h2>Monthly Summary</h2>")
        html_content.append(monthly_df.to_html())
    
    html_content.append("</body></html>")
    
    with open("output.html", "w") as f:
        f.write("\n".join(html_content))
    
    log.info("Report generated at /app/output.html. Waiting forever to keep container alive.")
    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()
