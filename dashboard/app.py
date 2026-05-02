import sys
from pathlib import Path
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
import streamlit as st
from paths import OUT
import requests
import time

# Mapping from Brazilian state codes to ISO-3166-2 codes for the choropleth map
BR_STATE_CODES = {
    "AC": "BR-AC", "AL": "BR-AL", "AP": "BR-AP", "AM": "BR-AM",
    "BA": "BR-BA", "CE": "BR-CE", "DF": "BR-DF", "ES": "BR-ES",
    "GO": "BR-GO", "MA": "BR-MA", "MT": "BR-MT", "MS": "BR-MS",
    "MG": "BR-MG", "PA": "BR-PA", "PB": "BR-PB", "PR": "BR-PR",
    "PE": "BR-PE", "PI": "BR-PI", "RJ": "BR-RJ", "RN": "BR-RN",
    "RS": "BR-RS", "RO": "BR-RO", "RR": "BR-RR", "SC": "BR-SC",
    "SP": "BR-SP", "SE": "BR-SE", "TO": "BR-TO"
}

# CONFIG
st.set_page_config(
    page_title="E-Commerce Dashboard",
    page_icon="🛒",
    layout="wide"
)

st_autorefresh(interval=40000, key="autorefresh")

# We are loading the gold parquet files
# @st.cache_data : Stores the data in memory so that it does not reread the files every time the user changes pages.
@st.cache_data(ttl=40)
def load_data():
    gold = OUT / "gold"
    return {
        "sales":      pd.read_parquet(gold / "gold_sales_overview.parquet"),
        "delivery":   pd.read_parquet(gold / "gold_delivery_performance.parquet"),
        "categories": pd.read_parquet(gold / "gold_top_categories.parquet"),
        "sellers":    pd.read_parquet(gold / "gold_seller_performance.parquet"),
        "geography":  pd.read_parquet(gold / "gold_customer_geography.parquet")
    }
data = load_data()

# Side bar

st.sidebar.title("🛒 E-Commerce")
page = st.sidebar.radio(
    "Πλοήγηση",
    ["Sales Overview", "Delivery Performance", "Top Categories", "Seller Performance", "Geography"]
)




# PAGE 1 — SALES OVERVIEW

if page == "Sales Overview":
    st.title("📈 Sales Overview")

    df_sales = data["sales"]
    df_geo = data["geography"]
    df_categories = data["categories"]

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Orders", f"{df_sales['total_orders'].sum():,}")
    col2.metric("Total Revenue", f"R$ {df_sales['total_revenue'].sum():,.2f}")
    col3.metric("Avg Order Value", f"R$ {df_sales['avg_order_value'].mean():,.2f}")
    col4.metric("Total Months", f"{len(df_sales)}")

    st.divider()

    # Line Chart: Monthly Revenue
    df_sales["period"] = df_sales["order_year"].astype(str) + "-" + df_sales["order_month"].astype(str).str.zfill(2)

    st.subheader("Monthly Revenue")
    fig_revenue = px.line(
        df_sales,
        x="period",
        y="total_revenue",
        markers=True,
        labels={"period": "Month", "total_revenue": "Revenue (R$)"}
    )
    st.plotly_chart(fig_revenue, use_container_width=True)

    # Line Chart: Monthly Orders
    st.subheader("Monthly Orders")
    fig_orders = px.line(
        df_sales,
        x="period",
        y="total_orders",
        markers=True,
        labels={"period": "Month", "total_orders": "Orders"}
    )
    st.plotly_chart(fig_orders, use_container_width=True)

    # Pie Chart: Revenue by Top 5 Categories
    st.subheader("Revenue Share — Top 5 Categories")
    top5 = df_categories.head(5)
    fig_pie = px.pie(
        top5,
        values="total_revenue",
        names="product_category_name",
        hole=0.4
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# PAGE 2 — DELIVERY PERFORMANCE
# Desplays delivery performance per month

elif page == "Delivery Performance":
    st.title("🚚 Delivery Performance")

    df_delivery = data["delivery"]

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Fulfillment Days", f"{df_delivery['avg_fulfillment_days'].mean():.1f} days")
    col2.metric("Avg Shipping Days", f"{df_delivery['avg_shipping_days'].mean():.1f} days")
    col3.metric("On-Time Rate", f"{df_delivery['on_time_rate'].mean():.1f}%")
    col4.metric("Late Rate", f"{100 - df_delivery['on_time_rate'].mean():.1f}%")

    st.divider()

    # Line Chart: Avg Fulfillment Days per Month
    df_delivery["period"] = df_delivery["order_year"].astype(str) + "-" + df_delivery["order_month"].astype(str).str.zfill(2)

    st.subheader("Avg Fulfillment Days per Month")
    fig_fulfillment = px.line(
        df_delivery,
        x="period",
        y="avg_fulfillment_days",
        markers=True,
        labels={"period": "Month", "avg_fulfillment_days": "Days"}
    )
    st.plotly_chart(fig_fulfillment, use_container_width=True)

    # Line Chart: On-Time Rate per Month
    st.subheader("On-Time Rate per Month (%)")
    fig_ontime = px.line(
        df_delivery,
        x="period",
        y="on_time_rate",
        markers=True,
        labels={"period": "Month", "on_time_rate": "On-Time Rate (%)"}
    )
    st.plotly_chart(fig_ontime, use_container_width=True)

    # Pie Chart: On-Time vs Late
    st.subheader("On-Time vs Late Deliveries")
    pie_data = {
        "Status": ["On-Time", "Late"],
        "Percentage": [
            df_delivery["on_time_rate"].mean(),
            100 - df_delivery["on_time_rate"].mean()
        ]
    }
    fig_pie = px.pie(
        pie_data,
        values="Percentage",
        names="Status",
        hole=0.4
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# PAGE 3 — TOP CATEGORIES
# It shows the top product categories by revenue and orders.

elif page == "Top Categories":
    st.title("🏆 Top Categories")

    df_categories = data["categories"]

    # KPI Cards
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Categories", f"{len(df_categories):,}")
    col2.metric("Top Category Revenue", f"R$ {df_categories['total_revenue'].max():,.2f}")
    col3.metric("Avg Category Revenue", f"R$ {df_categories['total_revenue'].mean():,.2f}")

    st.divider()

    # Bar Chart: Top 10 by Revenue
    st.subheader("Top 10 Categories by Revenue")
    fig_rev = px.bar(
        df_categories.head(10),
        x="product_category_name",
        y="total_revenue",
        color="total_revenue",
        labels={"product_category_name": "Category", "total_revenue": "Revenue (R$)"}
    )
    st.plotly_chart(fig_rev, use_container_width=True)

    # Bar Chart: Top 10 by Orders
    st.subheader("Top 10 Categories by Orders")
    top10_orders = df_categories.sort_values("total_orders", ascending=False).head(10)
    fig_ord = px.bar(
        top10_orders,
        x="product_category_name",
        y="total_orders",
        color="total_orders",
        labels={"product_category_name": "Category", "total_orders": "Orders"}
    )
    st.plotly_chart(fig_ord, use_container_width=True)

    # Pie Chart: Top 5 Categories by Orders
    st.subheader("Order Share — Top 5 Categories")
    top5_orders = df_categories.sort_values("total_orders", ascending=False).head(5)
    fig_pie = px.pie(
        top5_orders,
        values="total_orders",
        names="product_category_name",
        hole=0.4
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# PAGE 4 — SELLER PERFORMANCE

elif page == "Seller Performance":
    st.title("🏪 Seller Performance")

    df_sellers = data["sellers"]

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Sellers", f"{len(df_sellers):,}")
    col2.metric("Avg Fulfillment Days", f"{df_sellers['avg_fulfillment_days'].mean():.1f} days")
    col3.metric("Avg On-Time Rate", f"{df_sellers['on_time_rate'].mean():.1f}%")
    col4.metric(
        "Sellers > 90% On-Time",
        f"{len(df_sellers[df_sellers['on_time_rate'] > 90]):,}"
    )

    st.divider()

    # Dropdown filter by state
    states = ["All"] + sorted(df_sellers["seller_state"].unique().tolist())
    selected_state = st.selectbox("Filter by State", states)

    if selected_state == "All":
        filtered = df_sellers
    else:
        filtered = df_sellers[df_sellers["seller_state"] == selected_state]

    # Bar Chart: Top 10 Sellers by Revenue
    st.subheader("Top 10 Sellers by Revenue")
    fig_sellers = px.bar(
        filtered.head(10),
        x="seller_id",
        y="total_revenue",
        color="total_revenue",
        labels={"seller_id": "Seller ID", "total_revenue": "Revenue (R$)"}
    )
    st.plotly_chart(fig_sellers, use_container_width=True)

    # Pie Chart: Revenue Share by State
    st.subheader("Revenue Share by Seller State")
    state_revenue = df_sellers.groupby("seller_state")["total_revenue"].sum().reset_index()
    fig_pie = px.pie(
        state_revenue,
        values="total_revenue",
        names="seller_state",
        hole=0.4
    )
    st.plotly_chart(fig_pie, use_container_width=True)

    # Table: Top 20 Sellers
    st.subheader("Top 20 Sellers by Revenue")
    st.dataframe(
        filtered.head(20)[["seller_id", "seller_state", "total_orders", "total_revenue", "avg_fulfillment_days", "on_time_rate"]],
        use_container_width=True
    )

# PAGE 5 — GEOGRAPHY
# It sows orders and revenue per state.Evry line is one state of Brazil.

elif page == "Geography":
    st.title("🗺️ Customer Geography")
    df_geo = data["geography"]
    df_geo["state_iso"] = df_geo["customer_state"].map(BR_STATE_CODES)

    # KPI Cards
    col1, col2, col3 = st.columns(3)
    col1.metric("Total States", f"{len(df_geo)}")
    col2.metric("Top State", df_geo.iloc[0]["customer_state"])
    col3.metric("Avg Orders per State", f"{df_geo['total_orders'].mean():,.0f}")

    st.divider()

    # Bar Chart: Orders by State
    st.subheader("Orders by State")
    fig_orders = px.bar(
        df_geo,
        x="customer_state",
        y="total_orders",
        color="total_orders",
        labels={"customer_state": "State", "total_orders": "Orders"}
    )
    st.plotly_chart(fig_orders, use_container_width=True)

    # Bar Chart: Revenue by State
    st.subheader("Revenue by State")
    fig_revenue = px.bar(
        df_geo,
        x="customer_state",
        y="total_revenue",
        color="total_revenue",
        labels={"customer_state": "State", "total_revenue": "Revenue (R$)"}
    )
    st.plotly_chart(fig_revenue, use_container_width=True)

    # Pie Chart: Top 5 States by Orders
    st.subheader("Order Share — Top 5 States")
    top5_states = df_geo.head(5)
    fig_pie = px.pie(
        top5_states,
        values="total_orders",
        names="customer_state",
        hole=0.4
    )
    st.plotly_chart(fig_pie, use_container_width=True)

    # Full Table
    st.subheader("All States")
    st.dataframe(df_geo, use_container_width=True)

    # Choropleth Map: Orders by State
    st.subheader("Orders by State — Map")

    # Load Brazil states GeoJSON
    geojson_url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
    brazil_geojson = requests.get(geojson_url).json()

    # Match state codes to GeoJSON
    for feature in brazil_geojson["features"]:
        feature["id"] = feature["properties"]["sigla"]

    fig_map = px.choropleth(
        df_geo,
        geojson=brazil_geojson,
        locations="customer_state",
        color="total_orders",
        color_continuous_scale="Blues",
        labels={"total_orders": "Orders", "customer_state": "State"},
        fitbounds="locations",
    )
    fig_map.update_geos(fitbounds="locations", visible=False)
    fig_map.update_layout(
        height=600,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        coloraxis_colorbar=dict(
            thickness=15,
            len=0.6,
            x=1.0
        )
    )

    fig_map.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_map, use_container_width=True)

refresh_interval = 300
st.sidebar.divider()
st.sidebar.caption("Auto-refreshes every 40 seconds")
if st.sidebar.button("🔄 Refresh Now"):
    st.cache_data.clear()
    st.rerun()