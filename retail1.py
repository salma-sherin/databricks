import streamlit as st
import pandas as pd
from databricks.sql import connect


# ------------------ PAGE CONFIG ------------------
st.set_page_config(
    page_title="Online Retail Analytics",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Online Retail Analytics Dashboard")

# ------------------ DATABRICKS CONNECTION ------------------
conn = connect(
    server_hostname=st.secrets["databricks"]["server_hostname"],
    http_path=st.secrets["databricks"]["http_path"],
    access_token=st.secrets["databricks"]["access_token"]
)

# ------------------ KPI SECTION ------------------
kpi_query = """
SELECT
    ROUND(SUM(daily_revenue), 2) AS total_revenue,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue
FROM gold_daily_sales
"""

kpi_df = pd.read_sql(kpi_query, conn)

col1, col2, col3 = st.columns(3)
col1.metric("💰 Total Revenue", f"₹ {kpi_df.iloc[0,0]:,.2f}")
col2.metric("🧾 Total Orders", f"{int(kpi_df.iloc[0,1]):,}")
col3.metric("📈 Avg Daily Revenue", f"₹ {kpi_df.iloc[0,2]:,.2f}")

st.divider()

# ------------------ DATE FILTER ------------------
date_query = "SELECT MIN(order_date), MAX(order_date) FROM gold_daily_sales"
date_df = pd.read_sql(date_query, conn)

start_date, end_date = st.date_input(
    "📅 Select Date Range",
    [date_df.iloc[0,0], date_df.iloc[0,1]]
)

# ------------------ TABS ------------------
tab1, tab2, tab3 = st.tabs(["📅 Sales Trends", "🛍 Top Products", "🌍 Country Analysis"])

# ------------------ TAB 1: DAILY & MONTHLY SALES ------------------
with tab1:
    st.subheader("Daily Revenue Trend")

    daily_query = f"""
    SELECT order_date, daily_revenue, total_orders
    FROM gold_daily_sales
    WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY order_date
    """
    daily_df = pd.read_sql(daily_query, conn)

    st.line_chart(daily_df.set_index("order_date")[["daily_revenue"]])

    st.subheader("Monthly Revenue")

    monthly_query = """
    SELECT order_month, monthly_revenue
    FROM gold_monthly_sales
    ORDER BY order_month
    """
    monthly_df = pd.read_sql(monthly_query, conn)

    st.bar_chart(monthly_df.set_index("order_month"))

# ------------------ TAB 2: TOP PRODUCTS ------------------
with tab2:
    st.subheader("Top 10 Products by Revenue")

    product_query = """
    SELECT product_name, product_revenue
    FROM gold_top_products
    ORDER BY product_revenue DESC
    LIMIT 10
    """
    product_df = pd.read_sql(product_query, conn)

    st.bar_chart(product_df.set_index("product_name"))

    with st.expander("📄 View Product Data"):
        st.dataframe(product_df)

# ------------------ TAB 3: COUNTRY SALES ------------------
with tab3:
    st.subheader("Revenue by Country")

    country_query = """
    SELECT Country, country_revenue
    FROM gold_country_sales
    ORDER BY country_revenue DESC
    LIMIT 10
    """
    country_df = pd.read_sql(country_query, conn)

    st.bar_chart(country_df.set_index("Country"))

    with st.expander("📄 View Country Data"):
        st.dataframe(country_df)

# ------------------ FOOTER ------------------
st.divider()
st.caption("📌 Data Source: Databricks Delta Gold Layer | Built by Salma Sherin")

conn.close()


