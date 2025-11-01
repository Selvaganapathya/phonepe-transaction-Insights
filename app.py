# phonepe_streamlit_dashboard.py
# Fully working Streamlit Dashboard for PhonePe Pulse ETL

import streamlit as st
st.set_page_config(page_title="PhonePe Pulse Dashboard", layout="wide")

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import plotly.express as px
from typing import Dict, Optional

# -----------------------------------
# Configure DB (Edit credentials here)
# -----------------------------------
DB_URI = "mysql+pymysql://root:Admin%40123@localhost:3306/phonepe_db"

CSV_FALLBACKS = {
    "aggregated_user": None,
    "aggregated_transaction": None,
    "aggregated_insurance": None,
    "map_user": None,
    "map_map": None,
    "map_insurance": None,
    "top_user": None,
    "top_map": None,
    "top_insurance": None,
}

TABLES = list(CSV_FALLBACKS.keys())

# -----------------------------------
# Caching Utilities (✅ Correct Cache Use)
# -----------------------------------
@st.cache_resource
def get_engine(uri: str) -> Engine:
    return create_engine(
        uri,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10
    )

def test_connection(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False

@st.cache_data
def load_table(table_name: str, limit: int = 2000000) -> Optional[pd.DataFrame]:
    engine = get_engine(DB_URI)
    sql = f"SELECT * FROM `{table_name}` LIMIT {limit}"
    try:
        with engine.connect() as conn:
            return pd.read_sql(sql, conn)
    except Exception:
        csv_path = CSV_FALLBACKS.get(table_name)
        if csv_path:
            return pd.read_csv(csv_path)
        return None

def safe_load_all() -> Dict[str, Optional[pd.DataFrame]]:
    loaded = {}
    for t in TABLES:
        loaded[t] = load_table(t)
    return loaded

# -----------------------------------
# UI Setup
# -----------------------------------
st.title("📊 PhonePe Pulse — ETL Dashboard")
st.markdown("Explore PhonePe digital payments across Indian states with live SQL analysis.")

# Sidebar: Connection + Filters
with st.sidebar:
    st.header("Database Connection")
    connect_btn = st.button("Connect to Database")

engine = None

if connect_btn:
    with st.spinner("Connecting to DB..."):
        try:
            engine = get_engine(DB_URI)
            if test_connection(engine):
                st.sidebar.success("✅ Connected to DB successfully!")
            else:
                st.sidebar.error("❌ Connection failed!")
        except Exception as e:
            st.sidebar.error(f"⚠️ Error: {e}")
else:
    try:
        engine = get_engine(DB_URI)
        if test_connection(engine):
            st.sidebar.info("✅ Auto-connected to DB")
        else:
            st.sidebar.warning("⚠️ Database unreachable. CSV fallback if available.")
            engine = None
    except:
        engine = None

# -----------------------------------
# Load Data
# -----------------------------------
with st.spinner("📥 Loading data..."):
    data = safe_load_all()

# Sidebar Table Status
st.sidebar.markdown("### Table Status")
for t, df in data.items():
    if isinstance(df, pd.DataFrame):
        st.sidebar.write(f"✅ {t} ({df.shape[0]} rows)")
    else:
        st.sidebar.write(f"❌ {t}")

# -----------------------------------
# Analysis Section
# -----------------------------------
agg_tr = data.get("aggregated_transaction")

if isinstance(agg_tr, pd.DataFrame):
    st.markdown("## 🚀 Overall Performance")

    amount_col = next((c for c in agg_tr.columns if "amount" in c.lower()), None)
    count_col = next((c for c in agg_tr.columns if "count" in c.lower()), None)
    state_col = next((c for c in agg_tr.columns if "state" in c.lower()), None)

    if amount_col and count_col and state_col:
        total_amount = agg_tr[amount_col].sum()
        total_count = agg_tr[count_col].sum()

        col1, col2 = st.columns(2)
        col1.metric("Total Amount", f"{total_amount:,.0f}")
       

        # Top 10 States by Amount
        st.markdown("### 🏆 Top 10 States by Transaction Amount")
        top_states = agg_tr.groupby(state_col)[amount_col].sum().nlargest(10)
        fig = px.bar(top_states, x=top_states.values, y=top_states.index, orientation="h")
        st.plotly_chart(fig, use_container_width=True)

else:
    st.warning("aggregated_transaction table missing required columns")

# -----------------------------------
# Top Map Section
# -----------------------------------
st.markdown("## 🌟 Top Performers (top_map)")
top_map = data.get("top_map")

if isinstance(top_map, pd.DataFrame) and "State" in top_map.columns:
    rank_df = top_map.groupby("State")["Transaction_amount"].sum().sort_values(ascending=False).head(10)
    fig = px.bar(rank_df, x=rank_df.values, y=rank_df.index, orientation="h")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("top_map not available or missing 'State' & 'Transaction_amount' columns")

# -----------------------------------
# Insights
# -----------------------------------
st.markdown("## 🔍 Insights & Recommendations")
st.write("1. States with high transaction volume indicate strong digital adoption.")
st.write("2. Explore marketing opportunities in states with high amounts but lower user count.")
st.write("3. Enhance presence in tier-2 and tier-3 cities with growth potential.")

st.caption("Dashboard Version: Stable — DB optimized — Cached engine — Tested on MySQL 8")
