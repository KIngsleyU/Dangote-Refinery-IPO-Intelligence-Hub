# frontend/app.py

import streamlit as st
import requests
import sqlite3
import pandas as pd
import plotly.express as px
import os
import uuid

# --- Config & Setup ---
st.set_page_config(page_title="Dangote Refinery IPO Intelligence Hub", layout="wide")
API_URL = "http://127.0.0.1:8000/api/v1/query"

# Initialize session state for chat history and session ID
if "messages" not in st.session_state:
    st.session_state.messages = []
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

# --- Database Helper for Visualization ---
def fetch_historical_margins():
    """Fetches the time-series crack spread data directly from local SQLite for charting."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, "data_engineering", "dangote_hub.db")
    
    try:
        with sqlite3.connect(db_path) as conn:
            query = "SELECT calculation_date, metric_value FROM model_outputs WHERE metric_name = 'USD_margin_per_barrel' ORDER BY calculation_date ASC"
            df = pd.read_sql_query(query, conn)
            # Convert string dates to actual datetime objects for Plotly
            df['calculation_date'] = pd.to_datetime(df['calculation_date'])
            return df
    except Exception as e:
        st.error(f"Failed to load chart data: {e}")
        return pd.DataFrame()

# --- Layout: Two Columns ---
col1, col2 = st.columns([1, 1.5])

# Left Column: Data Visualization
with col1:
    st.title("📊 Hub Analytics")
    st.markdown("Real-time 3-2-1 Crack Spread (USD/bbl)")
    
    df_chart = fetch_historical_margins()
    if not df_chart.empty:
        fig = px.line(df_chart, x='calculation_date', y='metric_value', markers=True, 
                      title="Estimated Gross Margin per Barrel", template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No margin data available yet. Run the ETL and valuation models.")

# Right Column: Multi-Agent Chat Interface
with col2:
    st.title("🤖 Quant Agent Terminal")
    
    # Display chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Chat Input
    if prompt := st.chat_input("Ask about the refinery's valuation, margins, or IPO prospects..."):
        # Add user prompt to UI
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Call FastAPI Backend
        with st.chat_message("assistant"):
            with st.spinner("Agent is analyzing data..."):
                try:
                    payload = {"query": prompt, "session_id": st.session_state.session_id}
                    response = requests.post(API_URL, json=payload)
                    
                    if response.status_code == 200:
                        agent_reply = response.json().get("answer", "No response generated.")
                        st.markdown(agent_reply)
                        st.session_state.messages.append({"role": "assistant", "content": agent_reply})
                    else:
                        st.error(f"API Error {response.status_code}: {response.text}")
                except Exception as e:
                    st.error(f"Failed to connect to the Agent API. Is the FastAPI server running? Error: {e}")