# data_science/tools/db_tools.py

"""
Database access tools for agentic workflows in the Dangote Refinery IPO
Intelligence Hub.

This module provides small, composable utilities (exposed as LangChain tools)
that let LLM/agent components read curated results from the hub’s local SQLite
warehouse, `data_engineering/dangote_hub.db`.

## Purpose

Downstream models (e.g. `data_science/models/crack_spread.py`) write derived
metrics into the `model_outputs` table. Agentic systems can then query those
metrics via a stable interface without re-running the full computation.

## What’s included

- `get_latest_model_output(metric_name)`: returns the most recent value for a
  given `metric_name` (e.g. `USD_margin_per_barrel`) along with its calculation
  timestamp.

The database path is resolved dynamically relative to the repository root so
this tool can run from any working directory.
"""

import sqlite3
import os
from langchain_core.tools import tool

# Dynamically locate the database
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data_engineering", "dangote_hub.db")


@tool
def list_available_metrics() -> str:
    """
    Lists all metric names currently stored in model_outputs. Call this FIRST
    before get_latest_model_output so you know exactly which metrics exist.
    Only query metrics returned by this tool.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT metric_name FROM model_outputs ORDER BY metric_name"
            )
            rows = cursor.fetchall()
            if rows:
                names = [r[0] for r in rows]
                return f"Available metrics: {', '.join(names)}. Use get_latest_model_output with one of these exact names."
            return "No metrics in model_outputs yet. Run the crack spread model first."
    except Exception as e:
        return f"Database error: {str(e)}"


@tool
def list_available_tickers() -> str:
    """
    Lists all ticker symbols currently in market_data. Call this FIRST before
    get_latest_market_data so you know exactly which tickers exist.
    Only query tickers returned by this tool.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT ticker_symbol FROM market_data ORDER BY ticker_symbol"
            )
            rows = cursor.fetchall()
            if rows:
                symbols = [r[0] for r in rows]
                return f"Available tickers: {', '.join(symbols)}. Use get_latest_market_data with one of these exact symbols."
            return "No tickers in market_data yet. Run ingestion and ETL first."
    except Exception as e:
        return f"Database error: {str(e)}"


@tool
def get_latest_model_output(metric_name: str) -> str:
    """
    Fetches the most recently calculated financial metric from the intelligence hub's database.
    
    Args:
        metric_name: The exact name of the metric to look up (e.g., 'USD_margin_per_barrel').
        
    Returns:
        A string containing the latest metric value and calculation timestamp, 
        or an error message if the metric cannot be found.
    """
    query = """
        SELECT metric_value, calculation_date 
        FROM model_outputs 
        WHERE metric_name = ? 
        ORDER BY calculation_date DESC 
        LIMIT 1
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (metric_name,))
            result = cursor.fetchone()
            
            if result:
                value, calc_date = result
                return f"The latest {metric_name} is {value} (Calculated on: {calc_date})."
            else:
                return f"No data found for metric: {metric_name}."
    except Exception as e:
        return f"Database error occurred: {str(e)}"
    
@tool
def get_latest_market_data(ticker_symbol: str) -> str:
    """
    Fetches the most recent market data (close price) for a specific ticker symbol.
    
    Args:
        ticker_symbol: The symbol of the asset (e.g., 'BZ=F' for Brent Crude, 
                       'USDNGN_P2P' for parallel FX rate, 'NG10Y_BOND' for sovereign yield).
        
    Returns:
        A string containing the latest price and the exact timestamp it was recorded, 
        or an error message if the ticker cannot be found in the database.
    """
    query = """
        SELECT asset_name, close_price, timestamp 
        FROM market_data 
        WHERE ticker_symbol = ? 
        ORDER BY timestamp DESC 
        LIMIT 1
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (ticker_symbol,))
            result = cursor.fetchone()
            
            if result:
                asset_name, close_price, timestamp = result
                # We format this clearly so the LLM understands exactly what it is reading
                return f"The latest close price for {asset_name} ({ticker_symbol}) is {close_price} (Recorded on: {timestamp})."
            else:
                return f"No market data found in the database for ticker: {ticker_symbol}."
    except Exception as e:
        return f"Database error occurred while fetching {ticker_symbol}: {str(e)}"

# Discovery tools first so the agent knows what to query; then fetch tools
tools = [list_available_metrics, list_available_tickers, get_latest_model_output, get_latest_market_data]

if __name__ == "__main__":
    # Test the tool, 
    print(get_latest_model_output.invoke({"metric_name": "USD_margin_per_barrel"}))