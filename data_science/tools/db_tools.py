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

# We can easily add more tools here later, like get_latest_stock_price(ticker)
tools = [get_latest_model_output]

if __name__ == "__main__":
    # Test the tool, 
    print(get_latest_model_output.invoke({"metric_name": "USD_margin_per_barrel"}))