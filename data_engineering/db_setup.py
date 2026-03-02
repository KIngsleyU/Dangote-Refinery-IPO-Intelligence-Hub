# data_engineering/db_setup.py

"""
Database schema initialization for the Dangote Refinery IPO Intelligence Hub.

This module defines a lightweight SQLite-backed `DatabaseManager` responsible for
creating and maintaining the core analytical schema used by the hub. It
provisions:

- `market_data`: stores historical and intraday price series for tracked
  tickers (crude, refined products, FX, comparables), keyed by
  `(ticker_symbol, timestamp)` for time-series analytics.
- `model_outputs`: stores derived metrics from downstream data science and
  agentic workflows (e.g., crack spreads, profitability estimates, and
  valuation signals).

The module can be executed as a script to create or update the schema in
`data_engineering/dangote_hub.db`, providing a simple local warehouse for
experimentation before migrating to cloud databases.
"""

import sqlite3
import os

class DatabaseManager:
    def __init__(self, db_name="dangote_hub.db"):
        # Store the database in the data_engineering directory
        self.db_path = os.path.join("./data_engineering", db_name)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def setup_schema(self):
        """Creates the foundational tables for the intelligence hub."""
        print("Initializing database schema...")
        
        # Table for historical and real-time pricing data
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker_symbol TEXT NOT NULL,
                asset_name TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume INTEGER,
                UNIQUE(ticker_symbol, timestamp)
            )
        ''')
        
        # Table to store the calculated outputs from our Data Science models
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS model_outputs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT NOT NULL,
                calculation_date DATETIME NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                notes TEXT
            )
        ''')

        # Create indexes for faster querying by the AI agents
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_time ON market_data(ticker_symbol, timestamp)')
        
        self.conn.commit()
        print(f"✅ Schema created successfully at {self.db_path}")

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    db = DatabaseManager()
    db.setup_schema()
    db.close()