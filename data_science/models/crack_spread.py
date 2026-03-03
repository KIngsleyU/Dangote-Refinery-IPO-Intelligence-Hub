# data_science/models/crack_spread.py

"""
Crack spread valuation model for the Dangote Refinery IPO Intelligence Hub.

This module implements a local, database-backed version of the **3-2-1 crack
spread** calculation described in the project blueprint (`Dangote Refinery IPO
Intelligence Hub.pdf`). The crack spread is a standard refining economics proxy
for gross margin: it approximates the value of refined product outputs minus the
cost of crude inputs, normalized per barrel of crude processed.

## What this captures (and why)

Ahead of the 2026 Dangote Refinery IPO, a key source of information asymmetry is
the facility’s *effective refining margin* under changing crude prices, product
prices, and FX conditions. By continuously calculating a crack spread time
series, the hub can:

- Track margin expansion/contraction as a real-time driver of valuation.
- Provide a quantitative backbone for downstream agents (equity research,
  dividend sustainability, scenario analysis).
- Produce a durable metric for dashboards and alerts.

## Data dependencies

The model reads from the local SQLite database created by
`data_engineering/db_setup.py` and populated by `data_engineering/etl_loader.py`.

It expects rows in `market_data` for synchronized timestamps (minute bars) for:

- **Crude input**: Brent crude futures (`BZ=F`) priced in **USD/barrel**
- **Product outputs**:
  - RBOB gasoline futures (`RB=F`) priced in **USD/gallon**
  - NY Harbor ULSD / heating oil futures (`HO=F`) priced in **USD/gallon**

## Formula and unit handling

Because refined products are commonly quoted per gallon while crude is quoted
per barrel, the model converts product prices to a per-barrel basis using:

- \(1 \text{ barrel} = 42 \text{ gallons}\)

It then applies the 3-2-1 crack spread approximation:

\[
\\text{crack} = \\frac{(2 \\times P_{gas} + 1 \\times P_{distillate}) - (3 \\times P_{crude})}{3}
\]

The resulting `crack_spread_per_bbl` is an estimated **USD margin per barrel of
crude**.

## Outputs

The latest computed value is persisted to the `model_outputs` table as:

- `model_name`: `3-2-1_Crack_Spread`
- `metric_name`: `USD_margin_per_barrel`
- `metric_value`: latest crack spread value

This provides a simple integration point for other parts of the hub (agents,
dashboards, alerts) without requiring re-computation.

## Execution

Run as a script from the repository root:

```bash
python data_science/models/crack_spread.py
```

If the database is missing, empty, or the required tickers are not present, the
model returns an empty DataFrame and does not write outputs.
"""

import sqlite3
import pandas as pd
from datetime import datetime
import os

class CrackSpreadModel:
    """
    A quantitative valuation model to calculate the real-time 3-2-1 crack spread,
    representing the gross margin a refinery makes per barrel of crude oil processed.
    """
    def __init__(self, db_filename="dangote_hub.db"):
        # Dynamically locate the database sitting in the data_engineering directory
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.db_path = os.path.join(base_dir, "data_engineering", db_filename)

    def fetch_synchronized_data(self) -> pd.DataFrame:
        """
        Executes a multi-table SQL JOIN to align the precise minute-by-minute 
        timestamps of our raw material inputs and refined outputs.
        """
        query = """
            SELECT 
                c.timestamp,
                c.close_price AS crude_price,
                g.close_price AS gas_price,
                h.close_price AS heating_oil_price
            FROM market_data c
            JOIN market_data g ON c.timestamp = g.timestamp
            JOIN market_data h ON c.timestamp = h.timestamp
            WHERE c.ticker_symbol = 'BZ=F' 
              AND g.ticker_symbol = 'RB=F' 
              AND h.ticker_symbol = 'HO=F'
            ORDER BY c.timestamp ASC
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(query, conn)
            return df
        except sqlite3.OperationalError as e:
            print(f"Database connection error: {e}")
            return pd.DataFrame()

    def calculate_321_spread(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the financial engineering formula. 
        Note: Gasoline and Heating Oil are priced in $/gallon. 
        Crude is priced in $/barrel. There are 42 gallons in a barrel.
        """
        if df.empty:
            print("⚠️ No synchronized data available to calculate spread.")
            return df
            
        # 1. Convert refined product pricing from gallons to barrels
        df['gas_price_bbl'] = df['gas_price'] * 42
        df['heating_oil_price_bbl'] = df['heating_oil_price'] * 42
        
        # 2. Apply the 3-2-1 Formula: ((2 * Gas + 1 * Heating Oil) - (3 * Crude)) / 3
        # This outputs the estimated margin per barrel of crude oil.
        df['crack_spread_per_bbl'] = (
            (2 * df['gas_price_bbl'] + 1 * df['heating_oil_price_bbl']) - 
            (3 * df['crude_price'])
        ) / 3
        
        return df

    def save_outputs(self, df: pd.DataFrame):
        """Sinks the final calculated metric back into the SQLite model_outputs table."""
        if df.empty or 'crack_spread_per_bbl' not in df.columns:
            return
            
        # Isolate the most recent calculation
        latest_record = df.iloc[-1]
        
        insert_query = """
            INSERT INTO model_outputs (model_name, calculation_date, metric_name, metric_value, notes)
            VALUES (?, ?, ?, ?, ?)
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                insert_query, 
                (
                    "3-2-1_Crack_Spread", 
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                    "USD_margin_per_barrel", 
                    float(latest_record['crack_spread_per_bbl']), 
                    f"Derived from market timestamp: {latest_record['timestamp']}"
                )
            )
            conn.commit()
            
        print(f"✅ Executed Valuation: Latest 3-2-1 Crack Spread (${latest_record['crack_spread_per_bbl']:.2f}/bbl) saved to model_outputs.")

    def run(self):
        """Orchestrates the internal state and execution of the model."""
        print("Initializing Crack Spread Valuation Model...")
        df = self.fetch_synchronized_data()
        df_calculated = self.calculate_321_spread(df)
        self.save_outputs(df_calculated)

if __name__ == "__main__":
    model = CrackSpreadModel()
    model.run()