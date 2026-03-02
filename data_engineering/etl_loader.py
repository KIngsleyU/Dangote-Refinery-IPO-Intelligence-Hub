import sqlite3
import json
import glob
import os

def load_json_to_db():
    db_path = "./data_engineering/dangote_hub.db"
    raw_data_dir = "./data_engineering/raw_data"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Find all JSON files in the raw_data directory
    json_files = glob.glob(os.path.join(raw_data_dir, "*.json"))
    
    records_inserted = 0
    
    for file_path in json_files:
        # Extract the asset name from the filename (e.g., brent_crude_20260302_101500.json -> brent_crude)
        filename = os.path.basename(file_path)
        asset_name = "_".join(filename.split("_")[:-2]) 
        
        with open(file_path, 'r') as f:
            data = json.load(f)
            
            for row in data:
                # Use INSERT OR IGNORE to prevent duplicate entries based on our UNIQUE constraint
                cursor.execute('''
                    INSERT OR IGNORE INTO market_data 
                    (ticker_symbol, asset_name, timestamp, open_price, high_price, low_price, close_price, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row.get('symbol', 'UNKNOWN'), # You might need to map tickers if yfinance JSON drops them
                    asset_name,
                    row['Datetime'],
                    row['Open'],
                    row['High'],
                    row['Low'],
                    row['Close'],
                    row['Volume']
                ))
                if cursor.rowcount > 0:
                    records_inserted += 1
                    
    conn.commit()
    conn.close()
    print(f"✅ ETL Complete: {records_inserted} new records inserted into the database.")

if __name__ == "__main__":
    load_json_to_db()