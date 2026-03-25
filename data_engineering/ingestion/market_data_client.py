# data_engineering/ingestion/market_data_client.py

"""
Asynchronous market data ingestion client for the Dangote Refinery IPO
Intelligence Hub.

This module defines the `MarketDataIngestor` class, which:

- Fetches real-time and near real-time market data from:
  - Yahoo Finance (via `yfinance`) for:
    - Brent crude futures (`BZ=F`) as the primary input cost.
    - Refined product futures (`RB=F`, `HO=F`) to proxy refinery output revenue.
    - Listed independent refiners (`VLO`, `MPC`, `PSX`) as valuation comparables.
    - Nigeria-linked macro proxies (e.g. `USDNGN=X`, `SEPL.L`, `AFK`) for FX and
      sovereign risk.
  - Optional premium feeds (NGX, Argus) when API keys are configured, with data
    pushed directly to an S3 data lake.
- Persists raw JSON snapshots either to local storage under
  `data_engineering/raw_data` or to an AWS S3 bucket for downstream Glue/Athena
  processing.

The `run_pipeline` coroutine orchestrates all active data sources concurrently
using `asyncio` and `aiohttp`, making this module the ingestion gateway for the
hub’s quantitative models.
"""

import asyncio
import aiohttp
import os
import json
from datetime import datetime
import boto3
import yfinance as yf
import requests
from bs4 import BeautifulSoup
class MarketDataIngestor:
    def __init__(self):
        # We will use boto3 to push raw payloads directly into an S3 data lake
        self.s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION', 'us-east-1'))
        self.bucket_name = os.getenv('RAW_DATA_BUCKET', 'dangote-hub-raw-zone')
        
        # API Keys loaded from the .env file
        self.ngx_api_key = os.getenv('NGX_API_KEY')
        self.argus_api_key = os.getenv('ARGUS_API_KEY')
        
        # We will save locally for now before pushing to AWS S3
        self.local_storage_path = "./data_engineering/raw_data"
        os.makedirs(self.local_storage_path, exist_ok=True)
        
    def _save_locally(self, data: list, source: str):
        """Saves the raw JSON payload to our local directory."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = f"{self.local_storage_path}/{source}_{timestamp}.json"
        
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
    
    async def _upload_to_s3(self, data: dict, source: str, file_prefix: str):
        """Sinks the raw JSON payload into our AWS S3 Data Lake."""
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        file_key = f"{source}/{file_prefix}_{timestamp}.json"
        
        # In a real environment, you might use a memory buffer here instead of dumping to a string
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=file_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )

    async def fetch_ngx_equity_data(self, session: aiohttp.ClientSession):
        """Fetches real-time NGX equity data, specifically tracking energy and banking sectors."""
        url = "https://api.ngxgroup.com/marketdata/v3/equities/live"
        headers = {"Authorization": f"Bearer {self.ngx_api_key}"}
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    await self._upload_to_s3(data, "ngx_equities", "equity_snapshot")
                    print("✅ NGX Data successfully ingested and stored in S3.")
                else:
                    print(f"❌ NGX API Error: {response.status}")
        except Exception as e:
            print(f"Failed to fetch NGX data: {str(e)}")

    async def fetch_argus_crude_prices(self, session: aiohttp.ClientSession):
        """Fetches real-time Brent Crude and Refined Product crack spreads."""
        url = "https://api.argusmedia.com/v1/prices/latest"
        headers = {"x-api-key": self.argus_api_key}
        # Targeting specific commodity codes for Brent and Ultra-Low Sulfur Diesel (ULSD)
        payload = {"symbols": ["BRENT_CRUDE", "ULSD_CIF_WAF"]} 
        
        try:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    await self._upload_to_s3(data, "argus_commodities", "crack_spreads")
                    print("✅ Argus Commodity Data successfully ingested.")
                else:
                    print(f"❌ Argus API Error: {response.status}")
        except Exception as e:
            print(f"Failed to fetch Argus data: {str(e)}")

    async def fetch_yahoo_finance_data(self, ticker_symbol: str, name: str):
        """Fetches market data using Yahoo Finance."""
        print(f"Fetching data for {name} ({ticker_symbol})...")
        
        # yfinance is synchronous, but we wrap it in an async function for our pipeline structure
        ticker = yf.Ticker(ticker_symbol)
        data = ticker.history(period="1d", interval="1m") # 1-day data, 1-minute intervals
        
        if not data.empty:
            # Convert pandas dataframe to dictionary for JSON storage
            data_dict = data.reset_index().to_dict(orient="records")
            
            # Clean timestamp objects to strings
            for row in data_dict:
                row['Datetime'] = str(row['Datetime'])
                
            # Add the ticker_symbol to the data_dict
            data_dict[0]['ticker_symbol'] = ticker_symbol
            
            self._save_locally(data_dict, name)
            print(f"✅ {name} data successfully ingested.")
        else:
            print(f"⚠️ No data returned for {name} ({ticker_symbol}); skipping.")

    async def fetch_parallel_usd_ngn(self, session: aiohttp.ClientSession):
        """Fetches the real-time USD/NGN street rate via Binance P2P."""
        url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
        # Using a more robust User-Agent to bypass basic bot protection
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        payload = {
            "proMerchantAds": False,
            "page": 1,
            "rows": 5,
            "payTypes": [],
            "countries": [],
            "publisherType": None,
            "asset": "USDT",
            "fiat": "NGN",
            "tradeType": "BUY"
        }
        
        try:
            async with session.post(url, headers=headers, json=payload) as response:
                data = await response.json()
                
                if data.get('data'):
                    top_rate = float(data['data'][0]['adv']['price'])
                    print(f"✅ Live Parallel Market Rate: ₦{top_rate} / $1")
                    
                    # Format to match yfinance JSON structure so ETL loader doesn't break
                    record = [{
                        "ticker_symbol": "USDNGN_P2P",
                        "Datetime": str(datetime.now()),
                        "Open": top_rate, "High": top_rate, "Low": top_rate, "Close": top_rate, "Volume": 0
                    }]
                    self._save_locally(record, "parallel_usd_ngn")
                else:
                    print("⚠️ Binance P2P blocked the request or returned empty. (Will need proxy rotation in Prod)")
        except Exception as e:
            print(f"Failed to fetch parallel FX rate: {e}")

    async def fetch_sovereign_bond_yield(self, session: aiohttp.ClientSession):
        """Scrapes the Nigerian 10-Year Government Bond Yield from TradingEconomics."""
        url = "https://tradingeconomics.com/nigeria/government-bond-yield"
        headers = {"User-Agent": "Mozilla/5.0"}
        
        try:
            async with session.get(url, headers=headers) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Find the specific table cell holding the yield value
                yield_element = soup.find('td', {'id': 'p'})
                if yield_element:
                    bond_yield = float(yield_element.text.strip())
                    print(f"✅ Nigerian 10-Year Sovereign Yield: {bond_yield}%")
                    
                    # Format to match yfinance JSON structure
                    record = [{
                        "ticker_symbol": "NG10Y_BOND",
                        "Datetime": str(datetime.now()),
                        "Open": bond_yield, "High": bond_yield, "Low": bond_yield, "Close": bond_yield, "Volume": 0
                    }]
                    self._save_locally(record, "nigeria_10y_bond")
                else:
                    print("⚠️ Could not locate bond yield on page.")
        except Exception as e:
            print(f"Failed to scrape bond yield: {e}")

    async def run_pipeline(self):
        """Orchestrates the asynchronous data gathering."""
        async with aiohttp.ClientSession() as session:
            tasks = [
                # 1. The Input (Cost)
                self.fetch_yahoo_finance_data("BZ=F", "brent_crude"),
                
                # 2. The Outputs (Revenue)
                self.fetch_yahoo_finance_data("RB=F", "rbob_gasoline"),
                self.fetch_yahoo_finance_data("HO=F", "ulsd_heating_oil"),
                
                # 3. The Competitor Proxies (Valuation Multiples)
                self.fetch_yahoo_finance_data("VLO", "valero_energy"),
                self.fetch_yahoo_finance_data("MPC", "marathon_petroleum"),
                self.fetch_yahoo_finance_data("PSX", "phillips_66"),
                
                # 4. Macro & Sovereign Risk
                self.fetch_yahoo_finance_data("USDNGN=X", "usd_ngn_fx_rate"),
                self.fetch_yahoo_finance_data("SEPL.L", "seplat_energy_nigeria"),
                self.fetch_yahoo_finance_data("AFK", "africa_macro_proxy"),
                
                # Pass the aiohttp session to our new web scrapers
                self.fetch_parallel_usd_ngn(session),
                self.fetch_sovereign_bond_yield(session)
            ]
            await asyncio.gather(*tasks)

if __name__ == "__main__":
    # Local execution entry point
    ingestor = MarketDataIngestor()
    asyncio.run(ingestor.run_pipeline())