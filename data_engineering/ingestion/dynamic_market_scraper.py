# data_engineering/ingestion/dynamic_market_scraper.py
"""
Parallel FX (USDT/NGN) scraper. Uses Bybit P2P; Binance discontinued
all Nigerian Naira (NGN) services in March 2024.
"""
from seleniumbase import SB
import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone

class DynamicMarketExtractor:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)

    def _looks_like_ngn_rate(self, rate_str: str) -> bool:
        """NGN per USDT is typically 1,xxx; AED is ~3.67. Reject AED-range."""
        s = (rate_str or "").strip().replace(",", "")
        if not s:
            return False
        try:
            val = float(s)
            return 100 <= val <= 5000
        except ValueError:
            return False

    def _parse_rate_value(self, rate_str: str) -> float | None:
        """Extract numeric rate from strings like '1,417.08 NGN' or '1540.99'. Returns None if just 'NGN' or no number."""
        if not rate_str or not rate_str.strip():
            return None
        s = rate_str.strip()
        if s.upper() == "NGN" or re.match(r"^[A-Za-z\s]+$", s):
            return None
        # Allow digits, comma, dot
        num_part = re.sub(r"[^\d.,]", "", s).replace(",", "")
        if not num_part:
            return None
        try:
            return float(num_part)
        except ValueError:
            return None

    def extract_parallel_fx_rates_v2(self):
        """
        Legacy: Binance P2P. NGN no longer supported (Mar 2024); page shows AED.
        Use extract_parallel_fx_rates() for Bybit P2P USDT/NGN.
        """
        print("Initializing Undetected Chromedriver for P2P FX scraping...")
        # UC mode masks webdriver presence and solves JS challenges automatically
        with SB(uc=True, headless=False) as sb:
            url = "https://p2p.binance.com/en/trade/all-payments/USDT?fiat=NGN"
            
            # Open with reconnect logic to bypass initial WAF interstitial pages
            sb.uc_open_with_reconnect(url, reconnect_time=4)
            
            # Wait for dynamic DOM elements representing price rows to load
            try:
                # The exact CSS selector may require periodic updating based on site changes
                sb.wait_for_element_visible("div.css-1m1f8hn", timeout=20)
                
                # Extract raw text from price elements
                prices = sb.find_elements("div.css-1m1f8hn")
                extracted_rates = [p.text.strip() for p in prices[:5] if p.text.strip()]
                
                result = {
                    "asset": "USDT/NGN",
                    "source": "binance_p2p",
                    "timestamp": time.time(),
                    "timestamp_iso": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "top_5_rates": extracted_rates
                }
                
                out_path = os.path.join(self.storage_dir, "parallel_fx_rates.json")
                with open(out_path, "w") as f:
                    json.dump(result, f, indent=4)
                    
                print("Successfully Extracted Latest NGN/USDT Parallel Market Rates.")
                return result
                
            except Exception as e:
                print(f"Failed to locate dynamic elements. WAF block or DOM change: {e}")
        
    def extract_parallel_fx_rates(self):
        """
        Extract USDT/NGN P2P rates from Bybit P2P. Binance discontinued NGN (Mar 2024).
        """
        print("Initializing Undetected Chromedriver for P2P FX scraping (Bybit USDT/NGN)...")
        with SB(uc=True, headless=False, timeout_multiplier=2.0) as sb:
            # Use sell page (sell USDT → get NGN); /trade/ is empty. Fallback: /buy/USDT/NGN
            url = "https://www.bybit.com/en/p2p/sell/USDT/NGN"

            for attempt in range(3):
                try:
                    print(f"Opening Bybit P2P USDT/NGN page (attempt {attempt+1})...")
                    sb.open(url)
                    sb.sleep(6)
                    # Bybit P2P: try common patterns for price cells (table or div-based)
                    for selector in (
                        "div.headline5.text-primaryText",
                        "[class*='price']",
                        "table tbody tr td:nth-child(2)",
                        "table tbody tr td",
                        "div.css-1m1f8hn",
                        "[class*='AdvertisedPrice']",
                        "table tbody tr",
                    ):
                        try:
                            sb.wait_for_element_visible(selector, timeout=12)
                            break
                        except Exception:
                            continue
                    else:
                        raise Exception("No price/table element found with any selector")
                    break
                except Exception as e:
                    print(f"Open attempt {attempt+1} failed: {e}")
                    if attempt == 2:
                        raise
                    sb.sleep(5)

            price_selectors = (
                "div.headline5.text-primaryText",
                "[class*='price']",
                "table tbody tr td:nth-child(2)",
                "table tbody tr td",
                "div.css-1m1f8hn",
                "[class*='AdvertisedPrice']",
                "table tbody tr",
            )
            prices = []
            for sel in price_selectors:
                try:
                    prices = sb.find_elements(sel)
                    if prices:
                        break
                except (asyncio.TimeoutError, TimeoutError, Exception):
                    continue
            if not prices:
                raise Exception("No price elements found with any selector")
            raw_rates = [p.text.strip() for p in prices[:10] if p.text.strip()]
            # Parse numeric rate from "1,417.08 NGN" or "1540.99"; drop "NGN"-only labels
            parsed = []
            for r in raw_rates:
                val = self._parse_rate_value(r)
                if val is not None and 100 <= val <= 5000:
                    parsed.append(f"{val:.2f}")
            extracted_rates = parsed[:5]

            # If sell page had no NGN-like rates, try buy page once
            if not extracted_rates and not raw_rates:
                print("Sell page had no rates; trying buy page...")
                sb.open("https://www.bybit.com/en/p2p/buy/USDT/NGN")
                sb.sleep(6)
                for sel in price_selectors:
                    try:
                        prices = sb.find_elements(sel)
                        if prices:
                            raw_rates = [p.text.strip() for p in prices[:10] if p.text.strip()]
                            parsed = []
                            for r in raw_rates:
                                val = self._parse_rate_value(r)
                                if val is not None and 100 <= val <= 5000:
                                    parsed.append(f"{val:.2f}")
                            extracted_rates = parsed[:5]
                            if not extracted_rates and raw_rates:
                                extracted_rates = raw_rates[:5]
                            break
                    except (asyncio.TimeoutError, TimeoutError, Exception):
                        continue

            if not extracted_rates and raw_rates:
                # Fallback: keep only raw entries that contain a number (skip "NGN"-only)
                with_number = [r for r in raw_rates if re.search(r"\d", r)][:5]
                if with_number:
                    extracted_rates = with_number
                else:
                    extracted_rates = raw_rates[:5]
                print("Warning: No rates in NGN range (100–5000). Got: %s. Saving; verify source." % extracted_rates)

            result = {
                "asset": "USDT/NGN",
                "source": "bybit_p2p",
                "timestamp": time.time(),
                "timestamp_iso": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "top_5_rates": extracted_rates,
            }

            out_path = os.path.join(self.storage_dir, "parallel_fx_rates.json")
            with open(out_path, "w") as f:
                json.dump(result, f, indent=4)

            print("Successfully Extracted Latest NGN/USDT Parallel Market Rates (Bybit P2P).")
            return result
                
if __name__ == "__main__":
    extractor = DynamicMarketExtractor(storage_dir="data_engineering/ingestion/data")
    extractor.extract_parallel_fx_rates()