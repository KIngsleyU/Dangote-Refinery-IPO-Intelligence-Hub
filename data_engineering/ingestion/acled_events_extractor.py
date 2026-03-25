# data_engineering/ingestion/acled_events_extractor.py
"""
Event‑level ACLED ingest for Nigeria.

WHAT THIS IS FOR
----------------
The Dangote Refinery IPO Intelligence Hub uses conflict and geopolitical context
to link localised events (e.g. Niger Delta pipeline vandalism, protests, violence
against civilians) to macro and oil-sector outcomes. This script pulls ACLED
*event-level* data for Nigeria so the RAG pipeline can:
- Correlate spikes in conflict with NUPRC production or refinery risk narratives.
- Answer questions like “Where have recent incidents been?” or “How did violence
  in the Delta trend last year?” using concrete events, not only HDX aggregates.

Your HDX pipeline (geopolitics_extractor.py) already gives monthly HRP *aggregates*;
this script adds *incident-level* detail (actor, location, date, event type) for
years your ACLED tier allows (Research = lagged, e.g. 12 months old).

PLAUSIBLE OUTPUT
----------------
- Raw: data_engineering/ingestion/data/acled_events/acled_events_nigeria_<timestamp>.jsonl
  One JSON object per line, e.g.:
  {"event_id_cnty":"NGA-123","event_date":"2025-01-15","event_type":"Violence against civilians","sub_event_type":"Attack","actor1":"Boko Haram","admin1":"Borno","admin2":"Maiduguri","fatalities":5,"latitude":11.8,"longitude":13.1,...}
- Summary: same path with .md — a table of event counts by month and event_type for RAG.

AUTH & TERMS
------------
- You MUST have a myACLED account and agree to ACLED’s Terms of Use.
- Either set `ACLED_ACCESS_TOKEN` in env, or set `EMAIL` and `PASSWORD`
  (myACLED login); the script will obtain a short-lived OAuth token automatically.
- This module stores data locally under `data_engineering/ingestion/data/acled_events`
  and is intended only for internal modelling, not re‑publishing of raw ACLED data.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import dotenv
import pandas as pd
import requests

dotenv.load_dotenv()


ACLED_BASE_URL = "https://acleddata.com/api/acled/read"
ACLED_OAUTH_URL = "https://acleddata.com/oauth/token"


def get_acled_token(email: str, password: str) -> Dict:
    """Obtain OAuth access_token (and refresh_token) from ACLED using password grant."""
    payload = {
        "username": email,
        "password": password,
        "grant_type": "password",
        "client_id": "acled",
    }
    resp = requests.post(ACLED_OAUTH_URL, data=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_access_token() -> str:
    """
    Return a Bearer token for ACLED API.
    Uses ACLED_ACCESS_TOKEN if set; otherwise obtains one via EMAIL + PASSWORD from env.
    """
    token = os.getenv("ACLED_ACCESS_TOKEN")
    if token:
        return token.strip()
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")
    if not email or not password:
        raise SystemExit(
            "Set either ACLED_ACCESS_TOKEN or both EMAIL and PASSWORD (myACLED) in .env"
        )
    tokens = get_acled_token(email, password)
    access = tokens.get("access_token")
    if not access:
        raise SystemExit("ACLED OAuth response missing access_token.")
    return access


@dataclass
class ACLEDEventsConfig:
    """Configuration for ACLED event‑level ingestion."""

    access_token: str
    storage_dir: Path
    country: str = "Nigeria"
    # Recent window to pull (rolling); ACLED caps total rows per request.
    days_back: int = 365
    # Max pages to fetch in a single run to avoid huge pulls by accident.
    max_pages: int = 50
    # Page size; ACLED default is 500, but we set explicitly.
    page_size: int = 500


class ACLEDEventsExtractor:
    def __init__(self, config: ACLEDEventsConfig) -> None:
        self.config = config
        self.config.storage_dir.mkdir(parents=True, exist_ok=True)

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.config.access_token}"}

    def _build_params(self, page: int, year: int) -> Dict[str, str]:
        """Build query params for one page. Use year allowed by your tier (Research = lagged, e.g. last year)."""
        params: Dict[str, str] = {
            "country": self.config.country,
            "year": str(year),
            "limit": str(self.config.page_size),
            "page": str(page),
            "_format": "json",
        }
        return params

    def fetch_recent_events(self) -> List[Dict]:
        """Fetch Nigeria events from ACLED API. Uses lagged years (e.g. 2024, 2025) when your tier restricts recency (e.g. 12 months old)."""
        from datetime import date
        this_year = date.today().year
        # Research tier often allows only data ≥12 months old; request previous 2 years to get rows.
        years_to_fetch = [this_year - 1, this_year - 2]
        all_events: List[Dict] = []
        for year in years_to_fetch:
            for page in range(1, self.config.max_pages + 1):
                params = self._build_params(page, year)
                resp = requests.get(ACLED_BASE_URL, headers=self._headers(), params=params, timeout=60)
                if resp.status_code != 200:
                    print(
                        f"ACLED API request failed (year={year}, page={page}) "
                        f"({resp.status_code}): {resp.text[:300]}"
                    )
                    if resp.status_code == 403:
                        print(
                            "403 Access denied: API access requires a myACLED Research, Partner, or "
                            "Enterprise tier (not Open/Public). Request access at "
                            "https://developer.acleddata.com/ or https://acleddata.com/contact/"
                        )
                    break

                payload = resp.json()
                if isinstance(payload, list):
                    data = payload
                else:
                    data = payload.get("data") or []

                if not data and page == 1:
                    if isinstance(payload, dict):
                        print(f"Debug: API returned keys: {list(payload.keys())}")
                        if payload.get("messages"):
                            print(f"ACLED messages: {payload['messages']}")
                        if payload.get("data_query_restrictions"):
                            print(f"ACLED data_query_restrictions: {payload['data_query_restrictions']}")
                        if payload.get("count") is not None:
                            print(f"ACLED count: {payload['count']}, total_count: {payload.get('total_count')}")
                    else:
                        print(f"Debug: API returned type {type(payload).__name__}, len={len(payload)}")
                if not data:
                    print(f"No more events (year={year}, page={page}); moving on.")
                    break

                all_events.extend(data)
                print(f"Fetched {len(data)} events from ACLED (year={year}, page={page}).")

                if len(data) < self.config.page_size:
                    break

        print(f"Total ACLED events fetched: {len(all_events)}")
        return all_events

    def _save_raw_jsonl(self, events: Iterable[Dict]) -> Path:
        """Persist raw ACLED events as JSONL for internal analysis."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_path = self.config.storage_dir / f"acled_events_nigeria_{ts}.jsonl"
        with out_path.open("w", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(ev) + "\n")
        print(f"Wrote raw ACLED events to {out_path}")
        return out_path

    def _summarise_to_markdown(self, events: List[Dict], raw_path: Path) -> Optional[Path]:
        """Create a compact monthly event‑type summary for RAG ingestion."""
        if not events:
            print("No events fetched; skipping markdown summary.")
            return None

        df = pd.DataFrame(events)
        if df.empty or "event_date" not in df.columns or "event_type" not in df.columns:
            print("ACLED events missing expected columns; skipping markdown summary.")
            return None

        # Parse to datetime and create a month key
        df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
        df = df.dropna(subset=["event_date"])
        df["month"] = df["event_date"].dt.to_period("M").astype(str)

        # Focus on a simple matrix: month x event_type counts
        pivot = (
            df.groupby(["month", "event_type"])
            .size()
            .reset_index(name="event_count")
            .sort_values(["month", "event_type"])
        )

        # Keep the last 24 months of data to keep context small.
        unique_months = sorted(pivot["month"].unique())
        if len(unique_months) > 24:
            keep_months = set(unique_months[-24:])
            pivot = pivot[pivot["month"].isin(keep_months)]

        md_table = pivot.to_markdown(index=False)
        md_path = raw_path.with_suffix(".md")

        with md_path.open("w", encoding="utf-8") as f:
            f.write("# ACLED Nigeria Event‑Level Summary (internal use only)\n\n")
            f.write(
                "> Source: ACLED API. Internal analytics only; comply with ACLED Terms of Use for any "
                "external sharing.\n\n"
            )
            f.write(md_table)

        print(f"Generated ACLED event summary markdown at {md_path}")
        return md_path

    def run(self) -> None:
        """High‑level entry: fetch, persist raw, and summarise."""
        events = self.fetch_recent_events()
        if not events:
            print("No events to save; skipping file write.")
            return
        raw_path = self._save_raw_jsonl(events)
        self._summarise_to_markdown(events, raw_path)


if __name__ == "__main__":
    token = get_access_token()
    base_storage = Path("data_engineering/ingestion/data/acled_events")
    cfg = ACLEDEventsConfig(access_token=token, storage_dir=base_storage)
    extractor = ACLEDEventsExtractor(cfg)
    extractor.run()

