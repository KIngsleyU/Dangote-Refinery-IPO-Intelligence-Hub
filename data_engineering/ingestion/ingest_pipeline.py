# data_engineering/ingestion/ingest_pipeline.py

"""
data_engineering/ingestion/ingest_pipeline.py

Single-file, manifest-driven ingestion pipeline that:
 - downloads institutional PDFs
 - scrapes RSS/HTML article feeds (cleaned)
 - queries GDELT for events
 - optionally scrapes social media with snscrape (local user must install)
 - extracts text from PDFs and HTML
 - writes raw + text + metadata into a documents directory suitable for your
   existing RAG pipeline (SimpleDirectoryReader / LlamaIndex).
 - flags high-risk news for human review (quarantine)
"""

import os
import io
import json
import hashlib
import asyncio
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Optional
import aiohttp
import feedparser
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd
import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCS_DIR = os.path.join(BASE_DIR, "data_engineering", "documents", f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_new_and_media_data")
RAW_DIR = os.path.join(DOCS_DIR, "raw")
TEXT_DIR = os.path.join(DOCS_DIR, "text")
META_DIR = os.path.join(DOCS_DIR, "meta")
QUARANTINE_DIR = os.path.join(DOCS_DIR, "quarantine")

for d in (DOCS_DIR, RAW_DIR, TEXT_DIR, META_DIR, QUARANTINE_DIR):
    os.makedirs(d, exist_ok=True)

# Manifest: add or remove sources here. Keep it versioned in git.
MANIFEST = {
    "pdfs": {
        "PIA_2021": "https://www.nuprc.gov.ng/wp-content/uploads/2021/08/PETROLEUM-INDUSTRY-ACT-2021.pdf",
        "NNPC_AFS_2022": "https://cms1977.nnpcgroup.com/uploads/NNPC_CONSOLIDATED_AND_SEPARATE_FINANCIAL_STATEMENTS_AS_AT_31_ST_DECEMBER_2022_71c17fb1bb.pdf",
        "OPEC_MOMR": "https://www.opec.org/opec_web/static_files_project/media/downloads/publications/OPEC_MOMR_March_2026.pdf",
        # add more stable institutional PDF endpoints here
    },
    "rss": {
        "Nairametrics": "https://nairametrics.com/feed/",
        "Vanguard_Business": "https://www.vanguardngr.com/category/business/feed/",
        "Punch_Business": "https://rss.punchng.com/",
        "AllAfrica_Nigeria": "https://allafrica.com/tools/headlines/rdf/nigeria/headlines.rdf",
    },
    "apis": {
        # GDELT (example event query - you may parameterize)
        "GDELT_EVENTS": "https://api.gdeltproject.org/api/v2/events/search",
        # UN Comtrade / World Bank etc. can be added here with API keys if needed
    },
    "social": {
        # snscrape usage is local (no API key). Example: twitter-search
        # Represent hashtags as a list to avoid duplicate keys.
        "twitter_hashtags": [
            "#Dangote",
            "#DangoteRefinery",
            "#DangoteRefineryIPO",
            "#DangotePetroleumRefinery",
            "#LekkiRefinery",
        ],
    },
}
# keywords that cause "quarantine" / human review (customize for your ontology)
RISK_KEYWORDS = [
    "vandal", "pipeline", "closure", "halted", "sabotage", "attack", "explosion", "supply disruption",
    "strike", "fraud", "suspended", "bankruptcy", "default", "expropriation"
]

# ---------- utility helpers ----------
def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_json(path: str, obj: Dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def safe_filename(s: str) -> str:
    keep = "".join(c if (c.isalnum() or c in " ._-") else "_" for c in s)
    return "_".join(keep.split())[:180]

# ---------- network with retry ----------
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(4), reraise=True,
       retry=retry_if_exception_type(Exception))
async def fetch_bytes(session: aiohttp.ClientSession, url: str, timeout=30) -> bytes:
    headers = {"User-Agent": "OmniIngestor/1.0 (+https://your-org.example)"}
    async with session.get(url, headers=headers, timeout=timeout) as resp:
        resp.raise_for_status()
        return await resp.read()

# ---------- PDF extraction ----------
def extract_text_from_pdf_bytes(b: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(b)) as pdf:
            texts = []
            for p in pdf.pages:
                txt = p.extract_text()
                if txt:
                    texts.append(txt)
        return "\n\n".join(texts).strip()
    except Exception as e:
        # fallback: return empty and let caller log metadata
        return ""

# ---------- HTML cleaning ----------
def clean_html_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # remove scripts/styles
    for s in soup(["script", "style", "noscript"]):
        s.decompose()
    text = soup.get_text(separator="\n")
    # normalize spaces
    text = "\n".join(line.strip() for line in text.splitlines() if line.strip())
    return text

# ---------- dedup check ----------
def already_seen_text_hash(h: str) -> bool:
    # store a small index file
    idx_file = os.path.join(META_DIR, "text_index.json")
    try:
        if os.path.exists(idx_file):
            with open(idx_file, "r", encoding="utf-8") as f:
                idx = json.load(f)
        else:
            idx = {}
    except Exception:
        idx = {}
    if h in idx:
        return True
    idx[h] = now_ts()
    write_json(idx_file, idx)
    return False

# ---------- ingestion flows ----------
async def ingest_pdfs(session: aiohttp.ClientSession, pdf_manifest: Dict[str, str]):
    for key, url in pdf_manifest.items():
        print(f"[PDF] downloading {key} from {url}")
        try:
            data = await fetch_bytes(session, url)
            raw_path = os.path.join(RAW_DIR, f"{safe_filename(key)}.pdf")
            with open(raw_path, "wb") as f:
                f.write(data)
            text = extract_text_from_pdf_bytes(data)
            # dedup
            h = sha256_text(text if text else url)
            if already_seen_text_hash(h):
                print(f"  - duplicate (skipping): {key}")
                continue
            # write text and metadata
            ts = datetime.utcnow().isoformat()
            txt_path = os.path.join(TEXT_DIR, f"{safe_filename(key)}.txt")
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"SOURCE: {url}\nSCRAPED_AT: {ts}\n\n")
                f.write(text)
            meta = {
                "id": key,
                "type": "pdf",
                "source_url": url,
                "scraped_at": ts,
                "sha256": h,
                "raw_path": raw_path,
                "text_path": txt_path
            }
            write_json(os.path.join(META_DIR, f"{safe_filename(key)}.json"), meta)
            print(f"  - saved: {txt_path}")
        except Exception as e:
            print(f"  ! failed to download/extract {key}: {e}")

async def ingest_rss_feeds(session: aiohttp.ClientSession, rss_manifest: Dict[str, str], max_entries_per_feed=10):
    for name, feed_url in rss_manifest.items():
        print(f"[RSS] parsing {name} -> {feed_url}")
        try:
            # feedparser is synchronous; we do a simple requests call for freshness, then parse
            async with session.get(feed_url, headers={"User-Agent":"OmniIngestor/1.0"}, timeout=20) as r:
                r.raise_for_status()
                raw = await r.text()
            feed = feedparser.parse(raw)
            entries = feed.entries[:max_entries_per_feed]
            for e in entries:
                title = e.get("title", "untitled")
                published = e.get("published", e.get("updated", now_ts()))
                summary = e.get("summary", "") or e.get("content", [{"value": ""}])[0]["value"]
                # optionally fetch full article URL for richer text
                link = e.get("link", "")
                if link:
                    try:
                        async with session.get(link, timeout=20) as resp:
                            if resp.status == 200:
                                html = await resp.text()
                                body = clean_html_text(html)
                            else:
                                body = clean_html_text(summary)
                    except Exception:
                        body = clean_html_text(summary)
                else:
                    body = clean_html_text(summary)
                combined = f"TITLE: {title}\nPUBLISHED: {published}\nURL: {link}\n\n{body}"
                h = sha256_text(combined)
                if already_seen_text_hash(h):
                    continue
                filename = safe_filename(f"RSS_{name}_{title[:60]}_{published}")
                txt_path = os.path.join(TEXT_DIR, filename + ".txt")
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(combined)
                meta = {
                    "id": filename,
                    "type": "rss",
                    "feed": name,
                    "source_url": link or feed_url,
                    "published": published,
                    "scraped_at": now_ts(),
                    "sha256": h,
                    "text_path": txt_path
                }
                write_json(os.path.join(META_DIR, filename + ".json"), meta)

                # quarantine if high-risk keywords present
                low = combined.lower()
                if any(k in low for k in RISK_KEYWORDS):
                    qpath = os.path.join(QUARANTINE_DIR, filename + ".txt")
                    with open(qpath, "w", encoding="utf-8") as qf:
                        qf.write(combined)
                    print(f"  - quarantined (needs review): {filename}")
                else:
                    print(f"  - ingested: {filename}")
        except Exception as e:
            print(f"  ! failed feed {name}: {e}")

async def ingest_gdelt_events(session: aiohttp.ClientSession, query: str = "Nigeria", days: int = 1):
    # Uses GDELT v2 Events API; returns JSON/CSV depending on parameters.
    url = MANIFEST["apis"]["GDELT_EVENTS"]
    params = {
        "query": query,
        "mode": "TimelineVolatility",  # example - choose what you need
        "format": "JSON"
    }
    try:
        async with session.get(url, params=params, timeout=30) as r:
            r.raise_for_status()
            payload = await r.text()
            # store raw
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            fname = safe_filename(f"GDELT_{query}_{ts}")
            raw_path = os.path.join(RAW_DIR, fname + ".json")
            with open(raw_path, "w", encoding="utf-8") as f:
                f.write(payload)
            # lightweight indexing: create a text summary for RAG
            text_summary = f"GDELT query: {query}\nretrieved_at: {now_ts()}\n\n{payload[:20000]}"
            h = sha256_text(text_summary)
            if not already_seen_text_hash(h):
                txt_path = os.path.join(TEXT_DIR, fname + ".txt")
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(text_summary)
                meta = {
                    "id": fname,
                    "type": "gdelt",
                    "source_url": url,
                    "scraped_at": now_ts(),
                    "sha256": h,
                    "raw_path": raw_path,
                    "text_path": txt_path
                }
                write_json(os.path.join(META_DIR, fname + ".json"), meta)
                print(f"[GDELT] saved {fname}")
            else:
                print(f"[GDELT] duplicate result (skipping)")
    except Exception as e:
        print(f"[GDELT] failed query: {e}")

# ---------- orchestrator ----------
async def main(run_once: bool = False):
    async with aiohttp.ClientSession() as session:
        await ingest_pdfs(session, MANIFEST["pdfs"])
        await ingest_rss_feeds(session, MANIFEST["rss"])
        await ingest_gdelt_events(session, query="Nigeria")
        # optional: run social scrapers (snscrape) as subprocess / local command
        # optional: call UN Comtrade / World Bank APIs (requests with API keys / quotas)
    print("Ingestion cycle complete. Documents written to:", TEXT_DIR)
    print("Meta written to:", META_DIR)
    print("Quarantined items (if any) are in:", QUARANTINE_DIR)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-once", action="store_true", help="Run a single ingestion cycle")
    args = parser.parse_args()
    asyncio.run(main(run_once=args.run_once))