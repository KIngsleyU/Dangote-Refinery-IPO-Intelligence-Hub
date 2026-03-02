## Dangote Refinery IPO Intelligence Hub

**An agentic AI and data engineering platform for the 2026 Dangote Refinery IPO – integrating real-time market data, refinery economics, and multi‑agent analysis into a single institutional‑grade intelligence hub.**

---

### 1. Project overview

The **Dangote Refinery IPO Intelligence Hub** is a Python-based research and analytics stack inspired by the strategic blueprint in `Dangote Refinery IPO Intelligence Hub.pdf`. It is being built to **capitalize on the 2026 Dangote Refinery IPO** by reducing the massive information asymmetry around the refinery’s operations, margins, and macro impact. In a five‑month window before listing, the goal is to continuously capture and synthesize live data on crude input costs, refined product prices, FX dynamics, and comparable refiners so that investors and analysts can make **institutional‑grade decisions** instead of relying on headlines and marketing narratives.

Concretely, the hub is designed to:

- **Ingest and persist market data** for crude, refined products, FX, and refinery comparables.
- **Model refinery economics**, especially crack spreads and profitability under different macro regimes.
- **Prepare the foundation for multi‑agent AI workflows** (LangGraph, CrewAI, AutoGen, LlamaIndex, etc.) that will synthesize data into actionable IPO intelligence for:
  - Retail Nigerian investors.
  - Regional and global institutional investors.
  - Policy and macro research teams.

The current repository represents the **data engineering starting point** of the full architecture described in the PDF: a focused, testable ingestion client and local raw data lake that will later be extended into a full cloud‑native, agentic system.

---

### 2. Repository structure

- `Dangote Refinery IPO Intelligence Hub.pdf`  
Comprehensive strategic and technical blueprint for the full system: macro context, data providers, AWS reference architecture, agentic frameworks, and 5‑month execution roadmap.
- `data_engineering/ingestion/market_data_client.py`  
Asynchronous Python client that:
  - Uses **`yfinance`** to pull market data for:
    - **Input cost**: Brent crude futures (`BZ=F`).
    - **Output proxies**: RBOB gasoline (`RB=F`), ULSD/heating oil (`HO=F`).
    - **Competitor refiners**: Valero (`VLO`), Marathon Petroleum (`MPC`), Phillips 66 (`PSX`).
    - **Nigeria‑linked macro proxies**: USD/NGN FX (`USDNGN=X`), Seplat (`SEPL.L`), Africa ETF (`AFK`).
  - Optionally calls:
    - **NGX Market Data API** for Nigerian equity data (`fetch_ngx_equity_data`).
    - **Argus Media API** for refined product pricing (`fetch_argus_crude_prices`).
  - Persists raw JSON snapshots:
    - Locally under `data_engineering/raw_data/`.
    - To AWS S3 (when credentials and bucket are configured).
  - Orchestrates all tasks concurrently via `asyncio` and `aiohttp` in `run_pipeline()`.
- `main.py`  
Placeholder for the future application entrypoint (web/API/agentic orchestration). Currently empty; the primary executable for now is the ingestion client itself.
- `requirements.txt`  
Python dependencies for the ingestion layer and future expansion.
- `.env`  
Local environment configuration (ignored by git). Used for AWS and premium data provider credentials.
- `docker-compose.yml`  
Placeholder for container‑based local deployment of the hub (to be expanded as services are added).

---

### 3. Data ingestion pipeline (current implementation)

The heart of the current codebase is the `MarketDataIngestor` class in `data_engineering/ingestion/market_data_client.py`.

- **Asynchronous orchestration**
  - `run_pipeline()` creates an `aiohttp.ClientSession` and launches multiple asynchronous tasks in parallel via `asyncio.gather`.
  - Each task calls `fetch_yahoo_finance_data(...)` for a specific ticker or, when enabled, calls NGX/Argus ingestion methods.
- **yfinance ingestion**
  - For each configured ticker:
    - Downloads **1‑day, 1‑minute interval** history via `ticker.history(period="1d", interval="1m")`.
    - Resets the index and converts the pandas DataFrame to a list of records.
    - Normalizes `Datetime` values to strings for JSON serialization.
    - Saves data as a timestamped JSON file under `data_engineering/raw_data/`.
  - If no data is returned for a ticker, the client logs a **non‑fatal warning** and continues, keeping the pipeline robust to symbol issues or delistings.
- **NGX integration (optional, paid)**
  - `fetch_ngx_equity_data(...)` hits:
    - `https://api.ngxgroup.com/marketdata/v3/equities/live`
  - Uses an `Authorization: Bearer {NGX_API_KEY}` header.
  - On success, uploads the JSON payload into S3 under the `ngx_equities/` prefix.
  - Intended for real‑time Nigerian equity and sector‑specific data once a MarketDataV3 subscription is secured.
- **Argus integration (optional, paid)**
  - `fetch_argus_crude_prices(...)` calls:
    - `https://api.argusmedia.com/v1/prices/latest`
  - Uses `x-api-key: {ARGUS_API_KEY}` and a JSON payload of Argus symbols (e.g. `BRENT_CRUDE`, `ULSD_CIF_WAF`).
  - On success, uploads the JSON payload into S3 under the `argus_commodities/` prefix.
  - This is the institutional‑grade path for crack‑spread inputs described in the PDF (complementing or eventually replacing free `yfinance` data).
- **S3 integration**
  - `_upload_to_s3(...)` writes any JSON‑serializable payload to an S3 bucket using a timestamped key:
    - `s3://{RAW_DATA_BUCKET}/{source}/{file_prefix}_{YYYYMMDD_HHMMSS}.json`

---

### 4. Environment configuration

Create a `.env` file in the project root (if you have not already). Typical variables:

```bash
# AWS
AWS_REGION=us-east-1
RAW_DATA_BUCKET=dangote-hub-raw-zone

# Optional: explicit credentials for local dev
# (for production, prefer IAM roles or AWS CLI profiles)
# AWS_ACCESS_KEY_ID=your_access_key_id
# AWS_SECRET_ACCESS_KEY=your_secret_access_key

# Premium data provider keys (optional for now)
NGX_API_KEY=your_ngx_marketdatav3_token
ARGUS_API_KEY=your_argus_api_key
```

> **Security note:** Never commit `.env` or any credential files. This repo’s `.gitignore` already excludes common secret patterns.

---

### 5. Installation and setup

1. **Clone the repository**

```bash
git clone <your-repo-url>.git
cd Dangote-Refinery-IPO-Intelligence-Hub
```

1. **Create and activate a virtual environment (recommended)**

```bash
python -m venv .venv
source .venv/bin/activate  # on macOS / Linux
# .venv\Scripts\activate   # on Windows (PowerShell/CMD)
```

1. **Install Python dependencies**

```bash
pip install -r requirements.txt
```

1. **Configure environment**

- Create and populate `.env` as described above.
- If you plan to write to S3 from your local machine, configure AWS credentials via:

```bash
aws configure
```

or by setting the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.

---

### 6. Running the ingestion client

From the project root:

```bash
python data_engineering/ingestion/market_data_client.py
```

This will:

- Launch the `MarketDataIngestor`.
- Fetch market data for all active yfinance tickers.
- Write timestamped JSON snapshots into `data_engineering/raw_data/`.
- Optionally push NGX/Argus payloads into your configured S3 bucket when API keys and AWS permissions are in place (uncomment and enable those tasks in `run_pipeline()` when ready).

You can re-run this script on a schedule (cron, systemd timer, CI job, or later as an AWS Lambda/ECS task) to build a historical dataset for downstream modeling.

---

### 7. Roadmap (aligned with the PDF blueprint)

The current codebase corresponds broadly to **Phase 1: Architecture & Integration** from the PDF. Key future milestones include:

- **Data ingestion expansion**
  - Integrate real‑time NGX feeds (MarketDataV3, possibly iTick).
  - Connect Argus / Platts, OPEC, EIA, AIS vessel tracking, and localized Nigerian macro sources.
- **Agentic “Brain”**
  - Implement multi‑agent orchestration using LangGraph, CrewAI, and AutoGen.
  - Add localized sentiment analysis and Pidgin‑aware NLP for Nigerian news and social data.
  - Automate crack spread and dividend sustainability workflows as described in the blueprint.
- **Cloud & interfaces**
  - Migrate ingestion and reasoning workflows to AWS (Kinesis, Lambda, EventBridge, DynamoDB/Timestream).
  - Build investor‑facing web dashboards and APIs.
  - Deploy WhatsApp / Telegram conversational agents for omnichannel access.

---

### 8. Contributing

Contributions that keep the implementation aligned with the strategic blueprint are welcome. Helpful directions include:

- Extending `MarketDataIngestor` with additional asset classes (e.g. OPEC/EIA, vessel tracking, Nigeria‑specific indices).
- Adding unit tests and integration tests for ingestion and serialization.
- Implementing the initial multi‑agent orchestration skeleton in `main.py`.

Please open an issue describing your proposed change before submitting a pull request

---

### 9. Disclaimer

This project is an **educational and research tool**, not investment advice.  
All market data, analytics, and outputs should be independently verified before using them in any trading, investment, or risk‑management decisions. The Dangote Group, NGX, Argus, and all other referenced entities are **not** affiliated with this repository.