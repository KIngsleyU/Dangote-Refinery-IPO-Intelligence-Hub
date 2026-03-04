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

The repository now includes both **data engineering** (ingestion, ETL, SQLite, crack spread model) and an **agentic layer**: a LangGraph-backed quantitative agent and a FastAPI REST API for querying it.

---

### 2. Repository structure

- **`Dangote Refinery IPO Intelligence Hub.pdf`**  
  Strategic and technical blueprint: macro context, data providers, AWS architecture, agentic frameworks, 5‑month roadmap.

- **Data engineering**
  - **`data_engineering/ingestion/market_data_client.py`**  
    Asynchronous client: **yfinance** (BZ=F, RB=F, HO=F, VLO, MPC, PSX, USDNGN=X, SEPL.L, AFK), optional NGX/Argus; writes JSON to `data_engineering/raw_data/` or S3; orchestration via `run_pipeline()`.
  - **`data_engineering/db_setup.py`**  
    Creates `data_engineering/dangote_hub.db` with `market_data` (OHLCV by ticker/timestamp) and `model_outputs` (metrics).
  - **`data_engineering/etl_loader.py`**  
    Loads `raw_data/*.json` into `market_data` with `INSERT OR IGNORE`.

- **Data science – models and agent**
  - **`data_science/models/crack_spread.py`**  
    3-2-1 crack spread from BZ=F, RB=F, HO=F in SQLite; writes latest margin to `model_outputs`.
  - **`data_science/graph/state.py`**  
    `HubAgentState` TypedDict (messages, current_valuation_context) for the LangGraph pipeline.
  - **`data_science/tools/db_tools.py`**  
    LangChain tools (e.g. `get_latest_model_output(metric_name)`) reading from `model_outputs`; used by the agent.
  - **`data_science/agents/quant_agent.py`**  
    LangGraph definition: OpenRouter-backed LLM + tool node; compiles to `intelligence_hub_graph` (invoke/stream).

- **API and entrypoints**
  - **`api/main.py`**  
    FastAPI app: `load_dotenv()`, then `POST /api/v1/query` (forward to agent) and `GET /health`. Requires `OPENROUTER_API_KEY` in `.env`.
  - **`test_graph.py`**  
    CLI test: one question, stream graph, print node outputs and tool/AI messages.
  - **`test_graph_v2.py`**  
    Same as above with higher transparency (state keys, message count, raw tool_calls).
  - **`main.py`**  
    Placeholder; run ingestion, tests, or API as above.

- **Config and dependencies**
  - **`requirements.txt`** — yfinance, aiohttp, boto3, pandas (API/agent need fastapi, uvicorn, langchain-openai, langgraph, langchain-core, python-dotenv; install separately or add to requirements).
  - **`.env`** — AWS, NGX, Argus, and **OPENROUTER_API_KEY** (required for agent/API).
  - **`docker-compose.yml`** — Placeholder for future containerized deployment.

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

Create a `.env` file in the project root. Typical variables:

```bash
# AWS (optional for local dev; use aws configure or IAM in production)
AWS_REGION=us-east-1
RAW_DATA_BUCKET=dangote-hub-raw-zone
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...

# Premium data providers (optional for ingestion)
NGX_API_KEY=your_ngx_marketdatav3_token
ARGUS_API_KEY=your_argus_api_key

# Agent & API (required for quant_agent and api/main.py)
OPENROUTER_API_KEY=sk-or-v1-...
# OPENROUTER_MODEL=z-ai/glm-4.5-air:free
```

`OPENROUTER_API_KEY` is read at import time by `data_science.agents.quant_agent`; `api/main.py` and the test scripts call `load_dotenv()` before importing so the key is available.

> **Security note:** Never commit `.env`. `.gitignore` already excludes it and common secret patterns.

---

### 5. Installation and setup

1. **Clone the repository**

```bash
git clone <your-repo-url>.git
cd Dangote-Refinery-IPO-Intelligence-Hub
```

2. **Create and activate a virtual environment (recommended)**

```bash
python -m venv .venv
source .venv/bin/activate  # on macOS / Linux
# .venv\Scripts\activate   # on Windows (PowerShell/CMD)
```

3. **Install Python dependencies**

```bash
pip install -r requirements.txt
```

For the **agent and API**, also install: `fastapi`, `uvicorn[standard]`, `langchain-openai`, `langgraph`, `langchain-core`, `python-dotenv` (add to `requirements.txt` or run `pip install fastapi uvicorn[standard] langchain-openai langgraph langchain-core python-dotenv`).

4. **Configure environment**

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

### 7. Working with the local analytical database

The project includes a lightweight SQLite database to make it easy to prototype analytics before migrating to managed cloud databases.

- **Initialize or update the schema**

```bash
python data_engineering/db_setup.py
```

This creates (or updates) `data_engineering/dangote_hub.db` with the `market_data` and `model_outputs` tables and appropriate indexes.

- **Load ingested JSON into SQLite**

After you have run the ingestion client and generated JSON snapshots in `data_engineering/raw_data/`, load them into the database:

```bash
python data_engineering/etl_loader.py
```

This will:

- Scan all `*.json` files under `data_engineering/raw_data/`.
- Derive the `asset_name` from each filename.
- Insert OHLCV rows into the `market_data` table, ignoring duplicates based on the `(ticker_symbol, timestamp)` uniqueness constraint.

You can then inspect or query `data_engineering/dangote_hub.db` using any SQLite client or Python notebook.

---

### 8. Running the crack spread model

Once you have ingested data and loaded it into SQLite, you can compute a live proxy for refining gross margin using the **3-2-1 crack spread** model.

**Prerequisites**

- `data_engineering/dangote_hub.db` exists (run `data_engineering/db_setup.py`).
- `pandas` is installed (included in `requirements.txt`).
- `market_data` contains synchronized timestamps for:
  - `BZ=F` (Brent crude, USD/barrel)
  - `RB=F` (RBOB gasoline, USD/gallon)
  - `HO=F` (ULSD / heating oil, USD/gallon)
- The ETL step has been run (`data_engineering/etl_loader.py`), which loads `ticker_symbol` from ingested JSON into SQLite.

**Run**

```bash
python data_science/models/crack_spread.py
```

On success, the latest crack spread value is written to the `model_outputs` table with:

- `model_name`: `3-2-1_Crack_Spread`
- `metric_name`: `USD_margin_per_barrel`

---

### 9. Running the agent and API

The quantitative agent is a LangGraph that answers financial questions (e.g. crack spread, margin per barrel) by optionally calling `db_tools` (e.g. `get_latest_model_output`) and then synthesizing a reply. It is used by both the REST API and the CLI test scripts.

**Prerequisites**

- `OPENROUTER_API_KEY` (and optionally `OPENROUTER_MODEL`) in `.env`.
- For answers that use DB metrics: run ingestion → ETL → crack spread model so `model_outputs` has data.

**CLI (streaming, high transparency)**

```bash
python test_graph_v2.py
```

Uses `load_dotenv()`, then streams the graph with `stream_mode="updates"` and prints node name, state keys, message count, tool calls, and final answer. For a simpler trace, use `test_graph.py`.

**REST API**

```bash
uvicorn api.main:app --reload
```

- API: `http://127.0.0.1:8000`
- Docs: `http://127.0.0.1:8000/docs`
- **POST /api/v1/query** — JSON body: `{"query": "What is the latest USD margin per barrel?"}` (optional `session_id`). Returns `{"answer": "...", "session_id": "..."}`.
- **GET /health** — Returns `{"status": "operational", "service": "Dangote Intelligence Hub"}`.

`api/main.py` calls `load_dotenv()` at the top so the OpenRouter key is available when `quant_agent` is imported.

---

### 10. Roadmap (aligned with the PDF blueprint)

The repo already implements **Phase 1** (data pipelines, SQLite, crack spread) and an initial **agentic layer** (LangGraph agent, db_tools, FastAPI). Next steps:

- **Data ingestion expansion**  
  Real‑time NGX (MarketDataV3, iTick), Argus/Platts, OPEC/EIA, AIS vessel tracking, Nigerian macro/sentiment sources.
- **Agentic “Brain” (extend)**  
  Multi‑agent orchestration (CrewAI, AutoGen), Pidgin‑aware NLP, dividend-sustainability workflows, checkpointer/session store for the API.
- **Cloud & interfaces**  
  AWS (Kinesis, Lambda, EventBridge, DynamoDB/Timestream), investor dashboards, WhatsApp/Telegram bots.

---

### 11. Contributing

Contributions aligned with the strategic blueprint are welcome. Ideas:

- Extending `MarketDataIngestor` (OPEC/EIA, vessel tracking, Nigeria indices).
- Unit and integration tests for ingestion, ETL, and agent.
- Multi‑agent orchestration, session/checkpointer for the API, or additional LangChain tools.

Please open an issue before submitting a pull request.

---

### 12. Disclaimer

This project is an **educational and research tool**, not investment advice.  
All market data, analytics, and outputs should be independently verified before using them in any trading, investment, or risk‑management decisions. The Dangote Group, NGX, Argus, and all other referenced entities are **not** affiliated with this repository.