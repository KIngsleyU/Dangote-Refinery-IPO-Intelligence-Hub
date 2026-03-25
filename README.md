# Dangote Refinery IPO Intelligence Hub

**An agentic intelligence + data engineering stack built to reduce information asymmetry ahead of the 2026 Dangote Refinery IPO.**  
It continuously ingests Nigerian macro/energy/geopolitics signals, tracks market proxies, and exposes a LangGraph agent + dual-index RAG (Vector + Graph) to answer both institutional and retail-grade questions with evidence.

---

## Mission (from `docs/`)

The strategic goal (per the PDFs in `docs/`) is to build an **institutional-grade, near-real-time intelligence hub** around a macro catalyst: Dangote Refinery reaching full capacity and a planned **June/July 2026 IPO (10% float)**. The hub is designed to:

- **Hydrate a living knowledge base**: regulations, filings, macro policy, conflict/geopolitics, supply chain/logistics, and market proxies.
- **Quantify refinery economics**: crack spreads and margin proxies from market benchmarks.
- **Serve multiple user intents**: investor Q&A, macro research, governance/supply-chain tracing, and public-education explainers.

---

## What’s in this repo (today)

### Data ingestion → `data_engineering/ingestion/`

- **`run_all_ingestion.py`**: orchestrates multiple ingestion scripts and snapshots their outputs into `data_engineering/documents/<timestamp>__<label>/` so the RAG layer can ingest versioned “bundles”.
- **`macro_energy_extractor.py`**: institutional macro/energy extraction (CBN, etc.) into markdown-like outputs under `data_engineering/ingestion/data/`.
- **`geopolitics_extractor.py`**: pulls ACLED/HDX trend datasets and converts them to compact markdown for RAG.
- **`acled_events_extractor.py`**: pulls event-level ACLED Nigeria data (depending on API tier) into JSONL.
- **`dynamic_market_scraper.py`**: parallel/black-market FX proxy using **Bybit P2P USDT/NGN** (Binance NGN was discontinued in March 2024).
- **`ais_telemetry_stream.py`**: streams AIS telemetry to build maritime signals (long-running stream; not orchestrated by default).
- **`market_data_client.py`**: market proxy ingestion (yfinance and optional paid feeds) for crude/products/equities/FX.
- **`ingest_pipeline.py`**: a manifest-driven “media + institutional PDFs” collector that writes to a documents directory suitable for RAG ingestion.

### Storage / analytics → `data_engineering/`

- **`db_setup.py`**: creates/updates `data_engineering/dangote_hub.db` with `market_data` and `model_outputs`.
- **`etl_loader.py`**: loads ingested snapshots into SQLite (`market_data`).

### Modeling + RAG + agent → `data_science/`

- **`models/crack_spread.py`**: computes a 3-2-1 crack spread proxy from market benchmarks stored in SQLite, writing `USD_margin_per_barrel` to `model_outputs`.
- **`models/rag_pipeline.py`**: dual-index RAG:
  - **VectorStoreIndex**: narratives/sentiment/explanations.
  - **PropertyGraphIndex**: governance, entities, relationships, compliance/supply-chain tracing.
  - Uses `data_engineering/documents/` as the source corpus (including `run_all_ingestion.py` snapshots).
- **`tools/db_tools.py`**: LangChain tools to query SQLite (`list_available_metrics`, `get_latest_model_output`, etc.).
- **`tools/rag_tools.py`**: LangChain tool exposing document RAG:
  - `ask_dangote_ipo_documents_rag(query: str) -> str`
- **`agents/quant_agent.py`**: LangGraph “quant + research” agent using OpenRouter LLM + tool calls (DB tools + RAG tool).

### API / entrypoints

- **`api/main.py`**: FastAPI wrapper around the LangGraph agent (`POST /api/v1/query`, `GET /health`).
- **`test_graph.py`, `test_graph_v2.py`**: CLI harnesses to run/stream the agent with transparent traces.
- **`test_rag_v1.py`**: quick RAG smoke test.
- **`frontend/app.py`**: (early) UI entrypoint.

---

## Quickstart

### 1) Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure `.env`

Create `.env` at the repo root.

```bash
# Required for the agent (OpenRouter)
OPENROUTER_API_KEY=sk-or-v1-...
# Optional
# OPENROUTER_MODEL=z-ai/glm-4.5-air:free
# OPENROUTER_EMBED_MODEL=google/gemini-embedding-001

# Optional: DeepSeek (only if you configure rag_pipeline.py to use DeepSeek)
DEEPSEEK_API_KEY=...

# Optional: HDX / ACLED trends
HDX_API_KEY=...

# Optional: AIS Stream
AIS_STREAM_API_KEY=...

# Optional: AWS/S3 for raw zone uploads (if enabled in ingestion clients)
AWS_REGION=us-east-1
RAW_DATA_BUCKET=...
```

> `api/main.py` calls `load_dotenv()` before importing the agent so environment variables are visible at import time.

---

## Running the system

### A) Run ingestion and snapshot outputs (recommended)

This creates versioned bundles under `data_engineering/documents/` that the RAG pipeline reads.

```bash
python data_engineering/ingestion/run_all_ingestion.py
```

### B) Build / rebuild the dual-index RAG

```bash
python data_science/models/rag_pipeline.py
```

### C) Initialize SQLite + load market snapshots (optional quant path)

```bash
python data_engineering/db_setup.py
python data_engineering/etl_loader.py
python data_science/models/crack_spread.py
```

### D) Run the agent (CLI)

```bash
python test_graph_v2.py
```

### E) Run the API

```bash
uvicorn api.main:app --reload
```

- **API**: `http://127.0.0.1:8000`
- **Docs**: `http://127.0.0.1:8000/docs`

---

## Troubleshooting

- **RAG tool timeouts (`APITimeoutError`)**:
  - The RAG engine (`OmniIntentRAG`) uses whatever `Settings.llm` is set to in `data_science/models/rag_pipeline.py`.
  - If you point RAG at a slower provider (e.g., DeepSeek reasoning models) and run **graph extraction** with concurrency (`SchemaLLMPathExtractor(num_workers=5)`), you may hit timeouts/rate limits.
  - Mitigations:
    - Reduce `num_workers` (e.g., 1–2) during graph builds.
    - Increase the LLM timeout / retries.
    - Or configure RAG to use OpenRouter for better responsiveness.

- **Import error for `OpenAILike`**:
  - `OpenAILike` is an optional integration package. Install with:

```bash
pip install llama-index-llms-openai-like
```

---

## Docs / strategy references

All mission/architecture references live in `docs/`:

- `docs/Dangote Refinery IPO Intelligence Hub.pdf`
- `docs/Refining Intelligence_ Vector vs. Graph.pdf`
- `docs/Enhancing Data Ingestion for RAG.pdf`
- `docs/Dangote Refinery Data Acquisition Strategy.pdf`

---

## Disclaimer

This project is an **educational and research tool**, not investment advice.  
All outputs should be independently verified before use in trading, investing, or risk decisions.
