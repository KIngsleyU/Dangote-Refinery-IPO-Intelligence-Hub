# data_science/tools/rag_tools.py

from langchain_core.tools import tool
from data_science.models.rag_pipeline import OmniIntentRAG

# Initialize the RAG engine once when the tool is imported
try:
    rag_engine = OmniIntentRAG()
except Exception as e:
    print(f"Warning: RAG engine failed to initialize: {e}")
    rag_engine = None

@tool
def ask_dangote_ipo_documents_rag(query: str) -> str:
    """Answer questions using the Dangote IPO Intelligence Hub document RAG.

    Use this when you need *qualitative / unstructured* information from ingested docs
    (e.g., IPO prospectus excerpts, regulatory filings, governance statements, supply
    chain descriptions, macro/geopolitics writeups, or narrative context).

    Good for:
    - board / governance / ownership / counterparties
    - regulatory risks, compliance language, covenants, prospectus wording
    - narrative context that isn't in SQLite metrics/tickers

    Not ideal for:
    - numeric time series, prices, or computed metrics (use DB/market tools)

    Returns: a plain-text answer synthesized from retrieved document chunks.
    """
    if not rag_engine:
        return "The document retrieval system is currently offline."
    
    return rag_engine.query(query)


# Backwards-compatible alias (not a tool, so agents won't see it).
def query_regulatory_documents(query: str) -> str:
    return ask_dangote_ipo_documents_rag(query)