# api/main.py

"""
REST API entrypoint for the Dangote Refinery IPO Intelligence Hub.

This module exposes a minimal FastAPI application that forwards natural-language
queries to the LangGraph-backed quantitative agent (`data_science.agents.quant_agent`)
and returns the synthesized answer. It is the programmatic interface for
integrating the hub into dashboards, mobile apps, or third-party systems.

## Environment and import order

`load_dotenv()` is called at the top **before** importing `quant_agent`. The
agent reads `OPENROUTER_API_KEY` at import time; if `.env` is not loaded first,
the import will raise and the server will fail to start. Ensure `.env` exists
in the project root with `OPENROUTER_API_KEY` (and optionally `OPENROUTER_MODEL`).

## Endpoints

- **POST /api/v1/query**: Accepts a JSON body with `query` (required) and
  optional `session_id`. Runs the query through the compiled graph via
  `intelligence_hub_graph.invoke(initial_state)` and returns `answer` and
  `session_id` in the response. Each request is stateless; `session_id` is
  intended for future use with a checkpointer or conversation store.

- **GET /health**: Returns a simple operational status payload for load
  balancers and monitoring.

## Request and response

- **QueryRequest**: `query` (str), `session_id` (str, default: new UUID).
- **QueryResponse**: `answer` (str), `session_id` (str).

Errors (e.g. LLM or database failure) are surfaced as HTTP 500 with a detail
message; the server does not crash.

## Running the server

From the project root:

    uvicorn api.main:app --reload

The app is available at `http://127.0.0.1:8000`. OpenAPI docs at `/docs`.
"""

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from langchain_core.messages import HumanMessage
import uuid

# Import our compiled LangGraph agent
from data_science.agents.quant_agent import intelligence_hub_graph

# 1. Initialize the FastAPI Application
app = FastAPI(
    title="Dangote IPO Intelligence Hub API",
    description="REST API for querying the multi-agent valuation and sentiment engine.",
    version="1.0.0"
)

# 2. Define Pydantic Data Models for Request/Response Validation
class QueryRequest(BaseModel):
    query: str = Field(..., description="The user's question about the refinery or IPO.")
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique ID to maintain conversation state.")

class QueryResponse(BaseModel):
    answer: str
    session_id: str

# 3. Create the Primary Chat Endpoint
@app.post("/api/v1/query", response_model=QueryResponse)
async def query_intelligence_hub(request: QueryRequest):
    """
    Takes a user query, passes it through the LangGraph agent, 
    and returns the grounded financial response.
    """
    try:
        # Define the initial state for the LangGraph execution
        initial_state = {
            "messages": [HumanMessage(content=request.query)],
            "current_valuation_context": "The Dangote Refinery is preparing for a June/July 2026 IPO on the NGX."
        }
        
        # Invoke the graph. In a fully productionized setup with persistent memory,
        # you would pass the session_id into a checkpointer here.
        final_state = intelligence_hub_graph.invoke(
            initial_state,
            config={"recursion_limit": 50}
        )
        
        # Extract the final AI message from the state
        final_message = final_state["messages"][-1].content
        
        return QueryResponse(
            answer=final_message,
            session_id=request.session_id
        )
        
    except Exception as e:
        # Prevent the server from crashing if the LLM or Database fails
        raise HTTPException(status_code=500, detail=f"Agent Execution Failed: {str(e)}")

# 4. Create a Health Check Endpoint
@app.get("/health")
async def health_check():
    return {"status": "operational", "service": "Dangote Intelligence Hub"}