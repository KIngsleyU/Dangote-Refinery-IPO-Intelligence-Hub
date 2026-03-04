# api/main.py

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
        final_state = intelligence_hub_graph.invoke(initial_state)
        
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