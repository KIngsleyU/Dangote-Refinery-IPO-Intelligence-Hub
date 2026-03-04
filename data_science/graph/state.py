# data_science/graph/state.py

"""
Graph state definition for the Dangote Refinery IPO Intelligence Hub agent.

This module defines the **state schema** passed through the LangGraph pipeline
in `data_science.agents.quant_agent`. The state is a single TypedDict that
carries conversation history and optional context from node to node; LangGraph
uses it to enforce a consistent interface and to support reducers for
accumulating messages.

## Why state lives here

Centralizing state in `data_science.graph.state` keeps the graph definition
(`quant_agent.py`) and any future multi-agent or human-in-the-loop extensions
aligned on the same shape. Adding new top-level keys (e.g. session_id,
intermediate_calculations) can be done in one place.

## Key concepts

- **messages**: The list of chat turns (user, assistant, tool results). The
  `Annotated[..., add_messages]` reducer ensures that when a node returns
  `{"messages": [new_message]}`, the new message is **appended** to the
  existing list instead of replacing it. Without this, each node would
  overwrite the full history.

- **current_valuation_context**: Optional string (e.g. "Dangote Refinery
  preparing for June/July 2026 IPO on the NGX") that can be set once and
  reused across turns. The agent can reference it when synthesizing answers;
  it is not automatically injected into the LLM—callers set it in the initial
  state when invoking the graph.

## Usage

When invoking the compiled graph, pass an initial state that includes at least
`messages` (e.g. `[HumanMessage(content=user_query)]`). The graph will
read and update this state at each step; the final state contains the full
message history plus the last AI response.
"""

from typing import Annotated, TypedDict
from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

class HubAgentState(TypedDict):
    """
    The shared memory state for the Dangote IPO Intelligence Hub agent.
    
    Attributes:
        messages: A list of messages (HumanMessage, AIMessage, ToolMessage). 
                  The `add_messages` reducer ensures new messages are appended 
                  rather than overwriting the entire conversation history.
        current_valuation_context: A string to store high-level macro context 
                                   (like the current USD/NGN rate) that should 
                                   persist across multiple conversational turns.
    """
    messages: Annotated[list[AnyMessage], add_messages]
    current_valuation_context: str
    
    