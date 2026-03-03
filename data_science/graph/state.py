# data_science/graph/state.py

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
    
    