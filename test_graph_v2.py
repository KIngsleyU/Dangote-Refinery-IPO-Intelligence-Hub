# test_graph_v2.py

import os
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage

# Load environment variables before importing the agent graph so that
# OPENROUTER_API_KEY and related settings are available on import.
load_dotenv()

# Import the compiled graph we built in the previous step
from data_science.agents.quant_agent import intelligence_hub_graph

def run_local_test():
    print("🤖 Initializing Dangote IPO Intelligence Hub Agent...\n")
    
    # 1. The Input: A complex financial question
    question = "What is the current projected margin per barrel for the refinery based on our latest models?"
    print(f"👤 User: {question}\n")
    
    # 2. Initialize the State with the user's question
    initial_state = {"messages": [HumanMessage(content=question)]}
    
    print("🧠 Agent Working...\n")
    
    # 3. Stream the graph execution to watch the agent's step-by-step logic
    # Using stream_mode="updates" lets us see exactly what each node adds to the state
    for output in intelligence_hub_graph.stream(initial_state, stream_mode="updates"):
        
        # The output is a dictionary keyed by the name of the node that just executed
        for node_name, state_update in output.items():
            print(f"--- Routing through node: [{node_name.upper()}] ---")
            
            # Extract the latest message added to the state
            messages = state_update.get("messages", [])
            if messages:
                last_msg = messages[-1]
                
                # Condition A: The LLM decided to call a database tool
                if hasattr(last_msg, 'tool_calls') and last_msg.tool_calls:
                    tool_name = last_msg.tool_calls[0]['name']
                    print(f"🛠️  Agent Action: Pausing generation to execute tool -> {tool_name}()")
                
                # Condition B: The Tool node executed and returned the SQLite data
                elif last_msg.type == "tool":
                    print(f"📊 Database Result Extracted: {last_msg.content}")
                    
                # Condition C: The LLM synthesized the final answer
                elif last_msg.type == "ai" and not hasattr(last_msg, 'tool_calls'):
                    print(f"\n✅ Final Synthesized Answer:\n{last_msg.content}\n")

if __name__ == "__main__":
    run_local_test()