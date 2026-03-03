# test_graph_v2.py
"""
High-transparency debug harness for the Dangote IPO Intelligence Hub graph.

This script provides a more verbose view than `test_graph.py`, showing:
- Which node ran.
- How the state changed (keys, message history length).
- Raw tool call payloads and tool outputs.
- The final synthesized answer from the agent.
"""

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage

# Load environment variables before importing the agent graph so that
# OPENROUTER_API_KEY and related settings are available on import.
load_dotenv()

from data_science.agents.quant_agent import intelligence_hub_graph


def run_local_debug():
    print("\n" + "=" * 70)
    print("🤖 Initializing Dangote IPO Intelligence Hub Agent (DEBUG MODE)")
    print("=" * 70 + "\n")

    # 1. The Input: A complex financial question
    question = (
        "What is the current projected margin per barrel for the refinery "
        "based on our latest models?"
    )
    question = "What is the latest calculated USD margin per barrel (3-2-1 crack spread) for the refinery?"
    print(f"👤 User: {question}\n")

    # 2. Initialize the state with the user's question
    initial_state = {
        "messages": [HumanMessage(content=question)],
        "current_valuation_context": "The Dangote Refinery is preparing for a June/July 2026 IPO.",
    }

    print("🧠 Streaming graph execution with full state updates...\n")

    # 3. Stream the graph execution to watch node-by-node behavior
    for event in intelligence_hub_graph.stream(initial_state, stream_mode="updates"):
        # Each event is a dict keyed by node name
        for node_name, state_update in event.items():
            print(f"\n--- NODE COMPLETED: [{node_name.upper()}] ---")
            print("State keys:", list(state_update.keys()))

            messages = state_update.get("messages", [])
            print(f"Total messages in state: {len(messages)}")

            if messages:
                # Show the last 1–2 messages for context
                print("Recent messages:")
                for m in messages[-2:]:
                    role = getattr(m, "type", "unknown")
                    print(f"  - {role.upper()}: {getattr(m, 'content', '')}")

                last_msg = messages[-1]

                # A. LLM decided to call a tool
                if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
                    print("🛠️  Agent decided to use a tool.")
                    print("   Raw tool_calls:", last_msg.tool_calls)
                    for call in last_msg.tool_calls:
                        print(
                            f"   -> Tool: {call.get('name')} | Args: {call.get('args')}"
                        )

                # B. Tool node executed and returned data
                elif getattr(last_msg, "type", "") == "tool":
                    print("📊 Tool node output:")
                    print(f"   name: {getattr(last_msg, 'name', '')}")
                    print(f"   content: {last_msg.content}")

                # C. Standard AI response (no further tools)
                elif getattr(last_msg, "type", "") == "ai" and not getattr(
                    last_msg, "tool_calls", None
                ):
                    print("\n✅ FINAL SYNTHESIZED ANSWER:")
                    print(last_msg.content)

    print("\n" + "=" * 70)
    print("DEBUG RUN COMPLETE")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    run_local_debug()