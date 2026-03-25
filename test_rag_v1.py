# test_rag_v1.py

from langchain_core.tools import tool
from data_science.models.rag_pipeline import OmniIntentRAG

# Initialize the RAG engine once when the tool is imported
rag_engine = OmniIntentRAG()

print(rag_engine.query("what is the of the Dangote refinery?"))
print("--------------------------------")
print(rag_engine.query("What is the latest calculated USD margin per barrel (3-2-1 crack spread) for the Dangote refinery?"))
print("--------------------------------")

print(rag_engine.query("How do Nigerians feel about the Dangote refinery lowering inflation?"))
print("--------------------------------")
print(rag_engine.query("is the us-isreal conflict with iran affecting the Dangote refinery?"))
print("--------------------------------")
print(rag_engine.query("What are the refining capacity, production volumes, and key financial metrics of the Dangote Refinery? Include any information about revenue, costs, and profitability."))
print("--------------------------------")