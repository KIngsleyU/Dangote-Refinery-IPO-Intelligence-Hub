# data_science/models/rag_pipeline.py

import os
import shutil
from typing import Optional

import logging
import sys

# Option A: Standard LlamaIndex Simple Callback (Prints LLM inputs/outputs)
# import llama_index.core
# llama_index.core.set_global_handler("simple")

# Option B: Hardcore Debugging (Prints EVERYTHING happening in the network)
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

import tiktoken
from llama_index.core import (
    VectorStoreIndex, 
    PropertyGraphIndex,
    SimpleDirectoryReader, 
    StorageContext, 
    load_index_from_storage
)
from llama_index.core.tools import QueryEngineTool
from llama_index.core.query_engine import RouterQueryEngine
from llama_index.core.selectors import LLMSingleSelector
from llama_index.llms.openai import OpenAI
from llama_index.llms.openai.utils import (
    openai_modelname_to_contextsize,
    is_chat_model,
    is_function_calling_model,
    O1_MODELS,
)
from llama_index.core.base.llms.types import LLMMetadata, MessageRole
from llama_index.core import Settings
from langchain_openai import ChatOpenAI
from llama_index.llms.openai_like import OpenAILike

from typing import Literal
from llama_index.core.indices.property_graph import SchemaLLMPathExtractor

import dotenv
dotenv.load_dotenv()

# Configure global settings
# 1. Initialize the LLM and bind our database tools to it
# (Make sure OPENAI_API_KEY is set in your .env file)
# LLM Setup
api_key = os.getenv("OPENROUTER_API_KEY", "")
if not api_key:
    raise ValueError("OPENROUTER_API_KEY environment variable is required when using OpenRouter. Get your free key from https://openrouter.ai/")

model_name = os.getenv(
    "OPENROUTER_MODEL", 
    # "google/gemini-2.0-flash-exp:free",
    "z-ai/glm-4.5-air:free")
llm = ChatOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=api_key,
    model=model_name,  # Default: Gemini Flash (supports tool calling)
    temperature=0,
    max_retries=5
)
Settings.llm = llm

# OpenAI – from OpenAI/Google (or use openai.com)
Settings.llm = OpenAI(
    model="gpt-5.1",
    api_key=os.getenv("OPENAI_API_KEY"),
)

# DeepSeek – from DeepSeek (or use deepseek.com)
Settings.llm = ChatOpenAI(
    model="deepseek-reasoner",        # This is the actual API model ID
    base_url="https://api.deepseek.com", # Use 'api_base', not 'base_url'
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    temperature=0,
    # context_window=128_000,
    # model_version="DeepSeek-V3.2",
)

# DeepSeek – from DeepSeek (or use deepseek.com)
Settings.llm = OpenAILike(
    model="deepseek-reasoner",        
    api_base="https://api.deepseek.com/v1", # Append /v1 for DeepSeek
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    temperature=0,
    is_chat_model=True,
    context_window=128000,
    
    # --- Concurrency Safety Nets ---
    max_retries=5,       # If DeepSeek says "Too Many Requests", wait and try again up to 5 times.
    timeout=120.0,       # Reasoning models (like deepseek-reasoner) take longer to think. Give concurrent connections 2 minutes before dropping them.
)


# Embeddings via OpenRouter (same key as LLM) — use model_name for custom model IDs
embed_model_name = os.getenv(
    "OPENROUTER_EMBED_MODEL",
    "google/gemini-embedding-001",  # strong MTEB benchmark
)
from llama_index.embeddings.openai import OpenAIEmbedding
Settings.embed_model = OpenAIEmbedding(
    model="text-embedding-3-small",  # bypass enum; model_name overrides for API
    model_name=embed_model_name,
    api_base="https://openrouter.ai/api/v1",
    api_key=api_key,
)

# use openai embeddings instead of openrouter embeddings
# model_name="text-embedding-3-small"
# Settings.embed_model = OpenAIEmbedding(
#     model="text-embedding-3-small",
#     api_base="https://api.openai.com/v1",
#     api_key=os.getenv("OPENAI_API_KEY"),
#     api_key=api_key,
# )

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCS_DIR = os.path.join(BASE_DIR, "data_engineering", "documents")
VECTOR_PERSIST_DIR = os.path.join(BASE_DIR, "data_engineering", "vector_store")
GRAPH_PERSIST_DIR = os.path.join(BASE_DIR, "data_engineering", "graph_store")


def _ts() -> str:
    """UTC timestamp string for versioned index dirs."""
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def get_dangote_graph_extractor(llm: OpenAI) -> SchemaLLMPathExtractor:
    """
    The definitive Omni-Intent ontology for the Dangote Refinery IPO GraphRAG pipeline.
    Captures finance, physical logistics, risk events, and socio-economic impact.
    """
    
    # 1. Define the explicit node labels (The Complete Spectrum)
    Entities = Literal[
        "ORGANIZATION", "FACILITY", "REGULATOR", "PERSON", 
        "PRODUCT", "LOCATION", "FINANCIAL_INSTRUMENT", "CURRENCY", 
        "AGREEMENT", "EVENT", "COMMUNITY", "VESSEL", "MACRO_INDICATOR"
    ]
    
    # 2. Define the explicit edge labels
    Relations = Literal[
        "OWNS", "SUBSIDIARY_OF", "HAS_STAKE_IN", "INVESTS_IN",
        "REGULATES", "COMPLIES_WITH", "BOUND_BY",
        "SUPPLIES_TO", "RECEIVES_FROM", 
        "PRODUCES", "REFINES_INTO", 
        "EXPORTS_TO", "DISTRIBUTES_IN",
        "EXECUTIVE_OF", "BOARD_MEMBER_OF", 
        "COMPETES_WITH", "PRICES_AGAINST",
        "ISSUES", "OWES", "DENOMINATED_IN",
        "DISRUPTS", "IMPACTS", "BENEFITS", "TRANSPORTED_BY", "INFLUENCES"
    ]
    
    # 3. The Validation Schema: Guardrails for the LLM
    validation_schema = {
        "ORGANIZATION": [
            "OWNS", "SUBSIDIARY_OF", "HAS_STAKE_IN", "INVESTS_IN", 
            "SUPPLIES_TO", "COMPETES_WITH", "DISTRIBUTES_IN", 
            "COMPLIES_WITH", "BOUND_BY", "ISSUES", "OWES", "BENEFITS"
        ],
        "FACILITY": ["PRODUCES", "RECEIVES_FROM", "EXPORTS_TO", "DISTRIBUTES_IN", "COMPLIES_WITH", "BENEFITS"],
        "REGULATOR": ["REGULATES"],
        "PERSON": ["EXECUTIVE_OF", "BOARD_MEMBER_OF", "OWNS", "HAS_STAKE_IN"],
        "PRODUCT": ["PRICES_AGAINST", "PRODUCES", "SUPPLIES_TO", "DENOMINATED_IN", "TRANSPORTED_BY"],
        "LOCATION": ["RECEIVES_FROM", "EXPORTS_TO"],
        "FINANCIAL_INSTRUMENT": ["DENOMINATED_IN", "INFLUENCES"],
        "AGREEMENT": ["REGULATES", "INFLUENCES"],
        "EVENT": ["DISRUPTS", "IMPACTS"], # Maps vandalism, policy changes, etc.
        "COMMUNITY": ["RECEIVES_FROM", "COMPLIES_WITH"],
        "VESSEL": ["EXPORTS_TO", "DISTRIBUTES_IN"],
        "MACRO_INDICATOR": ["INFLUENCES", "IMPACTS"]
    }
    
    # 4. Instantiate the Extractor with strict=True
    kg_extractor = SchemaLLMPathExtractor(
        # llm=llm,
        llm=Settings.llm,
        possible_entities=Entities,
        possible_relations=Relations,
        kg_validation_schema=validation_schema,
        strict=True,
        num_workers=5  # <--- THIS IS THE CONCURRENCY ENGINE FOR THE LLM
        
    )
    
    return kg_extractor

class OmniIntentRAG:
    """
    A dual-index RAG system utilizing both VectorStoreIndex (for semantics/sentiment)
    and PropertyGraphIndex (for logistics/corporate structure).

    By default, it will load existing persisted indexes if present. To force a full
    rebuild after new documents have been ingested into `DOCS_DIR`, construct with:

        OmniIntentRAG(force_rebuild_vector=True, force_rebuild_graph=True)
    """
    def __init__(self, force_rebuild_vector: bool = False, force_rebuild_graph: bool = False):
        os.makedirs(DOCS_DIR, exist_ok=True)
        self.documents = self._load_documents()
        
        # Initialize both indexes (optionally forcing a rebuild)
        self.vector_index = self._initialize_vector_index(force_rebuild_vector)
        self.graph_index = self._initialize_graph_index(force_rebuild_graph)
        
        # Initialize the Router
        self.query_engine = self._initialize_router()

    def _load_documents(self):
        if not os.path.exists(DOCS_DIR) or not os.listdir(DOCS_DIR):
            print(f"⚠️ No documents found in {DOCS_DIR}.")
            return []
        # return SimpleDirectoryReader(DOCS_DIR).load_data()
        return SimpleDirectoryReader(input_dir=DOCS_DIR, recursive=True).load_data()

    def _initialize_vector_index(self, force_rebuild: bool = False):
        """Handles unstructured narratives, public sentiment, and educational explanations."""
        # If not forcing a rebuild, first try to load the latest good index.
        if not force_rebuild and os.path.exists(VECTOR_PERSIST_DIR) and os.listdir(VECTOR_PERSIST_DIR):
            try:
                print("Loading existing VectorStoreIndex...")
                storage_context = StorageContext.from_defaults(persist_dir=VECTOR_PERSIST_DIR)
                return load_index_from_storage(storage_context)
            except Exception as e:
                print(f"⚠️ Failed to load vector store ({e}), rebuilding into a new version...")

        if self.documents:
            print("Building new VectorStoreIndex for semantic search...")
            # Build into a versioned temporary dir so we don't destroy the last good index
            tmp_dir = VECTOR_PERSIST_DIR + f"_tmp_{_ts()}"
            index = VectorStoreIndex.from_documents(self.documents)
            index.storage_context.persist(persist_dir=tmp_dir)
            # Swap: remove old dir only after successful build, then move tmp into place
            if os.path.exists(VECTOR_PERSIST_DIR):
                shutil.rmtree(VECTOR_PERSIST_DIR, ignore_errors=True)
            os.rename(tmp_dir, VECTOR_PERSIST_DIR)
            return index
        return None
    
    def _initialize_graph_index(self, force_rebuild: bool = False):
        """Handles corporate governance, supply chain mapping, and multi-step logic."""
        # If not forcing a rebuild, first try to load the latest good graph index.
        if not force_rebuild and os.path.exists(GRAPH_PERSIST_DIR) and os.listdir(GRAPH_PERSIST_DIR):
            try:
                print("Loading existing PropertyGraphIndex...")
                storage_context = StorageContext.from_defaults(persist_dir=GRAPH_PERSIST_DIR)
                return load_index_from_storage(storage_context)
            except Exception as e:
                print(f"⚠️ Failed to load graph store ({e}), rebuilding into a new version...")
        if self.documents:
            print("Extracting entities to build new PropertyGraphIndex...")
            
            # 1. Get the strict extractor we just defined
            kg_extractor = get_dangote_graph_extractor(llm=Settings.llm)
            
            # 2. Pass it into the index generator
            # Build into a versioned temporary dir so we don't destroy the last good graph
            tmp_dir = GRAPH_PERSIST_DIR + f"_tmp_{_ts()}"
            index = PropertyGraphIndex.from_documents(
                self.documents,
                kg_extractors=[kg_extractor], # <--- The schema is enforced here
                show_progress=True
            )
            index.storage_context.persist(persist_dir=tmp_dir)
            # Swap: remove old dir only after successful build, then move tmp into place
            if os.path.exists(GRAPH_PERSIST_DIR):
                shutil.rmtree(GRAPH_PERSIST_DIR, ignore_errors=True)
            os.rename(tmp_dir, GRAPH_PERSIST_DIR)
            return index
        return None

    def _initialize_router(self):
        """Builds the LLM Router Query Engine to seamlessly direct user questions."""
        if not self.vector_index or not self.graph_index:
            return None

        # Create tools for each index with clear descriptions for the Router LLM
        vector_tool = QueryEngineTool.from_defaults(
            query_engine=self.vector_index.as_query_engine(),
            description="Useful for retrieving public sentiment, macroeconomic narratives, historical context, and educational explanations about the refinery."
        )
        
        graph_tool = QueryEngineTool.from_defaults(
            query_engine=self.graph_index.as_query_engine(),
            description="Useful for tracing corporate structure, board members, regulatory compliance rules, and exact physical supply chain logistics (e.g., pipelines to refineries)."
        )

        # The Router analyzes the prompt and picks the best tool
        return RouterQueryEngine(
            selector=LLMSingleSelector.from_defaults(),
            query_engine_tools=[vector_tool, graph_tool],
        )

    def query(self, question: str) -> str:
        if not self.query_engine:
            return "RAG Indexes are currently offline or missing data."
        
        response = self.query_engine.query(question)
        return str(response)

if __name__ == "__main__":
    # For ad-hoc tests you typically want to use existing indexes.
    # If you've just run ingestion and want a clean rebuild, call:
    #   OmniIntentRAG(force_rebuild_vector=True, force_rebuild_graph=True)
    rag = OmniIntentRAG(force_rebuild_vector=False, force_rebuild_graph=True)
    if rag.query_engine:
        # Test a graph-based question
        print(rag.query("Who sits on the board of the refinery?"))
        # Test a vector-based question
        print(rag.query("How do Nigerians feel about the refinery lowering inflation?"))
        
        print(rag.query("is the us-isreal conflict with iran affecting the refinery?"))
        
        