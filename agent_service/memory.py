"""
vector-based long-term memory of past incidents.

Chroma stores locally in ./chroma_data a folder is created automatically.
"""
import os
import chromadb
from google import genai
from dotenv import load_dotenv

load_dotenv()

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
EMBEDDING_MODEL = "gemini-embedding-001"

chroma_client = chromadb.PersistentClient(path="./chroma_data")
collection = chroma_client.get_or_create_collection(name="incidents")


def embed_text(text: str) -> list[float]:
    result = client.models.embed_content(model=EMBEDDING_MODEL, contents=text)
    return result.embeddings[0].values


def store_incident_memory(incident_id: int, error_message: str, failure_type: str, resolution: str):
    """Called after an incident is resolved — adds it to memory for future lookups."""
    embedding = embed_text(error_message)
    collection.add(
        ids=[str(incident_id)],
        embeddings=[embedding],
        documents=[error_message],
        metadatas=[{"failure_type": failure_type, "resolution": resolution}],
    )


def find_similar_incidents(error_message: str, n_results: int = 2) -> list[dict]:
    """Called BEFORE diagnosing a new incident — looks for similar past ones."""
    if collection.count() == 0:
        return []

    embedding = embed_text(error_message)
    results = collection.query(
        query_embeddings=[embedding],
        n_results=min(n_results, collection.count()),
    )

    similar = []
    for doc, meta, distance in zip(
        results["documents"][0], results["metadatas"][0], results["distances"][0]
    ):
        similar.append({
            "error_message": doc,
            "failure_type": meta["failure_type"],
            "resolution": meta["resolution"],
            "distance": distance,  # lower = more similar
        })
    return similar