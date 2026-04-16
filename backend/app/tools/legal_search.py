import os
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

load_dotenv(os.path.expanduser("~/hackblr-legal-ai/.env"))

model = SentenceTransformer("all-MiniLM-L6-v2")
qdrant = QdrantClient(url=os.getenv("QDRANT_URL"), api_key=os.getenv("QDRANT_API_KEY"))
COLLECTION_NAME = "indian_legal"

def search_legal_db(query: str, top_k: int = 5) -> list[dict]:
    embedding = model.encode(query, normalize_embeddings=True).tolist()
    results = qdrant.query_points(collection_name=COLLECTION_NAME, query=embedding, limit=top_k)
    formatted = []
    for r in results.points:
        formatted.append({
            "score": round(r.score, 3),
            "act_name": r.payload.get("act_name", ""),
            "section": r.payload.get("section", ""),
            "title": r.payload.get("title", ""),
            "content": r.payload.get("content", ""),
            "source": r.payload.get("source", ""),
            "type": r.payload.get("type", ""),
            "law_status": r.payload.get("law_status", ""),
        })
    return formatted
