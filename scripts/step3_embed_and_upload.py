#!/usr/bin/env python3
"""
=============================================================
STEP 3: Embed All Chunks and Upload to Qdrant Cloud
=============================================================
LOCAL VERSION — Uses sentence-transformers (no API rate limits!)

Model: all-MiniLM-L6-v2 (80MB, 384 dimensions, runs on CPU)
Speed: ~3,096 chunks in 2-3 minutes on your machine

BEFORE RUNNING:
  pip install sentence-transformers
  Make sure .env has QDRANT_URL and QDRANT_API_KEY

Usage:
  cd ~/hackblr-legal-ai
  source venv/bin/activate
  python scripts/step3_embed_and_upload.py
=============================================================
"""

import json
import os
import time
import sys
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/hackblr-legal-ai/.env"))

# Validate Qdrant keys
for key in ["QDRANT_URL", "QDRANT_API_KEY"]:
    if not os.getenv(key):
        print(f"ERROR: {key} is missing from .env file!")
        sys.exit(1)

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

# Config
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = "indian_legal"
EMBEDDING_DIM = 384  # all-MiniLM-L6-v2 output dimension
BATCH_SIZE = 64      # local model can handle bigger batches
CHUNKS_FILE = os.path.expanduser("~/hackblr-legal-ai/data/legal_chunks.json")

# Initialize
print("Loading embedding model (first time downloads ~80MB)...")
model = SentenceTransformer("all-MiniLM-L6-v2")
print("  Model loaded!")

qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_KEY)


def create_embedding_text(chunk):
    """Create rich text for embedding."""
    parts = []
    if chunk.get("act_name"):
        parts.append(f"Act: {chunk['act_name']}")
    if chunk.get("section"):
        parts.append(f"Section: {chunk['section']}")
    if chunk.get("title"):
        parts.append(f"Title: {chunk['title']}")
    if chunk.get("law_status") == "new":
        parts.append("(New law effective 1 July 2024)")
    if chunk.get("content"):
        parts.append(chunk["content"][:500])
    return " | ".join(parts)


def main():
    print("=" * 60)
    print("STEP 3: Embed and Upload to Qdrant")
    print("  Model: all-MiniLM-L6-v2 (LOCAL — no rate limits!)")
    print(f"  Dimensions: {EMBEDDING_DIM}")
    print("=" * 60)

    # Load chunks
    if not os.path.exists(CHUNKS_FILE):
        print(f"ERROR: {CHUNKS_FILE} not found!")
        print("Run step 2 first: python scripts/step2_process_all_data.py")
        sys.exit(1)

    with open(CHUNKS_FILE, "r", encoding="utf-8") as f:
        chunks = json.load(f)

    print(f"\nLoaded {len(chunks)} chunks")

    # Quick embedding test
    print("\nTesting embedding...")
    test_emb = model.encode("test legal query")
    print(f"  Works! Dimension: {len(test_emb)}")

    # Recreate Qdrant collection
    print(f"\nCreating Qdrant collection: {COLLECTION_NAME}")
    try:
        qdrant.delete_collection(COLLECTION_NAME)
        print("  Deleted old collection")
    except Exception:
        pass

    qdrant.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=EMBEDDING_DIM,
            distance=Distance.COSINE
        )
    )
    print("  Collection created")

    # Prepare all texts
    print(f"\nPreparing texts...")
    all_texts = [create_embedding_text(c) for c in chunks]

    # Embed ALL at once (sentence-transformers handles batching internally)
    print(f"Embedding {len(chunks)} chunks locally...")
    print("(This runs on your CPU/GPU — no API calls, no limits)\n")
    start_time = time.time()

    all_embeddings = model.encode(
        all_texts,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
        normalize_embeddings=True  # Cosine similarity works better normalized
    )

    embed_time = time.time() - start_time
    print(f"\n  Embedding done in {embed_time:.1f}s")

    # Upload to Qdrant in batches
    print(f"\nUploading to Qdrant...")
    total_uploaded = 0
    upload_batch_size = 100  # Qdrant can handle bigger upsert batches

    for i in range(0, len(chunks), upload_batch_size):
        batch_chunks = chunks[i:i + upload_batch_size]
        batch_embeddings = all_embeddings[i:i + upload_batch_size]

        points = []
        for j, (chunk, emb) in enumerate(zip(batch_chunks, batch_embeddings)):
            points.append(PointStruct(
                id=total_uploaded + j,
                vector=emb.tolist(),
                payload={
                    "act_name": chunk.get("act_name", ""),
                    "section": chunk.get("section", ""),
                    "title": chunk.get("title", ""),
                    "content": chunk.get("content", ""),
                    "source": chunk.get("source", ""),
                    "type": chunk.get("type", ""),
                    "law_status": chunk.get("law_status", ""),
                }
            ))

        qdrant.upsert(collection_name=COLLECTION_NAME, points=points)
        total_uploaded += len(points)
        pct = min(total_uploaded / len(chunks) * 100, 100)
        print(f"  [{pct:5.1f}%] Uploaded {total_uploaded}/{len(chunks)}")

    total_time = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"UPLOAD COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Vectors uploaded: {total_uploaded}")
    print(f"  Total time:       {total_time:.0f}s")
    print(f"  Collection:       {COLLECTION_NAME}")

    # Test searches
    print(f"\n{'=' * 60}")
    print("TEST SEARCHES")
    print(f"{'=' * 60}")

    test_queries = [
        "What is the punishment for cheating under new BNS law?",
        "fundamental rights article 21 right to life",
        "non compete clause employment contract India",
        "how to file consumer complaint",
    ]

    for query in test_queries:
        print(f"\n  Q: {query}")
        query_emb = model.encode(query, normalize_embeddings=True).tolist()

        results = qdrant.query_points(
            collection_name=COLLECTION_NAME,
            query=query_emb,
            limit=2
        )
        for r in results.points:
            status = r.payload.get("law_status", "")
            tag = " [NEW LAW]" if status == "new" else ""
            print(f"    -> [{r.score:.3f}]{tag} {r.payload['source']}")
            print(f"       {r.payload['content'][:100]}...")

    # Save model info for the backend to use
    config_file = os.path.expanduser("~/hackblr-legal-ai/embedding_config.json")
    with open(config_file, "w") as f:
        json.dump({
            "model_name": "all-MiniLM-L6-v2",
            "dimension": EMBEDDING_DIM,
            "provider": "local",
            "normalize": True
        }, f, indent=2)
    print(f"\nSaved embedding config to {config_file}")

    print(f"\nDone! Your RAG pipeline is ready.")
    print(f"\nIMPORTANT: You must also update backend/app/tools/legal_search.py")
    print(f"to use the same local model for search queries.")
    print(f"(I'll help you with that next)")


if __name__ == "__main__":
    main()