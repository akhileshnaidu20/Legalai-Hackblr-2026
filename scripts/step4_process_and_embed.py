import json, os, time
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/hackblr-legal-ai/.env"))
model = SentenceTransformer("all-MiniLM-L6-v2")
qdrant = QdrantClient(url=os.getenv("QDRANT_URL"), api_key=os.getenv("QDRANT_API_KEY"))
COLLECTION = "indian_legal"
DATA = os.path.expanduser("~/hackblr-legal-ai/data")

try:
    info = qdrant.get_collection(COLLECTION)
    START_ID = info.points_count + 100
    print(f"Current vectors in Qdrant: {info.points_count}")
except:
    START_ID = 5000

all_chunks = []

def read_file(filepath):
    items = []
    with open(filepath, "r", encoding="utf-8") as f:
        first = f.read(2).strip()
    with open(filepath, "r", encoding="utf-8") as f:
        if first == "[":
            try:
                items = json.load(f)
            except:
                pass
        else:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    items.append(json.loads(line))
                except:
                    continue
    return items

def extract_fields(item):
    q_keys = ["question","Question","instruction","Instruction","input","prompt","query"]
    a_keys = ["answer","Answer","output","Output","response","Response","completion"]
    q = ""
    a = ""
    for k in q_keys:
        v = item.get(k, "")
        if v and len(str(v)) > 5:
            q = str(v); break
    for k in a_keys:
        v = item.get(k, "")
        if v and len(str(v)) > 20:
            a = str(v); break
    ctx = str(item.get("context", item.get("Context", "")))
    case = str(item.get("case_name", item.get("Case Name", item.get("case", ""))))
    text = str(item.get("text", item.get("chunk", item.get("content", ""))))
    return q, a, ctx, case, text

def process_file(filepath, source_name):
    items = read_file(filepath)
    count = 0
    for item in items:
        if not isinstance(item, dict): continue
        q, a, ctx, case, text = extract_fields(item)
        if a and len(a) > 30:
            content = ""
            if case and len(case) > 3: content += f"Case: {case}\n"
            if q: content += f"Q: {q}\n"
            if ctx and len(ctx) > 10: content += f"Context: {ctx[:500]}\n"
            content += f"A: {a}"
            all_chunks.append({
                "act_name": source_name,
                "section": "",
                "title": q[:150] if q else a[:100],
                "content": content[:1500],
                "source": source_name,
                "type": "qa_pair",
                "law_status": "current"
            })
            count += 1
        elif text and len(text) > 50:
            all_chunks.append({
                "act_name": source_name,
                "section": "",
                "title": text[:100],
                "content": text[:1500],
                "source": source_name,
                "type": "legal_text",
                "law_status": "current"
            })
            count += 1
    return count

print("=" * 60)
print("STEP 4: Process & Embed Credible Datasets")
print("=" * 60)

files = {
    "mendeley_indiclegalqa.json": "IndicLegalQA — 10K SC Judgments (Mendeley, CC BY 4.0)",
    "hf_lawyer_gpt.json": "Lawyer GPT India (nisaar, HuggingFace)",
    "hf_legal_4k.json": "Legal Instructions 4.4K (nisaar, HuggingFace)",
    "hf_constitution.json": "Constitution Instructions (nisaar, HuggingFace)",
    "hf_articles_3300.json": "Constitution Articles 3300 (nisaar, HuggingFace)",
    "hf_indian_law.json": "Indian Law Dataset (viber1, HuggingFace)",
    "hf_legal_rag_chunks.json": "Legal RAG Chunks India (ShreyasP123, HuggingFace)",
    "hf_ninadn_legal.json": "Indian Legal Texts (ninadn, HuggingFace)",
    "lawyer_gpt_india.json": "Lawyer GPT India — step1 (nisaar, HuggingFace)",
    "constitution_instructions.json": "Constitution — step1 (nisaar, HuggingFace)",
}

print("\nProcessing datasets...\n")
for filename, source in files.items():
    path = os.path.join(DATA, filename)
    if not os.path.exists(path):
        print(f"  [SKIP] {filename}")
        continue
    count = process_file(path, source)
    print(f"  [OK]   {filename} -> {count} chunks")

if not all_chunks:
    print("\nERROR: No data found!")
    exit(1)

seen = set()
unique = []
for c in all_chunks:
    h = hash(c["content"][:150])
    if h not in seen:
        seen.add(h)
        unique.append(c)

print(f"\n  Total extracted: {len(all_chunks)}")
print(f"  After dedup:     {len(unique)}")

MAX = 5000
if len(unique) > MAX:
    print(f"  Capping at {MAX} for free tier")
    unique = unique[:MAX]

print(f"\nEmbedding {len(unique)} chunks locally...")
texts = []
for c in unique:
    parts = []
    if c["act_name"]: parts.append(c["act_name"])
    if c["title"]: parts.append(c["title"][:100])
    parts.append(c["content"][:500])
    texts.append(" | ".join(parts))

embeddings = model.encode(texts, batch_size=64, show_progress_bar=True, normalize_embeddings=True)

print(f"\nUploading to Qdrant...")
total = 0
for i in range(0, len(unique), 100):
    bc = unique[i:i+100]
    be = embeddings[i:i+100]
    points = []
    for j, (c, e) in enumerate(zip(bc, be)):
        points.append(PointStruct(
            id=START_ID + total + j,
            vector=e.tolist(),
            payload={
                "act_name": c["act_name"], "section": c["section"],
                "title": c["title"], "content": c["content"],
                "source": c["source"], "type": c["type"],
                "law_status": c["law_status"],
            }
        ))
    qdrant.upsert(collection_name=COLLECTION, points=points)
    total += len(points)
    print(f"  [{min(total/len(unique)*100,100):5.1f}%] {total}/{len(unique)}")

info = qdrant.get_collection(COLLECTION)
print(f"\n{'='*60}")
print(f"DONE!")
print(f"  New chunks added:        {total}")
print(f"  Total vectors in Qdrant: {info.points_count}")
print(f"  All sources: credible, open-license")
print(f"{'='*60}")

print(f"\n--- Test Searches ---")
tests = [
    "landlord not returning security deposit",
    "punishment for murder under Indian law",
    "fundamental rights constitution",
    "how to file FIR if police refuses",
]
for q in tests:
    emb = model.encode(q, normalize_embeddings=True).tolist()
    r = qdrant.query_points(collection_name=COLLECTION, query=emb, limit=1).points[0]
    print(f"\n  Q: {q}")
    print(f"  -> [{r.score:.3f}] {r.payload['source']}")
    print(f"     {r.payload['title'][:80]}...")
