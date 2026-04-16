"""
Script to process bare acts JSON into chunks ready for embedding.
Run this ONCE to prepare your data.
"""

import json
import os
import sqlite3
import glob

DATA_DIR = os.path.expanduser("~/hackblr-legal-ai/data")
RAW_DIR = os.path.join(DATA_DIR, "raw_acts")
OUTPUT_FILE = os.path.join(DATA_DIR, "legal_chunks.json")


def extract_from_json_files():
    """Extract sections from all JSON files in the raw_acts directory."""
    chunks = []
    json_files = glob.glob(os.path.join(RAW_DIR, "**/*.json"), recursive=True)

    for filepath in json_files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            print(f"  Skipping {filepath} (parse error)")
            continue

        act_name = os.path.basename(filepath).replace(".json", "").replace("_", " ")
        print(f"Processing: {act_name}")

        # Handle different JSON structures
        if isinstance(data, list):
            for item in data:
                chunk = process_item(item, act_name)
                if chunk:
                    chunks.append(chunk)
        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    for item in value:
                        chunk = process_item(item, act_name)
                        if chunk:
                            chunks.append(chunk)
                elif isinstance(value, dict):
                    chunk = process_item(value, act_name)
                    if chunk:
                        chunks.append(chunk)

    return chunks


def extract_from_sqlite():
    """Extract from SQLite DB if present."""
    chunks = []
    db_files = glob.glob(os.path.join(RAW_DIR, "**/*.db"), recursive=True)

    for db_path in db_files:
        print(f"Processing SQLite: {db_path}")
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()

            for (table_name,) in tables:
                cursor.execute(f"SELECT * FROM [{table_name}]")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                for row in rows:
                    row_dict = dict(zip(columns, row))
                    # Try to find section number and description
                    section = ""
                    description = ""
                    title = ""

                    for col, val in row_dict.items():
                        col_lower = col.lower()
                        if val is None:
                            continue
                        val = str(val)
                        if "section" in col_lower and "num" in col_lower:
                            section = val
                        elif "section" in col_lower and len(val) < 20:
                            section = val
                        elif "title" in col_lower or "heading" in col_lower:
                            title = val
                        elif "desc" in col_lower or "content" in col_lower or "text" in col_lower:
                            description = val

                    if description and len(description) > 20:
                        chunks.append({
                            "act_name": table_name.replace("_", " "),
                            "section": section,
                            "title": title,
                            "content": description,
                            "source": f"{table_name} Section {section}",
                            "type": "bare_act"
                        })

            conn.close()
        except Exception as e:
            print(f"  Error with {db_path}: {e}")

    return chunks


def process_item(item, act_name):
    """Process a single item from JSON into a chunk."""
    if not isinstance(item, dict):
        return None

    # Try to extract meaningful content
    section = ""
    title = ""
    content = ""

    for key, value in item.items():
        if value is None:
            continue
        key_lower = key.lower()
        value_str = str(value)

        if "section" in key_lower and len(value_str) < 20:
            section = value_str
        elif "title" in key_lower or "heading" in key_lower:
            title = value_str
        elif "desc" in key_lower or "content" in key_lower or "text" in key_lower or "definition" in key_lower:
            content = value_str
        elif len(value_str) > 50 and not content:
            content = value_str

    if content and len(content) > 20:
        return {
            "act_name": act_name,
            "section": section,
            "title": title,
            "content": content,
            "source": f"{act_name} - Section {section}" if section else act_name,
            "type": "bare_act"
        }
    return None


def main():
    print("=" * 60)
    print("Processing Legal Data for Embedding")
    print("=" * 60)

    all_chunks = []

    # Process JSON files
    json_chunks = extract_from_json_files()
    all_chunks.extend(json_chunks)
    print(f"\nExtracted {len(json_chunks)} chunks from JSON files")

    # Process SQLite
    sqlite_chunks = extract_from_sqlite()
    all_chunks.extend(sqlite_chunks)
    print(f"Extracted {len(sqlite_chunks)} chunks from SQLite")

    # Deduplicate by content
    seen = set()
    unique_chunks = []
    for chunk in all_chunks:
        content_hash = hash(chunk["content"][:100])
        if content_hash not in seen:
            seen.add(content_hash)
            unique_chunks.append(chunk)

    print(f"\nTotal unique chunks: {len(unique_chunks)}")

    # Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(unique_chunks, f, indent=2, ensure_ascii=False)

    print(f"Saved to {OUTPUT_FILE}")

    # Print sample
    if unique_chunks:
        print("\n--- Sample Chunk ---")
        sample = unique_chunks[0]
        print(f"Act: {sample['act_name']}")
        print(f"Section: {sample['section']}")
        print(f"Content: {sample['content'][:200]}...")


if __name__ == "__main__":
    main()