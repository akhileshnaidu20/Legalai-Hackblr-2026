#!/usr/bin/env python3
"""
=============================================================
STEP 2: Process ALL Data Sources into Unified Chunks
=============================================================
This replaces your old process_legal_data.py and add_legal_knowledge.py.
It processes ALL 5 sources into a single legal_chunks.json file.

Usage:
  cd ~/hackblr-legal-ai
  source venv/bin/activate
  python scripts/step2_process_all_data.py
=============================================================
"""

import json
import os
import sqlite3
import glob
import re
from pathlib import Path

PROJECT_DIR = os.path.expanduser("~/hackblr-legal-ai")
DATA_DIR = os.path.join(PROJECT_DIR, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "legal_chunks.json")

all_chunks = []
stats = {}


def add_chunks(source_name, chunks):
    """Add chunks to global list and track stats."""
    global all_chunks
    all_chunks.extend(chunks)
    stats[source_name] = len(chunks)
    print(f"  >> {source_name}: {len(chunks)} chunks")


# =============================================================
# SOURCE 1: civictech-India — Old Bare Acts (IPC, CrPC, CPC...)
# =============================================================
def process_civictech_json():
    """Process JSON files from civictech-India repo."""
    print("\n[1/5] Processing civictech-India bare acts...")
    raw_dir = os.path.join(DATA_DIR, "raw_acts")
    chunks = []

    json_files = glob.glob(os.path.join(raw_dir, "**/*.json"), recursive=True)
    for filepath in json_files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        act_name = os.path.basename(filepath).replace(".json", "").replace("_", " ").upper()

        items = data if isinstance(data, list) else []
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    items.extend(v)

        for item in items:
            if not isinstance(item, dict):
                continue
            section = ""
            title = ""
            content = ""
            for key, value in item.items():
                if value is None:
                    continue
                k = key.lower()
                v = str(value)
                if "section" in k and len(v) < 20:
                    section = v
                elif "title" in k or "heading" in k:
                    title = v
                elif "desc" in k or "content" in k or "text" in k or "definition" in k:
                    content = v
                elif len(v) > 50 and not content:
                    content = v

            if content and len(content) > 20:
                chunks.append({
                    "act_name": act_name,
                    "section": section,
                    "title": title,
                    "content": content,
                    "source": f"{act_name} Section {section}" if section else act_name,
                    "type": "bare_act",
                    "law_status": "old"  # Pre-2024 law
                })

    # Also process SQLite
    db_files = glob.glob(os.path.join(raw_dir, "**/*.db"), recursive=True)
    for db_path in db_files:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()

            for (table_name,) in tables:
                cursor.execute(f"SELECT * FROM [{table_name}]")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                for row in rows:
                    row_dict = dict(zip(columns, row))
                    section = ""
                    description = ""
                    title = ""
                    for col, val in row_dict.items():
                        if val is None:
                            continue
                        val = str(val)
                        cl = col.lower()
                        if "section" in cl and len(val) < 20:
                            section = val
                        elif "title" in cl or "heading" in cl:
                            title = val
                        elif "desc" in cl or "content" in cl or "text" in cl:
                            description = val

                    if description and len(description) > 20:
                        chunks.append({
                            "act_name": table_name.replace("_", " ").upper(),
                            "section": section,
                            "title": title,
                            "content": description,
                            "source": f"{table_name} Section {section}",
                            "type": "bare_act",
                            "law_status": "old"
                        })
            conn.close()
        except Exception as e:
            print(f"  Warning: SQLite error: {e}")

    add_chunks("civictech Bare Acts", chunks)


# =============================================================
# SOURCE 2: IndLegal — IPC to BNS Mapping (NEW LAWS!)
# =============================================================
def process_indlegal_mapping():
    """Process the IPC-to-BNS mapping JSON from IndLegal repo."""
    print("\n[2/5] Processing IndLegal IPC→BNS mapping...")
    mapping_dir = os.path.join(DATA_DIR, "indlegal", "mapping")
    chunks = []

    # Find all JSON files in the mapping directory
    if not os.path.exists(mapping_dir):
        print("  IndLegal mapping dir not found. Skipping.")
        add_chunks("IndLegal BNS Mapping", chunks)
        return

    json_files = glob.glob(os.path.join(mapping_dir, "*.json"))
    for filepath in json_files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        filename = os.path.basename(filepath).lower()

        # Handle different structures
        if isinstance(data, dict):
            for old_section, new_info in data.items():
                if isinstance(new_info, str):
                    content = (
                        f"IPC Section {old_section} corresponds to "
                        f"BNS Section {new_info}. "
                        f"The Bharatiya Nyaya Sanhita (BNS) 2023 replaced "
                        f"the Indian Penal Code (IPC) from 1 July 2024."
                    )
                    chunks.append({
                        "act_name": "BNS (Bharatiya Nyaya Sanhita) 2023",
                        "section": new_info,
                        "title": f"IPC {old_section} → BNS {new_info}",
                        "content": content,
                        "source": f"IPC Section {old_section} → BNS Section {new_info}",
                        "type": "mapping",
                        "law_status": "new"
                    })
                elif isinstance(new_info, dict):
                    bns_section = new_info.get("bns", new_info.get("section", ""))
                    description = new_info.get("description", new_info.get("title", ""))
                    punishment = new_info.get("punishment", "")

                    content = (
                        f"IPC Section {old_section} is now BNS Section {bns_section}. "
                        f"{description}. "
                    )
                    if punishment:
                        content += f"Punishment: {punishment}. "
                    content += (
                        "The Bharatiya Nyaya Sanhita (BNS) 2023 replaced "
                        "the Indian Penal Code (IPC) effective 1 July 2024."
                    )

                    chunks.append({
                        "act_name": "BNS (Bharatiya Nyaya Sanhita) 2023",
                        "section": str(bns_section),
                        "title": f"IPC {old_section} → BNS {bns_section}: {description}",
                        "content": content,
                        "source": f"IPC {old_section} → BNS {bns_section}",
                        "type": "mapping",
                        "law_status": "new"
                    })

        elif isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                ipc = item.get("ipc", item.get("old_section", item.get("IPC", "")))
                bns = item.get("bns", item.get("new_section", item.get("BNS", "")))
                desc = item.get("description", item.get("title", item.get("offence", "")))
                punishment = item.get("punishment", "")

                if not ipc and not bns:
                    continue

                content = f"IPC Section {ipc} corresponds to BNS Section {bns}. "
                if desc:
                    content += f"Offence: {desc}. "
                if punishment:
                    content += f"Punishment: {punishment}. "
                content += "BNS 2023 replaced IPC effective 1 July 2024."

                chunks.append({
                    "act_name": "BNS (Bharatiya Nyaya Sanhita) 2023",
                    "section": str(bns),
                    "title": f"IPC {ipc} → BNS {bns}: {desc}",
                    "content": content,
                    "source": f"IPC {ipc} → BNS {bns}",
                    "type": "mapping",
                    "law_status": "new"
                })

    add_chunks("IndLegal BNS Mapping", chunks)


# =============================================================
# SOURCE 3: LegalDrafts — BNS/BNSS/BSA converter data
# =============================================================
def process_legaldrafts_converter():
    """Extract mapping data from LegalDrafts HTML converter files."""
    print("\n[3/5] Processing LegalDrafts converter data...")
    converter_dir = os.path.join(DATA_DIR, "bns_converter")
    chunks = []

    if not os.path.exists(converter_dir):
        print("  LegalDrafts converter dir not found. Skipping.")
        add_chunks("LegalDrafts Converter", chunks)
        return

    # Look for any JSON or JS data files
    for ext in ["*.json", "*.js", "*.csv"]:
        files = glob.glob(os.path.join(converter_dir, "**", ext), recursive=True)
        for filepath in files:
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    content = f.read()

                # Try to extract JSON arrays/objects from JS files
                if filepath.endswith(".js"):
                    # Look for array patterns like [{...}, {...}]
                    matches = re.findall(r'\[[\s\S]*?\{[\s\S]*?\}[\s\S]*?\]', content)
                    for match in matches:
                        try:
                            data = json.loads(match)
                            if isinstance(data, list) and len(data) > 5:
                                for item in data:
                                    if isinstance(item, dict):
                                        ipc = str(item.get("ipc", item.get("old", "")))
                                        bns = str(item.get("bns", item.get("new", "")))
                                        desc = str(item.get("desc", item.get("title", "")))
                                        if ipc or bns:
                                            chunks.append({
                                                "act_name": "BNS/BNSS/BSA 2023",
                                                "section": bns,
                                                "title": f"{ipc} → {bns}: {desc}",
                                                "content": f"Old section {ipc} maps to new section {bns}. {desc}. New criminal codes effective 1 July 2024.",
                                                "source": f"LegalDrafts Converter: {ipc} → {bns}",
                                                "type": "mapping",
                                                "law_status": "new"
                                            })
                        except json.JSONDecodeError:
                            continue

                elif filepath.endswith(".json"):
                    data = json.loads(content)
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                ipc = str(item.get("ipc", item.get("old", "")))
                                bns = str(item.get("bns", item.get("new", "")))
                                desc = str(item.get("desc", item.get("title", "")))
                                if ipc or bns:
                                    chunks.append({
                                        "act_name": "BNS/BNSS/BSA 2023",
                                        "section": bns,
                                        "title": f"{ipc} → {bns}",
                                        "content": f"Old section {ipc} maps to new section {bns}. {desc}. New criminal codes effective 1 July 2024.",
                                        "source": f"LegalDrafts: {ipc} → {bns}",
                                        "type": "mapping",
                                        "law_status": "new"
                                    })

            except Exception as e:
                continue

    add_chunks("LegalDrafts Converter", chunks)


# =============================================================
# SOURCE 4: HuggingFace — Lawyer_GPT_India (QA pairs)
# =============================================================
def process_lawyer_gpt():
    """Process the Lawyer GPT India QA dataset."""
    print("\n[4/5] Processing Lawyer_GPT_India QA pairs...")
    filepath = os.path.join(DATA_DIR, "lawyer_gpt_india.json")
    chunks = []

    if not os.path.exists(filepath):
        print("  File not found. Skipping.")
        add_chunks("Lawyer GPT India", chunks)
        return

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            # HuggingFace .to_json() writes one JSON object per line
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Extract question and answer
                question = item.get("Question", item.get("question", item.get("instruction", "")))
                answer = item.get("Answer", item.get("answer", item.get("output", "")))

                if not question or not answer:
                    continue

                # Combine Q&A into a chunk
                content = f"Q: {question}\nA: {answer}"

                # Try to detect which area of law this is about
                act_name = "Indian Polity & Constitutional Law"
                text_lower = content.lower()
                if any(w in text_lower for w in ["ipc", "penal", "criminal", "bns"]):
                    act_name = "Criminal Law"
                elif any(w in text_lower for w in ["contract", "agreement", "breach"]):
                    act_name = "Contract Law"
                elif any(w in text_lower for w in ["fundamental right", "article 14", "article 19", "article 21"]):
                    act_name = "Constitutional Law — Fundamental Rights"
                elif any(w in text_lower for w in ["parliament", "lok sabha", "rajya sabha", "bill"]):
                    act_name = "Constitutional Law — Legislature"
                elif any(w in text_lower for w in ["supreme court", "high court", "judiciary"]):
                    act_name = "Constitutional Law — Judiciary"

                chunks.append({
                    "act_name": act_name,
                    "section": "",
                    "title": question[:100],
                    "content": content[:1500],  # Cap length
                    "source": "Lawyer GPT India — HuggingFace Dataset",
                    "type": "qa_pair",
                    "law_status": "current"
                })
    except Exception as e:
        print(f"  Error: {e}")

    add_chunks("Lawyer GPT India", chunks)


# =============================================================
# SOURCE 5: HuggingFace — Constitution of India Instructions
# =============================================================
def process_constitution_instructions():
    """Process the Constitution instruction dataset."""
    print("\n[5/5] Processing Constitution instruction set...")
    filepath = os.path.join(DATA_DIR, "constitution_instructions.json")
    chunks = []

    if not os.path.exists(filepath):
        print("  File not found. Skipping.")
        add_chunks("Constitution Instructions", chunks)
        return

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue

                instruction = item.get("instruction", item.get("Instruction", ""))
                inp = item.get("input", item.get("Input", ""))
                output = item.get("output", item.get("Output", ""))

                if not output or len(output) < 30:
                    continue

                content = ""
                if instruction:
                    content += f"Instruction: {instruction}\n"
                if inp:
                    content += f"Context: {inp}\n"
                content += f"Answer: {output}"

                chunks.append({
                    "act_name": "Constitution of India",
                    "section": "",
                    "title": instruction[:100] if instruction else "Constitutional Provision",
                    "content": content[:1500],
                    "source": "Constitution of India — HuggingFace Dataset",
                    "type": "instruction",
                    "law_status": "current"
                })
    except Exception as e:
        print(f"  Error: {e}")

    add_chunks("Constitution Instructions", chunks)


# =============================================================
# BONUS: Curated key legal knowledge (always included)
# =============================================================
def add_curated_knowledge():
    """Add hand-curated key legal principles that may not be in any repo."""
    print("\n[BONUS] Adding curated landmark legal knowledge...")
    chunks = [
        {
            "act_name": "BNS (Bharatiya Nyaya Sanhita) 2023",
            "section": "Overview",
            "title": "New Criminal Law Framework 2024",
            "content": "India replaced its three colonial-era criminal laws effective 1 July 2024. The Indian Penal Code 1860 (IPC) was replaced by Bharatiya Nyaya Sanhita (BNS) 2023. The Code of Criminal Procedure 1973 (CrPC) was replaced by Bharatiya Nagarik Suraksha Sanhita (BNSS) 2023. The Indian Evidence Act 1872 (IEA) was replaced by Bharatiya Sakshya Adhiniyam (BSA) 2023. Key changes in BNS: sedition removed, new offences for organised crime and terrorism, community service as punishment, increased penalties for crimes against women and children. Key changes in BNSS: mandatory videography of crime scenes, electronic FIR filing, zero FIR (file at any police station), 90-day deadline for charge sheet, summary trials for petty offences up to 3 years. Key changes in BSA: electronic and digital records given same legal status as paper records, admissibility of electronic evidence strengthened.",
            "source": "New Criminal Laws Overview 2024",
            "type": "overview",
            "law_status": "new"
        },
        {
            "act_name": "Indian Contract Act, 1872",
            "section": "Section 27",
            "title": "Non-compete clauses void in India",
            "content": "Under Section 27 of the Indian Contract Act 1872, every agreement by which anyone is restrained from exercising a lawful profession, trade or business of any kind, is to that extent void. This means non-compete clauses in employment contracts are generally unenforceable in India. The only exception is when someone sells the goodwill of a business — they may agree not to carry on similar business within specified local limits. Courts have consistently held that post-employment non-compete restrictions are void. However, non-solicitation clauses and confidentiality agreements are generally enforceable as they protect legitimate business interests without restraining trade.",
            "source": "Indian Contract Act Section 27",
            "type": "bare_act",
            "law_status": "current"
        },
        {
            "act_name": "Consumer Protection Act, 2019",
            "section": "Sections 34-37",
            "title": "Consumer complaint filing and jurisdiction",
            "content": "Under the Consumer Protection Act 2019, complaints can be filed at three levels based on value: District Commission for claims up to Rs 1 crore, State Commission for Rs 1-10 crore, National Commission for above Rs 10 crore. Complaints can be filed electronically (e-filing). No court fee is required for filing. Complaint must be filed within 2 years from cause of action. The Act introduced product liability, making manufacturers, sellers and service providers liable. Central Consumer Protection Authority (CCPA) can investigate suo motu, recall products, and impose penalties for misleading advertisements up to Rs 50 lakh (first offence).",
            "source": "Consumer Protection Act 2019",
            "type": "bare_act",
            "law_status": "current"
        },
        {
            "act_name": "Legal Principles",
            "section": "Landmark Cases",
            "title": "Key Supreme Court judgments every Indian should know",
            "content": "Kesavananda Bharati v State of Kerala 1973: Parliament cannot amend basic structure of Constitution. Maneka Gandhi v Union of India 1978: Right to life under Article 21 includes right to live with dignity, procedure must be fair and reasonable. Vishaka v State of Rajasthan 1997: Guidelines against sexual harassment at workplace (later replaced by POSH Act 2013). Justice K.S. Puttaswamy v Union of India 2017: Right to privacy is a fundamental right under Article 21. Navtej Singh Johar v Union of India 2018: Section 377 IPC decriminalized consensual same-sex relations. Joseph Shine v Union of India 2018: Section 497 IPC (adultery) struck down as unconstitutional. Indian Young Lawyers Association v State of Kerala 2018: Sabarimala — women of all ages can enter temple.",
            "source": "Landmark Supreme Court Judgments",
            "type": "case_law",
            "law_status": "current"
        },
        {
            "act_name": "Information Technology Act, 2000",
            "section": "Key Sections",
            "title": "Cyber law and digital offences",
            "content": "Section 43: Penalty for unauthorized access to computer, up to Rs 1 crore compensation. Section 66: Computer related offences, imprisonment up to 3 years and/or fine up to Rs 5 lakh. Section 66A: Struck down by Supreme Court in Shreya Singhal v Union of India 2015 (sending offensive messages online). Section 66B: Punishment for dishonestly receiving stolen computer resource. Section 66C: Identity theft, imprisonment up to 3 years and fine up to Rs 1 lakh. Section 66D: Cheating by impersonation using computer resource. Section 67: Publishing obscene material in electronic form. Section 72: Penalty for breach of confidentiality and privacy. The Digital Personal Data Protection Act 2023 (DPDP) now governs data privacy separately.",
            "source": "IT Act 2000 and DPDP Act 2023",
            "type": "bare_act",
            "law_status": "current"
        },
    ]
    add_chunks("Curated Knowledge", chunks)


# =============================================================
# MAIN: Run everything and deduplicate
# =============================================================
def main():
    print("=" * 60)
    print("STEP 2: Processing ALL Legal Data Sources")
    print("=" * 60)

    # Process each source
    process_civictech_json()
    process_indlegal_mapping()
    process_legaldrafts_converter()
    process_lawyer_gpt()
    process_constitution_instructions()
    add_curated_knowledge()

    # Deduplicate by content
    print("\n" + "-" * 40)
    print("Deduplicating...")
    seen = set()
    unique_chunks = []
    for chunk in all_chunks:
        content_hash = hash(chunk["content"][:150])
        if content_hash not in seen:
            seen.add(content_hash)
            unique_chunks.append(chunk)

    print(f"Before dedup: {len(all_chunks)}")
    print(f"After dedup:  {len(unique_chunks)}")

    # Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(unique_chunks, f, indent=2, ensure_ascii=False)

    # Final summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    for source, count in stats.items():
        print(f"  {source:.<40} {count:>5} chunks")
    print(f"  {'TOTAL (deduplicated)':.<40} {len(unique_chunks):>5} chunks")

    # Count by law_status
    old_count = sum(1 for c in unique_chunks if c.get("law_status") == "old")
    new_count = sum(1 for c in unique_chunks if c.get("law_status") == "new")
    current_count = sum(1 for c in unique_chunks if c.get("law_status") == "current")
    print(f"\n  Old laws (IPC/CrPC era):   {old_count}")
    print(f"  New laws (BNS/BNSS/BSA):   {new_count}")
    print(f"  Current/general:           {current_count}")

    print(f"\nSaved to: {OUTPUT_FILE}")
    print("\nNext step: python scripts/step3_embed_and_upload.py")


if __name__ == "__main__":
    main()