#!/usr/bin/env python3
"""
=============================================================
STEP 1: Download All Data Sources
=============================================================
Run this FIRST. It clones repos and downloads HuggingFace datasets.

Usage:
  cd ~/hackblr-legal-ai
  source venv/bin/activate
  pip install datasets    # only needed once
  python scripts/step1_download_all_data.py
=============================================================
"""

import os
import subprocess
import sys

PROJECT_DIR = os.path.expanduser("~/hackblr-legal-ai")
DATA_DIR = os.path.join(PROJECT_DIR, "data")


def run(cmd, cwd=None):
    """Run a shell command and print output."""
    print(f"  >> {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd or PROJECT_DIR,
                            capture_output=True, text=True)
    if result.returncode != 0 and result.stderr:
        print(f"  Warning: {result.stderr[:200]}")
    return result.returncode == 0


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    print("=" * 60)
    print("STEP 1: Downloading ALL Legal Data Sources")
    print("=" * 60)

    # -------------------------------------------------------
    # Source 1: civictech-India (OLD laws - IPC, CrPC, CPC etc)
    # You already have this, but let's make sure
    # -------------------------------------------------------
    print("\n[1/5] civictech-India — Old bare acts (IPC, CrPC, CPC...)")
    raw_acts_dir = os.path.join(DATA_DIR, "raw_acts")
    if os.path.exists(os.path.join(raw_acts_dir, "ipc.json")):
        print("  Already downloaded. Skipping.")
    else:
        run(f"git clone --depth 1 https://github.com/civictech-India/Indian-Law-Penal-Code-Json.git {raw_acts_dir}")

    # -------------------------------------------------------
    # Source 2: Pranshu321/IndLegal (IPC-to-BNS mapping JSON)
    # -------------------------------------------------------
    print("\n[2/5] IndLegal — IPC to BNS mapping JSON")
    indlegal_dir = os.path.join(DATA_DIR, "indlegal")
    if os.path.exists(indlegal_dir):
        print("  Already downloaded. Skipping.")
    else:
        run(f"git clone --depth 1 https://github.com/Pranshu321/IndLegal.git {indlegal_dir}")

    # -------------------------------------------------------
    # Source 3: LegalDrafts/IPCTOBNSConverter
    # -------------------------------------------------------
    print("\n[3/5] LegalDrafts — IPC/CrPC/IEA to BNS/BNSS/BSA converter")
    converter_dir = os.path.join(DATA_DIR, "bns_converter")
    if os.path.exists(converter_dir):
        print("  Already downloaded. Skipping.")
    else:
        run(f"git clone --depth 1 https://github.com/LegalDrafts/IPCTOBNSConverter.git {converter_dir}")

    # -------------------------------------------------------
    # Source 4: HuggingFace — Lawyer_GPT_India (QA pairs)
    # -------------------------------------------------------
    print("\n[4/5] HuggingFace — Lawyer_GPT_India QA pairs")
    lawyer_gpt_file = os.path.join(DATA_DIR, "lawyer_gpt_india.json")
    if os.path.exists(lawyer_gpt_file):
        print("  Already downloaded. Skipping.")
    else:
        try:
            from datasets import load_dataset
            ds = load_dataset("nisaar/Lawyer_GPT_India")
            ds["train"].to_json(lawyer_gpt_file)
            print(f"  Downloaded {len(ds['train'])} QA pairs")
        except Exception as e:
            print(f"  Could not download: {e}")
            print("  Run: pip install datasets   then try again")

    # -------------------------------------------------------
    # Source 5: HuggingFace — Constitution instruction set
    # -------------------------------------------------------
    print("\n[5/5] HuggingFace — Constitution of India instruction set")
    constitution_file = os.path.join(DATA_DIR, "constitution_instructions.json")
    if os.path.exists(constitution_file):
        print("  Already downloaded. Skipping.")
    else:
        try:
            from datasets import load_dataset
            ds = load_dataset("nisaar/Constitution_Of_India_Instruction_Set")
            ds["train"].to_json(constitution_file)
            print(f"  Downloaded {len(ds['train'])} entries")
        except Exception as e:
            print(f"  Could not download: {e}")
            print("  Run: pip install datasets   then try again")

    # -------------------------------------------------------
    # Summary
    # -------------------------------------------------------
    print("\n" + "=" * 60)
    print("Download Summary:")
    print("=" * 60)
    sources = [
        ("Old bare acts (civictech)", raw_acts_dir),
        ("IPC-to-BNS mapping (IndLegal)", indlegal_dir),
        ("BNS converter (LegalDrafts)", converter_dir),
        ("Lawyer GPT QA pairs", lawyer_gpt_file),
        ("Constitution instructions", constitution_file),
    ]
    for name, path in sources:
        exists = "OK" if os.path.exists(path) else "MISSING"
        print(f"  [{exists}] {name}")

    print("\nDone! Now run: python scripts/step2_process_all_data.py")


if __name__ == "__main__":
    main()