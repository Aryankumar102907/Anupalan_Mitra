"""
knowledge_base_loader.py
Indexes all ISO PDFs into a persistent ChromaDB vector store.
Uses ChromaDB's DefaultEmbeddingFunction (all-MiniLM-L6-v2 via ONNX Runtime).

✅  No PyTorch required — pure ONNX, works on Python 3.14
✅  No API key / rate limits for embeddings
✅  Model cached locally (~80 MB, downloaded once)

Run this ONCE (or re-run when you add new PDFs):
    python knowledge_base_loader.py
"""

import os
import sys
import time
import pdfplumber
import chromadb
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

# ── Config ────────────────────────────────────────────────────────────────────
KB_DIR          = os.path.join(os.path.dirname(__file__), "knowledge_base")
CHROMA_DIR      = os.path.join(os.path.dirname(__file__), "chroma_db")
COLLECTION_NAME = "iso_frameworks"
BATCH_SIZE      = 100   # DefaultEmbeddingFunction handles large batches fine

# ── ISO Metadata ──────────────────────────────────────────────────────────────
ISO_META = [
    ("ISO-37001", "ISO 37001 (Anti-bribery Management Systems)"),
    ("ISO-37002", "ISO 37002 (Whistleblowing Management Systems)"),
    ("ISO-37301", "ISO 37301 (Compliance Management Systems)"),
    ("ISO-37000", "ISO 37000 (Governance of Organizations)"),
]

def _iso_name_for(filename: str) -> str:
    stem = os.path.splitext(filename)[0]
    for key, label in ISO_META:
        if key.upper() in stem.upper():
            return label
    return stem


def extract_chunks(filepath: str, chunk_size: int = 800, overlap: int = 150) -> list[dict]:
    """Extract text page-by-page, split into overlapping chunks."""
    chunks   = []
    iso_name = _iso_name_for(os.path.basename(filepath))

    with pdfplumber.open(filepath) as pdf:
        full_text = ""
        page_map  = []
        for page_no, page in enumerate(pdf.pages, start=1):
            text  = (page.extract_text() or "").strip()
            start = len(full_text)
            full_text += text + "\n"
            page_map.append((start, len(full_text), page_no))

        step = chunk_size - overlap
        for i in range(0, max(1, len(full_text) - overlap), step):
            chunk = full_text[i: i + chunk_size].strip()
            if len(chunk) < 80:
                continue
            pg = 1
            for (s, e, pn) in page_map:
                if s <= i < e:
                    pg = pn
                    break
            chunks.append({
                "text":     chunk,
                "source":   iso_name,
                "filename": os.path.basename(filepath),
                "page":     str(pg),
            })
    return chunks


def build_index():
    files = sorted(f for f in os.listdir(KB_DIR) if f.lower().endswith(".pdf"))
    if not files:
        print("No PDFs found in knowledge_base/. Aborting.")
        sys.exit(1)

    print(f"Found {len(files)} PDFs – parsing …\n")

    # ── Collect all chunks ─────────────────────────────────────────────────
    all_texts, all_metas, all_ids = [], [], []
    for filename in files:
        path   = os.path.join(KB_DIR, filename)
        chunks = extract_chunks(path)
        label  = _iso_name_for(filename)
        print(f"  [{label}] → {len(chunks)} chunks")
        for chunk in chunks:
            uid = f"{filename}_c{len(all_ids)}"
            all_texts.append(chunk["text"])
            all_metas.append({
                "source":   chunk["source"],
                "filename": chunk["filename"],
                "page":     chunk["page"],
            })
            all_ids.append(uid)

    total = len(all_texts)
    print(f"\nTotal chunks : {total}")
    print(f"Embed model  : all-MiniLM-L6-v2 via ONNX (offline, no API key)\n")

    # ── Create embedding function (ONNX, no PyTorch) ───────────────────────
    print("Loading ONNX embedding model (cached at ~/.cache/chroma/) …")
    t0 = time.time()
    ef = DefaultEmbeddingFunction()

    # ── Create / recreate ChromaDB collection ──────────────────────────────
    client = chromadb.PersistentClient(path=CHROMA_DIR)
    try:
        client.delete_collection(COLLECTION_NAME)
        print("Deleted existing collection (clean re-index).")
    except Exception:
        pass

    collection = client.create_collection(
        name=COLLECTION_NAME,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )

    # ── Upsert in batches (embedding happens inside ChromaDB) ──────────────
    print(f"Embedding + indexing {total} chunks …")
    for start in range(0, total, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total)
        collection.upsert(
            ids        = all_ids[start:end],
            documents  = all_texts[start:end],
            metadatas  = all_metas[start:end],
        )
        pct = int(end / total * 100)
        bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
        print(f"  [{bar}] {end}/{total}  ({pct}%)", end="\r")

    elapsed = time.time() - t0
    print(f"\n\n✅  Indexed {total} chunks in {elapsed:.0f}s")
    print(f"    Stored in: {CHROMA_DIR}")
    print("    Next step: run  python main.py\n")


if __name__ == "__main__":
    build_index()
