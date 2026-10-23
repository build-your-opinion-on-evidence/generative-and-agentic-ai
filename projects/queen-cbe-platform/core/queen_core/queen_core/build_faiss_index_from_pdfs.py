import os
import glob
import pickle
from typing import Any, Dict, List, Tuple

import numpy as np
import faiss
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter

from queen_core.queen import (
    SHARED_DIR,
    INDEX_PATH,
    META_PATH,
    embedder,
)


def get_raw_data_dir() -> str:
    """
    Returns the directory where raw PDFs are stored.
    Expected structure:
      project_root/
        shared/
          data/
            raw/
              *.pdf
    """
    raw_dir = os.path.join(SHARED_DIR, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    return raw_dir


def load_pdfs_as_chunks() -> List[Dict[str, Any]]:
    """
    Load all PDFs from shared/data/raw, split them into text chunks,
    and return a list of dicts: {"text": ..., "source": ...}.
    """
    raw_dir = get_raw_data_dir()
    pdf_files = glob.glob(os.path.join(raw_dir, "*.pdf"))

    if not pdf_files:
        raise RuntimeError(f"No PDF files found in raw data folder: {raw_dir}")

    docs: List[Dict[str, Any]] = []
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=800,
        chunk_overlap=150,
    )

    print(f"Found {len(pdf_files)} PDF files in {raw_dir}")
    for pdf_path in pdf_files:
        print(f"Loading PDF: {pdf_path}")
        loader = PyPDFLoader(pdf_path)
        pages = loader.load()
        for page in pages:
            chunks = splitter.split_documents([page])
            for chunk in chunks:
                docs.append(
                    {
                        "text": chunk.page_content,
                        "source": os.path.basename(pdf_path),
                    }
                )

    print(f"Total chunks created: {len(docs)}")
    return docs


def build_embeddings(docs: List[Dict[str, Any]]) -> np.ndarray:
    """
    Compute embeddings for all chunks using the global SentenceTransformer embedder.
    """
    texts = [d["text"] for d in docs]
    if not texts:
        raise RuntimeError("No texts found to embed.")

    print(f"Building embeddings for {len(texts)} chunks...")
    embeddings = embedder.encode(
        texts,
        batch_size=16,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    embeddings = np.array(embeddings, dtype="float32")
    print("Embeddings shape:", embeddings.shape)
    return embeddings


def build_faiss_index(embeddings: np.ndarray) -> faiss.IndexFlatIP:
    """
    Build an inner-product FAISS index from normalized embeddings.
    """
    dim = embeddings.shape[1]
    print(f"Creating FAISS IndexFlatIP with dim={dim}")
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)
    print("FAISS index built. Total vectors:", index.ntotal)
    return index


def persist_index_and_meta(
    index: faiss.IndexFlatIP,
    docs: List[Dict[str, Any]],
) -> Tuple[str, str]:
    """
    Persist FAISS index and metadata to disk.
    Returns paths to the written files.
    """
    index_dir = os.path.dirname(INDEX_PATH)
    os.makedirs(index_dir, exist_ok=True)

    faiss.write_index(index, INDEX_PATH)
    with open(META_PATH, "wb") as f:
        pickle.dump(docs, f)

    return INDEX_PATH, META_PATH


def main() -> None:
    """
    CLI entry point:
    - Loads PDFs from shared/data/raw
    - Splits into chunks
    - Builds embeddings
    - Builds FAISS index
    - Persists index and metadata under shared/index
    """
    print("Starting FAISS index build from PDFs in shared/data/raw ...")

    docs = load_pdfs_as_chunks()
    embeddings = build_embeddings(docs)
    index = build_faiss_index(embeddings)
    index_path, meta_path = persist_index_and_meta(index, docs)

    print("\nIndex build completed successfully.")
    print(f"FAISS index written to: {index_path}")
    print(f"Metadata written to:   {meta_path}")


if __name__ == "__main__":
    main()

