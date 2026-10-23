from __future__ import annotations

import os
import glob
import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from pypdf import PdfReader

# Import queen_core index builder (batch rebuild)
from queen_core.queen import build_corpus  # uses shared/index and shared/data automatically


# Paths inside the Docker/Airflow environment
BASE_DIR = os.environ.get("QUEEN_BASE_DIR", "/app")
RAW_DATA_DIR = os.path.join(BASE_DIR, "shared", "data", "raw")
METADATA_DIR = os.path.join(BASE_DIR, "shared", "data", "metadata")
METADATA_PATH = os.path.join(METADATA_DIR, "docs_metadata.jsonl")
INGESTION_LOG_DIR = os.path.join(BASE_DIR, "shared", "logs", "ingestion")


def ensure_dirs() -> None:
    """Ensure required directories exist."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(METADATA_DIR, exist_ok=True)
    os.makedirs(INGESTION_LOG_DIR, exist_ok=True)


def compute_file_hash(path: str, chunk_size: int = 8192) -> str:
    """Compute SHA256 hash for a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def load_metadata() -> Dict[str, Dict]:
    """Load existing metadata from JSONL file into a dict keyed by hash."""
    if not os.path.exists(METADATA_PATH):
        return {}

    meta: Dict[str, Dict] = {}
    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                file_hash = obj.get("hash")
                if file_hash:
                    meta[file_hash] = obj
            except json.JSONDecodeError:
                continue
    return meta


def append_metadata(records: List[Dict]) -> None:
    """Append new metadata records to JSONL file."""
    if not records:
        return
    with open(METADATA_PATH, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def log_ingestion_event(message: str) -> None:
    """Write a simple log line for ingestion runs."""
    ensure_dirs()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(INGESTION_LOG_DIR, f"ingestion_{ts}.log")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")


def scan_and_validate_pdfs(**context) -> int:
    """
    Scan RAW_DATA_DIR for PDFs, compute hashes, validate basic text extraction
    using pypdf (no heavy OCR yet), and update metadata.
    Returns number of new documents discovered.
    """
    ensure_dirs()

    pdf_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.pdf"))
    existing_meta = load_metadata()

    new_records: List[Dict] = []
    new_docs_count = 0

    for pdf_path in pdf_files:
        file_hash = compute_file_hash(pdf_path)
        if file_hash in existing_meta:
            # Already seen this file, skip
            continue

        # Basic validation with pypdf: try to open and extract some text
        doc_ok = True
        text_sample = ""
        try:
            reader = PdfReader(pdf_path)
            if reader.pages:
                first_page = reader.pages[0]
                text_sample = (first_page.extract_text() or "").strip()
        except Exception as e:
            doc_ok = False
            text_sample = f"ERROR: {e}"

        record = {
            "hash": file_hash,
            "filename": os.path.basename(pdf_path),
            "path": pdf_path,
            "validated": doc_ok,
            "text_sample": text_sample[:500],
            "ingested_at_utc": datetime.utcnow().isoformat(),
        }

        new_records.append(record)
        new_docs_count += 1

    append_metadata(new_records)
    log_ingestion_event(
        f"Scanned PDFs. Found {len(pdf_files)} files, {new_docs_count} new documents."
    )

    # Push count to XCom for downstream tasks if needed
    context["ti"].xcom_push(key="new_docs_count", value=new_docs_count)

    return new_docs_count


def rebuild_faiss_index(**context) -> None:
    """
    Trigger a full FAISS index rebuild using queen_core.build_corpus().
    This will read all PDFs from the configured data folder and
    regenerate the embeddings and FAISS index files.
    """
    ensure_dirs()
    log_ingestion_event("Starting FAISS index rebuild via queen_core.build_corpus().")

    # build_corpus is assumed to write index + meta files under shared/index
    index, docs = build_corpus()

    log_ingestion_event(
        f"FAISS index rebuild completed. Total chunks: {len(docs)}."
    )


# DAG definition
default_args = {
    "owner": "queen",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cbe_doc_ingestion_and_indexing",
    default_args=default_args,
    description="Document ingestion + PDF validation (pypdf) + FAISS index rebuild for Queen RAG.",
    schedule_interval=None,  # manual trigger for now
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["queen", "ingestion", "indexing"],
) as dag:

    scan_pdfs_task = PythonOperator(
        task_id="scan_and_validate_pdfs",
        python_callable=scan_and_validate_pdfs,
        provide_context=True,
    )

    rebuild_index_task = PythonOperator(
        task_id="rebuild_faiss_index",
        python_callable=rebuild_faiss_index,
        provide_context=True,
    )

    scan_pdfs_task >> rebuild_index_task

