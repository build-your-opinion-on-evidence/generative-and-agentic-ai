import os
import json
import time
import socket
import subprocess
import pickle
from typing import Any, Dict, List, Tuple

import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
import ollama


# ================== PATHS & CONFIG ================== #

# Project root is three levels above this file:
# core/queen_core/queen_core/queen.py  →  project_root/
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)

SHARED_DIR = os.path.join(PROJECT_ROOT, "shared")
DATA_DIR = os.path.join(SHARED_DIR, "data")          # raw/processed if needed later
INDEX_DIR = os.path.join(SHARED_DIR, "index")
LOGS_DIR = os.path.join(SHARED_DIR, "logs", "rag_queries")

os.makedirs(INDEX_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

INDEX_PATH = os.path.join(INDEX_DIR, "faiss_index.bin")
META_PATH = os.path.join(INDEX_DIR, "faiss_meta.pkl")

EMBED_MODEL_NAME = "Ahmedhisham/queen_of_embedded_egy_20k"
LLM_MODEL = "qwen2.5:1.5b-instruct-q4_0"

# Embedding device can be configured via environment:
#   QUEEN_EMBED_DEVICE=cpu   or   QUEEN_EMBED_DEVICE=cuda
EMBED_DEVICE = os.getenv("QUEEN_EMBED_DEVICE", "cuda")

embedder = SentenceTransformer(EMBED_MODEL_NAME, device=EMBED_DEVICE)


# ================== NETWORK MONITOR (OFFLINE SAFETY) ================== #

NETWORK_EVENTS: List[Dict[str, Any]] = []

_original_socket_connect = socket.socket.connect


def _monitored_connect(self, address):
    """
    Wrapper around socket.connect to record non-local connections.
    Localhost connections (127.0.0.1, ::1, localhost) are ignored.
    """
    try:
        host, port = address
    except Exception:
        host, port = str(address), None

    host_str = str(host)
    if not (
        host_str.startswith("127.")
        or host_str == "localhost"
        or host_str == "::1"
    ):
        event = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "host": host_str,
            "port": port,
        }
        NETWORK_EVENTS.append(event)

    return _original_socket_connect(self, address)


# Patch socket.connect once at import time
socket.socket.connect = _monitored_connect  # type: ignore[assignment]


def snapshot_network_events(start_len: int) -> List[Dict[str, Any]]:
    """
    Return only the network events that occurred after a given index.
    """
    return NETWORK_EVENTS[start_len:]


# ================== GPU MONITOR ================== #

def get_gpu_stats() -> List[Dict[str, Any]]:
    """
    Take a snapshot of GPU utilization and memory usage using nvidia-smi.
    Returns a list of dicts (one per GPU). If anything fails, returns an
    entry with an error message.
    """
    stats: List[Dict[str, Any]] = []
    try:
        cmd = [
            "nvidia-smi",
            "--query-gpu=index,name,utilization.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits",
        ]
        output = subprocess.check_output(cmd).decode("utf-8").strip().splitlines()
        for line in output:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) != 5:
                continue
            idx, name, util, mem_used, mem_total = parts
            stats.append(
                {
                    "index": int(idx),
                    "name": name,
                    "utilization_gpu": float(util),
                    "memory_used_mb": float(mem_used),
                    "memory_total_mb": float(mem_total),
                }
            )
    except Exception as e:
        stats.append({"error": str(e)})
    return stats


# ================== INDEX LOAD (READ-ONLY) ================== #

def load_index_from_disk() -> Tuple[faiss.IndexFlatIP, List[Dict[str, Any]]]:
    """
    Load FAISS index and metadata from disk.
    Index and meta files must already exist (ingestion/indexer_service is responsible).
    """
    if not os.path.exists(INDEX_PATH):
        raise RuntimeError(f"FAISS index file not found: {INDEX_PATH}")
    if not os.path.exists(META_PATH):
        raise RuntimeError(f"FAISS meta file not found: {META_PATH}")

    index = faiss.read_index(INDEX_PATH)
    with open(META_PATH, "rb") as f:
        docs = pickle.load(f)
    return index, docs


# ================== CORE RETRIEVAL ================== #

def retrieve_passages(
    query: str,
    index: faiss.IndexFlatIP,
    docs: List[Dict[str, Any]],
    k: int = 5,
) -> List[Dict[str, Any]]:
    """
    Return top-k most similar chunks for the query from the index.
    """
    q_emb = embedder.encode([query], normalize_embeddings=True)
    q_emb = np.array(q_emb, dtype="float32")
    scores, idxs = index.search(q_emb, k)

    results: List[Dict[str, Any]] = []
    for i, score in zip(idxs[0], scores[0]):
        results.append(
            {
                "text": docs[i]["text"],
                "source": docs[i]["source"],
                "score": float(score),
            }
        )
    return results


# ================== LLM HELPER (Qwen via Ollama) ================== #

def call_llm(system_prompt: str, user_prompt: str) -> str:
    """
    Helper to call Qwen via Ollama with a system-style instruction and user content.
    """
    messages = [
        {"role": "system", "content": system_prompt.strip()},
        {"role": "user", "content": user_prompt.strip()},
    ]
    response = ollama.chat(
        model=LLM_MODEL,
        messages=messages,
        options={"temperature": 0.1},
    )
    return response["message"]["content"]


# ================== LOGGING HELPERS ================== #

def log_step(
    logs: List[Dict[str, Any]],
    step: int,
    agent: str,
    kind: str,
    content: str,
) -> None:
    """
    Append a structured log entry to the agents conversation log.
    """
    logs.append(
        {
            "step": step,
            "agent": agent,
            "kind": kind,
            "content": content,
        }
    )


def persist_logs_to_file(
    query: str,
    logs: List[Dict[str, Any]],
    network_events: List[Dict[str, Any]],
    gpu_stats: List[Dict[str, Any]],
) -> str:
    """
    Persist logs, network events, and GPU stats to a JSON file under LOGS_DIR.
    Returns the path to the created file.
    """
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    safe_query = "".join(c for c in query if c.isalnum() or c in (" ", "_", "-"))[:50]
    safe_query = safe_query.strip().replace(" ", "_") or "query"
    filename = f"logs_{timestamp}_{safe_query}.json"
    filepath = os.path.join(LOGS_DIR, filename)

    payload = {
        "timestamp": timestamp,
        "query": query,
        "logs": logs,
        "network_events": network_events,
        "gpu_stats": gpu_stats,
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return filepath


# ================== AGENTS ================== #

def retrieval_agent(
    query: str,
    index: faiss.IndexFlatIP,
    docs: List[Dict[str, Any]],
    logs: List[Dict[str, Any]],
    step: int,
    top_k: int = 5,
) -> Tuple[Dict[str, Any], int]:
    """
    Agent responsible for retrieving relevant passages from the vector store.
    """
    log_step(
        logs,
        step,
        "retrieval_agent",
        "thought",
        "Received query and will search the vector store.",
    )
    step += 1

    passages = retrieve_passages(query, index, docs, k=top_k)

    summary_sources = ", ".join(sorted({p["source"] for p in passages}))
    log_step(
        logs,
        step,
        "retrieval_agent",
        "observation",
        f"Retrieved {len(passages)} passages from sources: {summary_sources}",
    )
    step += 1

    return {
        "type": "retrieval_agent",
        "query": query,
        "passages": passages,
    }, step


def analysis_agent(
    query: str,
    retrieval_output: Dict[str, Any],
    logs: List[Dict[str, Any]],
    step: int,
) -> Tuple[Dict[str, Any], int]:
    """
    Agent responsible for analyzing retrieved passages and drafting a candidate answer.
    """
    passages = retrieval_output["passages"]
    context_blocks: List[str] = []
    for p in passages:
        context_blocks.append(f"[{p['source']}] {p['text']}")
    context = "\n\n".join(context_blocks)

    system_prompt = """
انت Agent متخصص في تحليل تعليمات البنك المركزي المصري.
مهمتك:
- تعتمد فقط على النصوص المزودة لك كمصدر.
- ممنوع اختراع مصطلحات أو إجراءات غير مذكورة في النص.
- لو السؤال أوسع من النصوص المتاحة، اشرح بوضوح أن النص لا يغطي كل الأجزاء.

قواعد خاصة للقسم [1]:
- كل نقطة في [1] يجب أن تتكون من ثلاثة أسطر بالترتيب:
  1) "الخلاصة: ..." → تلخيص قصير جدًا للنقطة (جملة واحدة).
  2) "النص الداعم: \"...\"" → جملة مقتبسة حرفيًا من النص بين علامتي اقتباس.
  3) "المصدر: اسم_الملف" → اكتب اسم الملف كما هو ظاهر في السياق.
- إذا لم تستطع إيجاد جملة واضحة في النص تدعم النقطة، لا تضع هذه النقطة في [1] إطلاقًا.
- ممنوع استخدام عبارات مثل "يُرجح التحليل" أو "يبدو" أو أي لغة استنتاجية داخل [1]. هذه العبارات مكانها [2].

قواعد خاصة للقسم [2]:
- اكتب فقط استنتاجات مبنية مباشرة على النقاط الموجودة في [1].
- كل نقطة في [2] يجب أن تشير صراحة إلى أي نقطة في [1] تعتمد عليها، مثل:
  "استنتاج مبني على [1-1] و [1-2]: ..."

قواعد خاصة للقسم [3]:
- لا تكتب أي جملة على أنها حقيقة.
- اكتب فقط على شكل:
  - "النص لا يوضح كذا..."
  - "لا توجد معلومات كافية عن كذا..."
  ولا تضف أي تفاصيل غير موجودة في النص.

اكتب مخرجاتك في ثلاثة أقسام واضحة:

[1] نقاط مستندة مباشرة إلى النص (مع النص الداعم والمصدر كما هو موضح أعلاه).
[2] استنتاجات منطقية مسموح بها لكن ليست مذكورة حرفيًا (واكتب أنها استنتاجات).
[3] أمور لا يغطيها النص بوضوح (واكتب أن النص لا يحتوي على معلومات كافية عنها).
"""
    user_prompt = f"""
السؤال:
{query}

النصوص المتاحة من التعليمات (اعتبرها المصدر الوحيد الموثوق):

{context}

المطلوب:
اكتب المخرجات في ثلاث فقرات معنونة:

[1] نقاط مستندة للنص:
- ...

[2] استنتاجات منطقية مسموح بها (غير مذكورة حرفيًا):
- ...

[3] أمور لا يغطيها النص بوضوح:
- ...
"""

    log_step(
        logs,
        step,
        "analysis_agent",
        "action",
        "Will generate a preliminary structured draft ([1]/[2]/[3]) based on retrieved passages.",
    )
    step += 1

    draft_answer = call_llm(system_prompt, user_prompt)

    log_step(
        logs,
        step,
        "analysis_agent",
        "observation",
        "Structured draft answer created by analysis agent.",
    )
    step += 1

    return {
        "type": "analysis_agent",
        "query": query,
        "draft": draft_answer,
        "passages": passages,
    }, step


# ================== QUEEN ORCHESTRATOR (REAL MODE) ================== #

def queen_orchestrator(
    query: str,
    index: faiss.IndexFlatIP,
    docs: List[Dict[str, Any]],
    top_k: int = 5,
) -> Dict[str, Any]:
    """
    Queen orchestrator:
    - Manages the reasoning flow (ReAct-style trace).
    - Calls retrieval_agent to get relevant passages.
    - Calls analysis_agent to draft a preliminary answer.
    - Uses Qwen again to produce the final answer.
    - Tracks network usage and GPU utilization, and persists logs to disk.
    """
    logs: List[Dict[str, Any]] = []
    step = 1

    # Snapshot current network events length
    network_start_len = len(NETWORK_EVENTS)

    log_step(
        logs,
        step,
        "queen",
        "thought",
        "Received user query. Will coordinate retrieval and analysis.",
    )
    step += 1

    log_step(
        logs,
        step,
        "queen",
        "action",
        "Calling retrieval_agent to fetch relevant passages.",
    )
    step += 1

    retrieval_output, step = retrieval_agent(query, index, docs, logs, step, top_k=top_k)

    log_step(
        logs,
        step,
        "queen",
        "action",
        "Calling analysis_agent to draft a structured preliminary answer.",
    )
    step += 1

    analysis_output, step = analysis_agent(query, retrieval_output, logs, step)

    passages = retrieval_output["passages"]
    draft = analysis_output["draft"]

    context_blocks: List[str] = []
    for p in passages:
        context_blocks.append(f"[{p['source']}] {p['text']}")
    context = "\n\n".join(context_blocks)

    system_prompt = """
انت "الملكة" Queen – Orchestrator نهائي لمساعد قانوني في بنك مصري.
معك:
- نصوص تعليمات من البنك المركزي المصري.
- تحليل أولي مقسوم إلى:
  [1] نقاط مستندة للنص.
  [2] استنتاجات منطقية.
  [3] أمور لا يغطيها النص.

قواعد صارمة:
- ممنوع اختراع تعليمات أو سياسات غير موجودة في النصوص.
- لو النص لا يغطي جزءًا من السؤال، قل ذلك بوضوح ولا تحاول ملء الفراغ من خيالك.
- ركّز فقط على ما يغطيه الباب الرابع في المقاطع المتاحة، ولا تفترض محتوى لأجزاء غير موجودة أمامك.

اكتب الإجابة النهائية في شكل:
1) ملخص واضح لما يغطيه النص فعلاً عن الموضوع، مستندًا بالأساس إلى [1].
2) توضيح ما لا يغطيه النص من أجزاء السؤال، مستندًا إلى [3].
3) إشارة قصيرة أن هذه الإجابة مبنية فقط على المقاطع المتاحة وليست على الكتاب بالكامل.
"""
    user_prompt = f"""
السؤال الأصلي:
{query}

النصوص المسترجعة من التعليمات:

{context}

تحليل الـ Analysis Agent (مقسوم إلى [1] و [2] و [3]):

{draft}

المطلوب:
- استخدم فقط ما هو في [1] كمصدر أساسي للإجابة.
- إذا استخدمت أي شيء من [2]، اذكر بوضوح أنه استنتاج وليس نصًا صريحًا.
- إذا كانت أجزاء من السؤال ضمن [3]، اذكر أن النص المتاح لا يغطيها بشكل واضح.
- التزم بالأقسام الثلاثة المطلوبة في النظام أعلاه.
"""

    log_step(
        logs,
        step,
        "queen",
        "thought",
        "Reviewing structured draft and retrieved context to produce final grounded answer.",
    )
    step += 1

    final_answer = call_llm(system_prompt, user_prompt)

    log_step(
        logs,
        step,
        "queen",
        "observation",
        "Final answer generated by the Queen orchestrator.",
    )
    step += 1

    # Snapshot network events for this query
    new_network_events = snapshot_network_events(network_start_len)
    if new_network_events:
        log_step(
            logs,
            step,
            "queen",
            "observation",
            f"Detected {len(new_network_events)} external network connection(s) during this query.",
        )
        step += 1
    else:
        log_step(
            logs,
            step,
            "queen",
            "observation",
            "No external network connections detected during this query.",
        )
        step += 1

    # GPU stats snapshot
    gpu_stats = get_gpu_stats()
    log_step(
        logs,
        step,
        "queen",
        "observation",
        f"GPU stats snapshot: {gpu_stats}",
    )
    step += 1

    # Persist logs + network events + GPU stats to disk
    log_file_path = persist_logs_to_file(query, logs, new_network_events, gpu_stats)
    log_step(
        logs,
        step,
        "queen",
        "info",
        f"Logs, network events, and GPU stats persisted to: {log_file_path}",
    )
    step += 1

    return {
        "answer": final_answer,
        "passages": passages,
        "analysis_draft": draft,
        "logs": logs,
        "network_events": new_network_events,
        "gpu_stats": gpu_stats,
        "log_file_path": log_file_path,
    }


