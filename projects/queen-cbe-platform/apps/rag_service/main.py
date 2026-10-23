import os
import logging
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
import jwt

from queen_core.queen import load_index_from_disk, queen_orchestrator


SERVICE_NAME = "rag_service"

SECRET_KEY = os.environ["AUTH_JWT_SECRET"]
if not SECRET_KEY.strip():
    raise RuntimeError("AUTH_JWT_SECRET must be set before starting the RAG service.")
ALGORITHM = "HS256"

PERMISSION_RAG_QUERY = "rag:query"
PERMISSION_RAG_DEBUG_VIEW = "rag:debug:view"

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SHARED_LOGS_DIR = os.path.join(PROJECT_ROOT, "shared", "logs", "services")
os.makedirs(SHARED_LOGS_DIR, exist_ok=True)


# =========================
# Logging setup
# =========================

logger = logging.getLogger(SERVICE_NAME)
logger.setLevel(logging.INFO)

log_file_path = os.path.join(SHARED_LOGS_DIR, f"{SERVICE_NAME}.log")
file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# =========================
# JWT / RBAC helpers
# =========================

security = HTTPBearer()


class TokenClaims(BaseModel):
    sub: str
    role: Optional[str] = None
    permissions: List[str] = []


def decode_jwt(token: str) -> TokenClaims:
    """
    Decode and validate JWT using HS256.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired.",
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token.",
        )

    permissions = payload.get("permissions", [])
    if permissions is None:
        permissions = []

    return TokenClaims(
        sub=payload.get("sub", "unknown"),
        role=payload.get("role"),
        permissions=list(permissions),
    )


def get_current_claims(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> TokenClaims:
    """
    Extract and validate JWT from Authorization header.
    """
    token = credentials.credentials
    return decode_jwt(token)


def require_permission(permission: str):
    """
    Dependency factory that enforces a specific permission.
    """

    def _dependency(claims: TokenClaims = Depends(get_current_claims)) -> TokenClaims:
        if permission not in claims.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing permission: {permission}",
            )
        return claims

    return _dependency


# =========================
# Pydantic models
# =========================

class RAGQueryRequest(BaseModel):
    query: str
    debug: bool = False


class PassageModel(BaseModel):
    text: str
    source: str
    score: float


class RAGQueryResponse(BaseModel):
    answer: str
    passages: Optional[List[PassageModel]] = None
    analysis_draft: Optional[str] = None
    gpu_stats: Optional[List[Dict[str, Any]]] = None
    network_events: Optional[List[Dict[str, Any]]] = None
    log_file_path: Optional[str] = None


# =========================
# App init
# =========================

app = FastAPI(
    title="Queen CBE RAG Service",
    description="RAG microservice powered by Queen orchestrator and Qwen via Ollama.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_index = None
_docs: Optional[List[Dict[str, Any]]] = None


# =========================
# Startup / health
# =========================

@app.on_event("startup")
def on_startup() -> None:
    """
    Load FAISS index and docs into memory on service startup.
    """
    global _index, _docs
    logger.info("Starting rag_service and loading FAISS index...")
    try:
        _index, _docs = load_index_from_disk()
        logger.info("FAISS index and metadata loaded successfully.")
        logger.info("rag_service is ready to accept queries.")
    except Exception as exc:
        logger.exception("Failed to load FAISS index or metadata.")
        raise exc


@app.get("/health", tags=["health"])
def health_check() -> Dict[str, Any]:
    """
    Basic health endpoint used by monitoring / load balancer.
    """
    status_ok = _index is not None and _docs is not None
    return {
        "service": SERVICE_NAME,
        "status": "ok" if status_ok else "not_ready",
        "index_loaded": _index is not None,
        "docs_loaded": _docs is not None,
    }


# =========================
# RAG endpoint
# =========================

@app.post("/rag/query", response_model=RAGQueryResponse, tags=["rag"])
async def rag_query(
    request: RAGQueryRequest,
    claims: TokenClaims = Depends(require_permission(PERMISSION_RAG_QUERY)),
) -> RAGQueryResponse:
    """
    Main RAG endpoint:
    - Validates JWT and rag:query permission.
    - Delegates to Queen orchestrator in real mode.
    - Optionally returns debug information if:
      - request.debug is True
      - and caller has rag:debug:view permission.
    """
    if _index is None or _docs is None:
        logger.error("RAG query received but index/docs are not loaded.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Index is not loaded yet. Try again later.",
        )

    logger.info(
        "RAG query received",
        extra={"user": claims.sub, "role": claims.role, "debug": request.debug},
    )

    try:
        result = queen_orchestrator(
            query=request.query,
            index=_index,
            docs=_docs,
            top_k=5,
        )
    except Exception as exc:
        logger.exception("Error while running Queen orchestrator.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error while processing RAG query.",
        ) from exc

    answer: str = result["answer"]
    passages_raw: List[Dict[str, Any]] = result["passages"]
    analysis_draft: str = result["analysis_draft"]
    gpu_stats: List[Dict[str, Any]] = result["gpu_stats"]
    network_events: List[Dict[str, Any]] = result["network_events"]
    log_file_path: str = result["log_file_path"]

    response = RAGQueryResponse(answer=answer)

    can_view_debug = PERMISSION_RAG_DEBUG_VIEW in claims.permissions

    if request.debug and can_view_debug:
        response.passages = [
            PassageModel(
                text=p["text"],
                source=p["source"],
                score=float(p["score"]),
            )
            for p in passages_raw
        ]
        response.analysis_draft = analysis_draft
        response.gpu_stats = gpu_stats
        response.network_events = network_events
        response.log_file_path = log_file_path

    return response

