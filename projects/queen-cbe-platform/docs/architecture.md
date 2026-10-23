# Queen architecture and source review

## Online query flow

1. A caller sends a JWT and query to NGINX on port 8080.
2. NGINX forwards the request to FastAPI on container port 8002.
3. FastAPI validates the JWT and `rag:query` permission.
4. Queen embeds the query and retrieves passages from an in-memory FAISS index loaded at startup.
5. The analysis function sends the passages and question to Qwen through Ollama and creates a structured draft.
6. Queen sends the original passages and draft to Qwen for the final answer.
7. Queen persists local query diagnostics. FastAPI returns the answer and, only when authorized, debug fields.

The same Python process runs retrieval, analysis coordination, and final synthesis. Ollama is a separate container. No dynamic planning loop or message broker is implemented here.

## Offline ingestion flow

The active Compose mount is `infra/airflow/dags/`:

1. `scan_and_validate_pdfs` scans `shared/data/raw/*.pdf`, computes hashes, extracts first-page samples, and appends metadata.
2. `rebuild_faiss_index` checks the new-document count.
3. When new documents exist, it imports `build_corpus` from `queen_core.queen`.
4. That function is missing from the source, so this path cannot complete as supplied.

`core/queen_core/queen_core/build_faiss_index_from_pdfs.py` is a separate CLI builder. It loads PDFs, creates chunks of 800 characters with 150-character overlap, embeds them, and writes FAISS plus pickle metadata. It is not currently wired into the active DAG.

`mlops/data_pipelines/airflow_dags/doc_ingestion_dag.py` is an alternative older DAG with the same DAG ID and an eager import of the same missing function. Mounting both directories would introduce duplicate definitions; Compose mounts only the `infra` version.

## Runtime boundaries

| Container | Role | Host port / storage |
| --- | --- | --- |
| `nginx` | HTTP forwarding | `8080`; read-only gateway config |
| `rag-service` | JWT checks, retrieval, generation coordination | `8002`; bind-mounted `shared/` |
| `ollama` | Model inference | Internal Docker network; named model volume |
| `airflow` | Manual ingestion DAG | `8081`; DAG/log mounts and bind-mounted `shared/` |

The RAG host port remains published in the imported topology; requests made directly to it bypass NGINX, while JWT checks remain in FastAPI.

## Known blockers and limitations

| Finding | Consequence |
| --- | --- |
| `build_corpus` is missing | Airflow indexing fails when it reaches the rebuild import. |
| RAG service loads its index only at startup | Missing indexes prevent startup; an externally rebuilt index requires a service restart to be reloaded. |
| RAG image is referenced by name, without a Compose build section | Build it with the supplied Dockerfile before using Compose. |
| The standalone builder imports `langchain_community` and `langchain_text_splitters`, absent from RAG requirements | Its dependency set must be resolved before indexing. |
| Embedding model and LLM identifiers are fixed in source | The exact models must be available; an offline deployment must provision/cache them beforehand. |
| GPU runtime is required by Compose; Dockerfile starts from `python:3.11-slim` | CUDA/PyTorch and NVIDIA compatibility have not been established by this import; `faiss-cpu` remains CPU retrieval. |
| `QUEEN_BASE_DIR` is set in Compose, but the core derives its root from its file location | Inspect actual paths when moving the package; the core does not read this variable. |
| Connection observation patches Python sockets after loading the embedder | It neither prevents egress nor records all bootstrap activity; it does not establish air-gap compliance. |
| Retrieval does not filter FAISS sentinel IDs | Ensure corpus size is at least top-k; smaller indexes need a retrieval fix. |
| Query text and drafts are persisted to local logs | Apply appropriate access and retention controls for the deployment's corpus. |
| Airflow uses SQLite, SequentialExecutor, and one webserver/scheduler container | The supplied stack is a development topology. |
| CORS is permissive and there is no included issuer, TLS, or rate limiting | These boundaries must be designed for any shared deployment. |
| Only replica-count YAML is supplied for autoscaling | No working autoscaler, evaluator, frontend, or separate auth service is included. |

These findings describe the supplied implementation. They are not new experimental results.
