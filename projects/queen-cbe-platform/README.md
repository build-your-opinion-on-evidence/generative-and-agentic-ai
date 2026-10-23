# Queen CBE Platform

**Project ID: QUEEN-001 · Status: imported prototype; end-to-end deployment not rerun.**

A local RAG prototype for Arabic questions over Egyptian Central Bank regulations. It combines PDF ingestion, FAISS retrieval, a structured analysis draft, and a final answer generated through Ollama. It is part of [Build Your Opinion on Evidence](https://github.com/build-your-opinion-on-evidence/evidence-hub).

## Research question

Can a retrieval → analysis → final-answer workflow produce answers supported by the supplied documents while identifying questions those documents do not cover?

The code implements that workflow. Answer quality, latency, resource consumption, and improvements over a single-generation RAG baseline have **not been measured during this import**. There is no paper reproduction claim or regulatory approval claim.

## Implemented scope

| Component | Implementation |
| --- | --- |
| Gateway | NGINX proxies `/health` and `/rag/query` to the RAG service. |
| API and authorization | FastAPI validates HS256 JWTs and requires `rag:query`. Debug fields also require `rag:debug:view`. |
| Retrieval | SentenceTransformer embeddings and normalized FAISS inner-product search; the API uses top-k 5. |
| Generation | A fixed sequence of retrieval, analysis draft, and final synthesis; two Ollama LLM calls per successful query path. |
| Ingestion | Airflow PDF scanning and metadata recording; the downstream rebuild task currently has an unresolved implementation dependency. |
| Diagnostics | Local query logs, socket connection observations, and best-effort `nvidia-smi` snapshots. |

The retrieval and analysis “agents” are Python functions within one service. This snapshot uses a fixed execution sequence; its trace labels do not implement a dynamic ReAct tool-selection loop. Several empty service directories in the archive were scaffolding and are not included as implemented services.

See [architecture and known blockers](docs/architecture.md) and [import provenance](IMPORT.md).

## Repository layout

| Path | Purpose |
| --- | --- |
| `apps/rag_service/` | FastAPI service, JWT checks, requirements, and Dockerfile |
| `core/queen_core/` | Retrieval, prompting, generation, manual CLI, and PDF index builder |
| `infra/nginx/` | Gateway configuration |
| `infra/airflow/` | Container build and the Compose-mounted DAG |
| `mlops/data_pipelines/airflow_dags/` | Older alternative DAG retained for provenance; not mounted by Compose |
| `autoscaling/policies.yaml` | Replica settings only; no autoscaling controller is included |
| `docker-compose.yml` | Original four-service topology with credentials supplied externally |

Source PDFs, extracted text, metadata, indexes, model weights, tokens, and execution logs are not distributed. Runtime files belong under ignored `shared/` paths.

## Prepare a local environment

Work from this project directory after cloning the subject repository:

```bash
cd projects/queen-cbe-platform
cp .env.example .env
python -c 'import secrets; print(secrets.token_urlsafe(48))'
```

Set `AUTH_JWT_SECRET` and an independently generated `AIRFLOW_ADMIN_PASSWORD` in `.env`. The public API copy requires a nonempty JWT secret; Compose also requires both settings. Existing private deployment values are not included in this snapshot.

The archived Compose configuration expects an NVIDIA-enabled Docker host and uses `QUEEN_EMBED_DEVICE=cuda` for the RAG container. It references a locally built `queen-cbe-rag-service:latest` image:

```bash
docker build -f apps/rag_service/Dockerfile -t queen-cbe-rag-service:latest .
```

This command is the source-defined build entry point, **not a verified clean-install recipe**. Resolve the blockers in [the architecture notes](docs/architecture.md#known-blockers-and-limitations) before expecting a complete deployment. In particular:

1. Make the embedding model `Ahmedhisham/queen_of_embedded_egy_20k` and Ollama model `qwen2.5:1.5b-instruct-q4_0` available in the target environment. Model downloads are separate from the code import.
2. Supply an authorized PDF corpus under `shared/data/raw/` and rebuild the index locally. The standalone builder is `python -m queen_core.build_faiss_index_from_pdfs` with `core/queen_core` on `PYTHONPATH`; its LangChain/PDF dependencies are not declared in the supplied RAG requirements.
3. The API loads `shared/index/faiss_index.bin` and `shared/index/faiss_meta.pkl` at startup. It cannot start successfully before these exist.
4. The Airflow DAG calls `queen_core.queen.build_corpus`, which is absent in the supplied source. Starting Airflow does not repair that missing function.

Only use locally built, trusted pickle metadata. The runtime reads the metadata with Python pickle.

## Request contract

Once the deployment blockers are resolved, a token needs a subject and `permissions: ["rag:query"]`, signed using the deployment's JWT secret. This archive does not include a login/token-issuing service. Use a short-lived token generated by your own local development process or authorized issuer.

```bash
curl http://localhost:8080/health
curl http://localhost:8080/rag/query \
  -H "Authorization: Bearer ${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"query":"ما الذي توضحه المستندات المتاحة؟","debug":false}'
```

The response contains `answer`. Optional debug fields include passages, the generated analysis draft, GPU snapshots, network observations, and a local log path; these require both `debug: true` and `rag:debug:view` permission.

## Evidence and next experiment

Static source and configuration checks were performed for the import; details are in [IMPORT.md](IMPORT.md). Containers, model inference, indexing, and answer accuracy were not executed in the import environment.

After restoring a working deployment, compare this two-generation workflow against the same retrieval followed by one generation. Hold the corpus, embedding model, top-k, LLM, prompts, and evaluation questions fixed where the comparison permits. Record grounded-answer accuracy, source support, abstention on uncovered questions, latency, failures, and resource use. Publish measured results and a supporting commit before drawing a comparative conclusion.

## License

The source package metadata declares `Proprietary`. This import preserves that declaration and does not add an open-source license grant. Corpus and model licenses must be assessed separately.
