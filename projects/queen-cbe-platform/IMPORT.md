# Import provenance — QUEEN-001

- Import date: 2026-10-23.
- Source: user-supplied `queen-cbe-platform.zip`.
- Archive SHA-256: `ee71aaf66970281771b0866fa18746bd051a1faca778bebb80453e8055f4449c`.
- Destination: `generative-and-agentic-ai/projects/queen-cbe-platform`.
- Source history: no Git history or upstream commit was supplied; this is an archive snapshot.

## Included source

Sixteen nonempty source/configuration files were selected from the archive: the RAG application, Queen core, its standalone builder and manual CLI, two Airflow DAG variants, two Dockerfiles, requirements, Compose, NGINX, and the replica-policy stub. Existing paths were retained so the Docker build context and package layout remain recognizable.

Thirteen of the sixteen source files remain byte-for-byte identical to the archive. Three were changed for publication:

| File | Change |
| --- | --- |
| `docker-compose.yml` | Removed the embedded JWT secret and default Airflow password, require environment-supplied values, replace the administrator email with `admin@example.invalid`, and use an explicit shell argument list to pass the password through the container environment. |
| `apps/rag_service/main.py` | Require a nonempty `AUTH_JWT_SECRET` instead of accepting a shared fallback value. |
| `core/queen_core/pyproject.toml` | Replace internal-looking placeholder project URLs with this repository's public URLs; retain the original proprietary license declaration. |

Added project documentation and ignore/environment templates are separate from the archived implementation.

## Excluded material

- Original PDF documents and all extracted metadata, including text samples.
- FAISS binaries and pickle metadata, which contain corpus-derived information.
- Airflow/service/query logs and cached Python bytecode.
- Root command scratchpads, GPU/admin instructions, token-generation snippets, and stored bearer tokens.
- Empty service directories and empty root README/environment files.

The original archive was left unchanged. Secret values were not reproduced in the public documentation. Ignore rules prevent runtime data, populated environment files, logs, and common key files from being committed or copied into Docker builds by default.

## Validation scope

Import validation covers Python syntax, YAML/TOML parsing, an explicit file allowlist, comparisons with the source archive, credential-pattern scans, and local documentation links. The missing Airflow `build_corpus` function and other deployment limitations are documented in [the architecture review](docs/architecture.md).

Docker, Airflow, FAISS, the embedding model, and Ollama inference were not run in this import environment. No accuracy, latency, GPU, or offline-compliance result is claimed.
