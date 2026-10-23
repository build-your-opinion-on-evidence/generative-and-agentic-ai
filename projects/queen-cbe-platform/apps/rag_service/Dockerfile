FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*

# Install rag_service dependencies (includes faiss-cpu, sentence-transformers, ollama, etc.)
COPY apps/rag_service/requirements-rag.txt ./requirements-rag.txt
RUN pip install --no-cache-dir -r requirements-rag.txt

# Copy full project code into the container
COPY . .

# Make queen_core package importable by adding it to PYTHONPATH
# /app/core/queen_core contains the 'queen_core' package directory.
ENV PYTHONPATH="/app/core/queen_core:${PYTHONPATH}"

ENV RAG_SERVICE_PORT=8002
EXPOSE 8002

CMD ["uvicorn", "apps.rag_service.main:app", "--host", "0.0.0.0", "--port", "8002"]


