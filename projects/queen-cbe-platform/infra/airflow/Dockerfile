FROM apache/airflow:2.10.2-python3.11

USER airflow

# Install CPU-only PyTorch first (no CUDA, no NVIDIA packages)
RUN pip install --no-cache-dir \
    --index-url https://download.pytorch.org/whl/cpu \
    torch==2.3.1

# Install general Python dependencies for Airflow DAGs (no torch here)
COPY infra/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# Install sentence-transformers without pulling its dependencies (uses already installed torch/transformers/etc.)
RUN pip install --no-cache-dir sentence-transformers --no-deps


