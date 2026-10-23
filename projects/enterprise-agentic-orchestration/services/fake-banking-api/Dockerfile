FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy shared library
COPY shared/ /app/shared/

# Copy service requirements and install
COPY services/fake-banking-api/requirements.txt /app/requirements.txt
ENV PIP_DEFAULT_TIMEOUT=120
RUN pip install --no-cache-dir --retries 5 -r /app/requirements.txt

# Copy service code
COPY services/fake-banking-api/ /app/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8001

CMD ["python", "main.py"]
