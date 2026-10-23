FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY shared/ /app/shared/
COPY prompts/ /app/prompts/
COPY services/fraud-scoring-agent/requirements.txt /app/requirements.txt
ENV PIP_DEFAULT_TIMEOUT=120
RUN pip install --no-cache-dir --retries 5 -r /app/requirements.txt
COPY services/fraud-scoring-agent/ /app/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8013

CMD ["python", "main.py"]
