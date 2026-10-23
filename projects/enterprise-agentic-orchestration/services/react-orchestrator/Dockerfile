FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY shared/ /app/shared/
COPY services/react-orchestrator/requirements.txt /app/requirements.txt
ENV PIP_DEFAULT_TIMEOUT=120
RUN pip install --no-cache-dir --retries 5 -r /app/requirements.txt
COPY services/react-orchestrator/ /app/
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8005
CMD ["python", "main.py"]
