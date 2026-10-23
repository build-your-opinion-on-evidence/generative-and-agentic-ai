FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev curl && rm -rf /var/lib/apt/lists/*
COPY shared/ /app/shared/
COPY services/approval-service/requirements.txt /app/requirements.txt
ENV PIP_DEFAULT_TIMEOUT=120
RUN pip install --no-cache-dir --retries 5 -r /app/requirements.txt
COPY services/approval-service/ /app/
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8021
CMD ["python", "main.py"]
