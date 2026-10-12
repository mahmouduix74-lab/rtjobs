# Small browser-free image for the personal web UI (no Chrome, no scraper deps).
FROM python:3.13-slim
WORKDIR /app
COPY web/requirements.txt web/requirements.txt
RUN pip install --no-cache-dir -r web/requirements.txt
COPY web web
USER 1000:1000
ENV PYTHONUNBUFFERED=1 DATA_DIR=/data
EXPOSE 8000
CMD ["uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
