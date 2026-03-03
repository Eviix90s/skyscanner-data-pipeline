FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY apiskyscanner_api.py .

# Crear directorios para datos persistentes
RUN mkdir -p /app/credentials /app/data /app/logs

# Variables de entorno por defecto (Docker paths)
ENV GOOGLE_KEYFILE=/app/credentials/service-account.json
ENV SS_ENTITY_CACHE=/app/data/entity_cache.json
ENV SS_LOCKFILE=/app/data/.script.lock
ENV SS_LOG_FILE=/app/logs/skyscanner_api.log

CMD ["python", "-u", "apiskyscanner_api.py"]
