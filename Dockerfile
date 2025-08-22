# syntax=docker/dockerfile:1.7
FROM python:3.13-slim

# Avoid .pyc files & ensure unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Basic OS deps + tini for signal handling (clean shutdown)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates tini && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better build cache)
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt


# Copy the app code
COPY . .

# Default port (Render/Railway will override with $PORT)
ENV PORT=5050

# Entrypoint for proper signal handling
ENTRYPOINT ["/usr/bin/tini","--"]

# Start the API (your api_server.py reads PORT env)
CMD ["python", "api_server.py"]
