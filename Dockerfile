FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Basic runtime tools often needed by crawlers (TLS/debug/network checks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy full source first so Hatch can resolve scrapy/VERSION.
COPY . /app

RUN pip install --no-cache-dir --upgrade pip setuptools wheel build \
    && pip install --no-cache-dir .

# Default command can be overridden in Coolify.
# This keeps the container alive for worker-style usage.
CMD ["bash", "-lc", "sleep infinity"]
