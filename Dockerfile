FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libreoffice-nogui \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# Stamped by CI with the exact commit that produced this image, and surfaced on
# /health. Without it there is no way to tell which build is actually running,
# because ":latest" moves under you.
ARG APP_VERSION=dev
ENV APP_VERSION=${APP_VERSION}

# --proxy-headers + --forwarded-allow-ips: the container is only reachable
# through Caddy on an internal network, so X-Forwarded-For is trustworthy here.
# Without this, every client IP in the auth log is just the proxy's address.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--proxy-headers", "--forwarded-allow-ips", "*"]
