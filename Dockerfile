FROM python:3.12-slim

# Install ffmpeg and curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependencies and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Run with Gunicorn on dynamic Railway port
CMD gunicorn app:app --bind 0.0.0.0:${PORT:-5000} --workers 4 --threads 4 --timeout 120
