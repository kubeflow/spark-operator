# Dockerfile for Docling + PySpark

# Use Official apache/spark image (already has PySpark + Java)
FROM apache/spark-py:latest

# Metadata
LABEL maintainer="roburishabh@outlook.com"
LABEL version="1.0"
LABEL description="Docling + PySpark for distributed PDF processing"

# Set the working directory
WORKDIR /app

# Install System dependencies (Linux)
USER root 
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    libgomp1 \
    libglib2.0-0 \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# Copy ONLY requirements first (for better caching)
COPY requirements-docker.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements-docker.txt

# Set Python path
ENV PYTHONPATH="/app:$PYTHONPATH"
ENV HOME=/home/spark

RUN mkdir -p /app/input /app/output /home/spark && \
    chmod 777 /app/input /app/output && \
    chmod -R 777 /opt/spark/work-dir && \
    if ! id -u spark > /dev/null 2>&1; then \
        useradd -m -u 185 -s /bin/bash spark; \
    fi && \
    chown -R spark:0 /home/spark /app && \
    chmod -R 777 /home/spark /app

# Copy application code LAST
COPY --chown=spark:0 scripts/ /app/scripts/
COPY --chown=spark:0 assets/ /app/assets/

# Switch to non-root user
USER spark