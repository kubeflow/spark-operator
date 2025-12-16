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

# --- OpenShift Arbitrary UID Compatibility ---
# IMPORTANT: Temporarily switch to root (UID 0) to change permissions
USER 0

# Set Spark/App directories to be owned by group 0 and group-writable (g=u)
RUN chgrp -R 0 /opt/spark \
    && chmod -R g=u /opt/spark \
    && mkdir -p /opt/spark/work-dir /opt/spark/logs /tmp \
    && chgrp -R 0 /opt/spark/work-dir /opt/spark/logs /tmp \
    && chmod -R g=u /opt/spark/work-dir /opt/spark/logs /tmp \
    && mkdir -p /app/input /app/output \
    && chgrp -R 0 /app \
    && chmod -R g=u /app

# Copy ONLY requirements first (for better caching)
COPY requirements-docker.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements-docker.txt

# Set Python path
ENV PYTHONPATH="/app:$PYTHONPATH"

# Copy application code LAST
COPY --chown=spark:0 scripts/ /app/scripts/
COPY --chown=spark:0 assets/ /app/assets/