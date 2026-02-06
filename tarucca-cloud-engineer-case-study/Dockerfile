# Tarucca Data Processor - Docker Container
# 
# TODO: Complete this Dockerfile to containerize the processor
#
# Requirements:
# 1. Use Python 3.11-slim as base image for small size
# 2. Set working directory to /app
# 3. Copy and install requirements.txt
# 4. Copy source code
# 5. Set environment variable PYTHONUNBUFFERED=1 (for proper logging)
# 6. Define entrypoint to run the processor
#
# Docker best practices:
# - Keep image small (use slim base, multi-stage if needed)
# - Don't copy unnecessary files (use .dockerignore)
# - Run as non-root user for security (optional but good practice)
# - Use layer caching effectively (copy requirements before code)

FROM python:3.11-slim

# TODO: Your Dockerfile implementation here

# Example structure:
# WORKDIR /app
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
# COPY src/ ./src/
# COPY data/ ./data/
# ENV PYTHONUNBUFFERED=1
# ...

CMD ["python", "-m", "src.processor"]
