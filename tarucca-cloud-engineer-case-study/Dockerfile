# Tarucca Data Processor - Docker Container

# Use a small Python image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements first (better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project files
COPY src/ ./src/
COPY data/ ./data/

# Make logs show up immediately
ENV PYTHONUNBUFFERED=1

# Default command to run the processor
CMD ["python", "-m", "src.processor"]

