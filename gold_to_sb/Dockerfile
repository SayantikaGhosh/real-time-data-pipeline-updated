# Lightweight Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary pyarrow

# Copy script (data will be mounted at runtime)
COPY gold_to_sb.py .

# Set entrypoint
ENTRYPOINT ["python", "gold_to_sb.py"]
