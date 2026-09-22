FROM python:3.11-slim

# System dependencies required by psycopg2 and common build steps.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application.
COPY . .

RUN pip install --no-cache-dir -e .

# Default command runs the pipeline; override in docker-compose for other roles.
CMD ["python", "-m", "flows.etl_flow", "--pipeline", "retail_aggregation"]
