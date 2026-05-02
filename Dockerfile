# Use the official lightweight Python 3.11 image as the base
FROM python:3.12-slim

# Set working directory
WORKDIR /etl_pipeline_relationaldb

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create a non-root user for GKE Autopilot security
RUN useradd -m etluser
USER etluser

# Copy application code
COPY . .

# Default command (can be overridden by K8s 'command' field)
CMD ["python", "-m", "routes.run_cron_job"]