FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and install package
COPY src/setup.py .
COPY src/scraper_manager ./scraper_manager
RUN pip install --no-cache-dir .

# Non-root user for security
RUN useradd --create-home --shell /bin/bash app
USER app

# Health check port
EXPOSE 8080

ENTRYPOINT ["python", "-m", "scraper_manager"]
