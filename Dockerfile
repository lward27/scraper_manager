FROM python:3.11-slim
USER root
WORKDIR /app

COPY requirements.txt .
COPY src/setup.py .
COPY src/scraper_manager ./scraper_manager

RUN pip install -r requirements.txt --user
RUN pip install . --user

ENTRYPOINT [ "python", "-m", "scraper_manager" ]
