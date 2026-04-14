FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY mv_scraper ./mv_scraper

ENTRYPOINT ["python", "-m"]
CMD ["mv_scraper.web", "--host", "0.0.0.0", "--port", "7860"]
