#docker build -t tab_scraper .
#docker run --network=tab-scraper_backend --name="tab_scraper" tab_scraper

FROM python:3.12.6-slim-bookworm

# Python
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

# Poetry
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR='/var/cache/pypoetry' \
    POETRY_HOME='/usr/local' \
    POETRY_VERSION=1.8.3

ENV WORKDIR=/app/

RUN apt-get update && apt-get -y install cron
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /app

COPY pyproject.toml poetry.lock /app/
RUN touch README.md

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

COPY *.py /app/
COPY .env /app/

# may not be needed
RUN poetry install --without dev

# Add the cron job
COPY tab_scraper_crontab /etc/cron.d/tab_scraper_crontab
RUN chmod 644 /etc/cron.d/tab_scraper_crontab

#Install Cron
RUN apt-get update
RUN apt-get -y install cron
# Run the command on container startup

ENTRYPOINT cron -f