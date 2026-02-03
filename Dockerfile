FROM harbor.dell.com/devops-images/debian-12/python-3.12:latest as base

FROM base as build

USER root

WORKDIR /app

COPY requirements.txt .

RUN apt-get -y update && apt-get -y upgrade && \
    apt-get install -y \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -f /Python-3.*/Lib/test/*.pem \
    && rm -f /usr/bin/vault

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

COPY src /app/src
COPY main.py /app

# Create necessary directories and set permissions
RUN mkdir -p /data/pvc /app/OutputDirectory \
    && chmod 755 /data/pvc \
    && chmod 777 /app/OutputDirectory \
    && chown -R 1001 /data/pvc

COPY entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh

# Create non-root user and set permissions
RUN adduser -u 5678 --disabled-password --gecos "" appuser \
    && chown -R appuser /app

USER appuser

ENV PYTHONPATH=/app

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]