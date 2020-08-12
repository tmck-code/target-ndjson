FROM python:3.8-slim

WORKDIR /home/python

ADD requirements.txt .

RUN BUILD_DEPS="gcc build-essential" \
    && apt-get update \
    && apt-get install -y --no-install-recommends git ${BUILD_DEPS} \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove ${BUILD_DEPS} \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /deps
RUN git clone https://github.com/singer-io/singer-tools.git

WORKDIR /home/python

ADD . .
