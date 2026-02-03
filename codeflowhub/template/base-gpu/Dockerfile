FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        libsndfile1 ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install boto3 requests
RUN pip install torch==2.9.1 torchvision==0.24.1 torchaudio==2.9.1 torchcodec==0.9.1

RUN pip install -U codeflowhub