#!/bin/sh

# 스크립트가 있는 디렉토리 기준으로 이동
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

TAG=creaddiscans/codeflowhub:base-gpu
docker build --platform linux/amd64 -t $TAG .
docker push $TAG