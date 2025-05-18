#!/bin/bash

# NiFi MCP Server 시작 스크립트
cd "$(dirname "$0")"

# 가상 환경 활성화 (가상 환경을 사용하는 경우)
# source venv/bin/activate

# 서버 시작 - 서버만 실행 (UI 없이)
python run.py --server-only --server-host 0.0.0.0 --server-port 8000 --log-level info 