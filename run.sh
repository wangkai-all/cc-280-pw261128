#!/usr/bin/env bash
# 启动类型推断与入仓工作台（纯标准库，无需安装依赖）
set -euo pipefail
cd "$(dirname "$0")"
DB="${1:-data/workbench.db}"
PORT="${2:-8000}"
mkdir -p data
exec python3 -m src.app "$DB" "$PORT"
