#!/usr/bin/env bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

PY_BIN="/home/ubuntu/venv/bin/python"
if [ ! -f "$PY_BIN" ]; then
    PY_BIN="python3"
fi

echo "========================================================"
echo "  ETH 双策略自动化模拟交易系统 v2.0/v3.0 (Linux Server)"
echo "  Web 监控大屏: http://0.0.0.0:8787"
echo "  请在本地电脑浏览器访问: http://<服务器公网IP>:8787"
echo "========================================================"
echo ""

exec "$PY_BIN" auto_trading.py --resume "$@"
