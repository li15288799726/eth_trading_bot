#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

if command -v systemctl >/dev/null 2>&1 && [ -f /etc/systemd/system/eth_trading.service ]; then
    echo "正在停止 systemd eth_trading.service 服务..."
    sudo systemctl stop eth_trading.service >/dev/null 2>&1 || true
fi

PIDS=$(pgrep -f "python.*auto_trading.py" || true)
if [ -n "$PIDS" ]; then
    echo "[停止] 正在终止 auto_trading.py 进程: $PIDS"
    kill $PIDS 2>/dev/null || true
    sleep 1
    STILL_ALIVE=$(pgrep -f "python.*auto_trading.py" || true)
    if [ -n "$STILL_ALIVE" ]; then
        kill -9 $STILL_ALIVE 2>/dev/null || true
    fi
    echo "[完成] 已成功停止进程"
else
    echo "[完成] 没有发现正在运行的 auto_trading.py 进程"
fi
