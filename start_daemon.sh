#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# 优先使用 systemd 服务托管（服务器系统级 7x24 守护，彻底脱离 AGY/SSH 会话）
if command -v systemctl >/dev/null 2>&1 && [ -f /etc/systemd/system/eth_trading.service ]; then
    echo "正在通过 systemd 服务启动 eth_trading..."
    sudo systemctl restart eth_trading.service
    sudo systemctl enable eth_trading.service >/dev/null 2>&1 || true
    sleep 2
    if systemctl is-active --quiet eth_trading.service; then
        echo "[启动成功] eth_trading.service 已在服务器后台守护运行"
        echo "Web 监控地址: http://124.156.193.157:8787"
        echo "查看实时运行日志: tail -f \"$DIR/auto_trading.log\""
        echo "停止服务: \"$DIR/stop.sh\" 或 sudo systemctl stop eth_trading.service"
        exit 0
    fi
fi

# 备用方案：setsid nohup 脱机启动
PY_BIN="/home/ubuntu/venv/bin/python3"
if [ ! -f "$PY_BIN" ]; then
    PY_BIN="python3"
fi

PID=$(pgrep -f "python.*auto_trading.py" | head -n 1 || true)
if [ -n "$PID" ]; then
    echo "[提示] auto_trading.py 已经在运行中 (PID: $PID)"
    echo "Web 监控地址: http://124.156.193.157:8787"
    echo "查看日志: tail -f \"$DIR/auto_trading.log\""
    exit 0
fi

setsid nohup "$PY_BIN" auto_trading.py --resume "$@" < /dev/null >> "$DIR/auto_trading.log" 2>&1 &
sleep 2
NEW_PID=$(pgrep -f "python.*auto_trading.py" | head -n 1 || true)
echo "[启动成功] 服务已在后台脱机运行 (PID: $NEW_PID)"
echo "Web 监控地址: http://124.156.193.157:8787"
echo "查看实时运行日志: tail -f \"$DIR/auto_trading.log\""
echo "停止后台服务: \"$DIR/stop.sh\""
