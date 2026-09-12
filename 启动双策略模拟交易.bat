@echo off
chcp 65001 >nul
title ETH 双策略自动化模拟交易 v2.0 (量仓增强)
echo ========================================================
echo   ETH 5m 双策略自动化模拟交易系统 v2.0 (量仓增强版)
echo   策略一: VWAP #1#2 中心值均值回归 (+空 / -多) (量仓门禁)
echo   策略二: 清算地图突破 (量仓出清确认 / 动能保盈)
echo   官方量仓: 实时持仓 OI + 5m OI Delta + Taker买卖比 + 大户多空比
echo   复盘日志: 每小时自动归因并写入 TRADING_REVIEW_LOG.md
echo   Web 监控大屏: http://127.0.0.1:8787
echo ========================================================
echo.
py auto_trading.py --resume
pause
