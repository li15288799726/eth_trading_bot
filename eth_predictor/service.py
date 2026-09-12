# -*- coding: utf-8 -*-
"""
ETH 预测与持续自优化中枢服务 (service.py)
=======================================
作为统一协调层：
1. 消费行情与量仓微观数据源 (BinanceFuturesFeed + CoinGlass 清算地图)
2. 实时生成未来 5m / 1h / 1d 预测看板与目标价格
3. 后台自动化监控预测流水生命周期 (到期比对行情判定准确率)
4. 调度 12 小时自主意愿复盘与参数自优化循环 (达到 90% 自动锁定停止)
5. 向上提供 REST API 结构与实时快照
"""
import asyncio
import json
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from eth_predictor.models import ETHPredictor
from eth_predictor.optimizer import PredictionOptimizer
from eth_predictor.storage import PredictionStorage
from eth_predictor.indicators import safe_float
from eth_predictor.macro_events import MacroEventManager

TZ_BJT = timezone(timedelta(hours=8))


from eth_predictor.lifecycle import PredictionLifecycleManager


class PredictorService:
    def __init__(self, feed, liq_path=None):
        self.feed = feed
        self.liq_path = Path(liq_path) if liq_path else Path("data/auto/liq_latest.json")
        self.storage = PredictionStorage()
        self.optimizer = PredictionOptimizer(self.storage)
        self.predictor = self.optimizer.predictor
        self.macro_manager = MacroEventManager()
        self.macro_events = {}
        self._last_macro_fetch = 0.0
        self._macro_fetching = False

        # 目标驱动状态机生命周期管理器 (场景1/2/3 阶段追踪)
        self.lifecycle = PredictionLifecycleManager(self.predictor, self.storage, self.optimizer)
        self.latest_predictions = {}
        self._running = False
        self._liq_cache = {}

    def load_liq_data(self):
        """读取最新清算地图数据"""
        try:
            if self.liq_path.exists():
                data = json.loads(self.liq_path.read_text(encoding="utf-8"))
                if data:
                    self._liq_cache = data
                return data
        except Exception:
            pass
        return self._liq_cache

    def _fetch_macro_background(self):
        try:
            res = self.macro_manager.evaluate_composite_events()
            if res and isinstance(res, dict):
                self.macro_events = res
        except Exception:
            pass
        finally:
            self._macro_fetching = False

    def update_tick(self):
        """主行情循环每 tick 调用：驱动阶段性目标追踪状态机并检测到期验证"""
        now = time.time()
        price = self.feed.price
        if not price or price <= 0:
            return

        # 0. 轮询宏观突发事件、以太坊基金会与 ETF 净流向 (每 60 秒在后台线程异步拉取，绝不阻塞主交易事件循环)
        if (now - self._last_macro_fetch >= 60.0 or not self.macro_events) and not getattr(self, "_macro_fetching", False):
            self._macro_fetching = True
            self._last_macro_fetch = now
            threading.Thread(target=self._fetch_macro_background, daemon=True).start()

        # 1. 组装盘口数据快照并由生命周期状态机驱动 (触及 TP1、TP2、SL 或超时)
        liq_data = self.load_liq_data()
        rt_liq = getattr(self.feed, "get_realtime_liquidation_stats", lambda: {})()
        btc_lead = getattr(self.feed, "get_btc_lead_lag_stats", lambda: {})()
        snapshot = {
            "price": price,
            "klines_5m": getattr(self.feed, "klines", []),
            "klines_1h": getattr(self.feed, "klines_1h", []),
            "klines_1d": getattr(self.feed, "klines_1d", []),
            "oi_current": getattr(self.feed, "oi_current", 0.0),
            "oi_delta_5m": getattr(self.feed, "oi_delta_5m", 0.0),
            "oi_delta_1h": getattr(self.feed, "oi_delta_1h", 0.0),
            "oi_delta_1d": getattr(self.feed, "oi_delta_1d", 0.0),
            "vol_ratio": getattr(self.feed, "vol_ratio", 1.0),
            "buy_sell_ratio_5m": getattr(self.feed, "buy_sell_ratio_5m", 1.0),
            "top_ls_ratio": getattr(self.feed, "top_ls_ratio", 1.0),
            "global_ls_ratio": getattr(self.feed, "global_ls_ratio", 1.0),
            "funding_rate": getattr(self.feed, "funding_rate", 0.0001),
            "liq_raw_data": liq_data,
            "macro_events": self.macro_events,
            "realtime_liquidations": rt_liq,
            "btc_lead_lag": btc_lead,
        }

        # 核心：实时状态机检验（追踪 TP1 -> 二次推算 -> 冲刺 TP2 / 止损 / 偏离核验）
        self.lifecycle.update_ticks(price, getattr(self.feed, "klines", []), snapshot)
        self.latest_predictions = self.lifecycle.get_display_state(price)

        # 2. 定周期检测 12 小时实盘自动复盘日志 (每 60 秒轻量核查一次)
        if now - getattr(self, "_last_review_check", 0) >= 60.0:
            self._last_review_check = now
            all_history = self.storage.load_history(limit=1000)
            self.optimizer.check_and_run_12h_review(all_history)

    def trigger_review(self, reason="Web 用户手动触发 12H 实盘复盘分析"):
        """手动触发 12 小时实盘复盘分析与报告生成"""
        all_history = self.storage.load_history(limit=1000)
        return self.optimizer.check_and_run_12h_review(all_history, force=True, reason=reason)

    def reset_all_history(self):
        """重置历史预测流水与活跃追踪器，重新以现价开启全新的生命周期"""
        self.storage.reset_history()

        # 重置指标状态
        opt_state = self.optimizer.state
        opt_state["current_accuracy"] = 0.0
        opt_state["accuracy_5m"] = 0.0
        opt_state["accuracy_1h"] = 0.0
        opt_state["accuracy_1d"] = 0.0
        opt_state["status"] = "MONITORING"
        self.storage.save_optimizer_state(opt_state)

        # 清除旧的活跃预测文件
        if self.lifecycle.active_file.exists():
            try:
                self.lifecycle.active_file.unlink()
            except Exception:
                pass
        self.lifecycle.active_predictions = {}

        # 立即以当前现价重新初始化新目标
        price = self.feed.price or 2500.0
        liq_data = self.load_liq_data()
        snapshot = {
            "price": price,
            "klines_5m": getattr(self.feed, "klines", []),
            "klines_1h": getattr(self.feed, "klines_1h", []),
            "klines_1d": getattr(self.feed, "klines_1d", []),
            "oi_current": getattr(self.feed, "oi_current", 0.0),
            "oi_delta_5m": getattr(self.feed, "oi_delta_5m", 0.0),
            "oi_delta_1h": getattr(self.feed, "oi_delta_1h", 0.0),
            "buy_sell_ratio_5m": getattr(self.feed, "buy_sell_ratio_5m", 1.0),
            "top_ls_ratio": getattr(self.feed, "top_ls_ratio", 1.0),
            "global_ls_ratio": getattr(self.feed, "global_ls_ratio", 1.0),
            "funding_rate": getattr(self.feed, "funding_rate", 0.0001),
            "liq_raw_data": liq_data,
            "macro_events": self.macro_events,
        }
        self.lifecycle.ensure_active_predictions(snapshot)
        self.latest_predictions = self.lifecycle.get_display_state(price)
        print("[*] 历史预测已完全重置，新阶段追踪器已全部启动！", flush=True)
        return {"ok": True, "message": "历史预测已成功重置，新生命周期目标已重新就绪！"}

    def snapshot(self):
        """生成前端看板所需的完整预测数据包"""
        opt_state = self.optimizer.state

        # 获取最近 30 条历史预测与验证结果
        recent_history = self.storage.load_history(limit=30)
        # 逆序排序，最新在最前
        recent_history_reversed = list(reversed(recent_history))

        # 计算当前各项胜率指标
        metrics = self.optimizer.compute_accuracy_metrics(self.storage.load_history(limit=1000))

        # 清算地图核心档位摘要
        liq_data = self.load_liq_data()
        from eth_predictor.indicators import calc_liquidation_gravity
        liq_summary = calc_liquidation_gravity(self.feed.price, liq_data)

        return {
            "latest_predictions": self.latest_predictions,
            "optimizer": {
                "status": opt_state.get("status", "MONITORING"),
                "is_converged": False,
                "current_accuracy": metrics.get("overall_accuracy", 0.0),
                "accuracy_5m": metrics.get("5m", {}).get("accuracy", 0.0),
                "accuracy_1h": metrics.get("1h", {}).get("accuracy", 0.0),
                "accuracy_1d": metrics.get("1d", {}).get("accuracy", 0.0),
                "total_verified": metrics.get("total", 0),
                "total_predictions": metrics.get("total_predictions", 0),
            },
            "metrics": metrics,
            "liquidation_gravity": liq_summary,
            "realtime_liquidations": getattr(self.feed, "get_realtime_liquidation_stats", lambda: {})(),
            "btc_lead_lag": getattr(self.feed, "get_btc_lead_lag_stats", lambda: {})(),
            "recent_predictions": recent_history_reversed[:20],
            "macro_events": self.macro_events,
        }
