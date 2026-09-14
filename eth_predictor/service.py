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
from eth_predictor.indicators import safe_float, calc_daily_vwap, calc_weekly_vwap
from eth_predictor.macro_events import MacroEventManager
from eth_predictor.asof import (
    filter_closed_klines, resolve_liq_snapshot, load_liq_history_from_data_dir, now_ms,
)

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
        self._liq_history_cache = []
        self._liq_history_loaded_at = 0.0

    def load_liq_data(self, as_of_ms=None):
        """读取清算地图；as-of-T 仅用 fetched_ts <= T，否则禁用 (AUDIT_FIX_ASOF_001)。"""
        latest = None
        try:
            if self.liq_path.exists():
                data = json.loads(self.liq_path.read_text(encoding="utf-8"))
                if data:
                    if "fetched_ts" not in data:
                        try:
                            data["fetched_ts"] = int(self.liq_path.stat().st_mtime * 1000)
                            data["_asof_mtime_fallback"] = True
                        except Exception:
                            pass
                    self._liq_cache = data
                    latest = data
        except Exception:
            latest = None
        if latest is None:
            latest = self._liq_cache

        # Refresh decrypted history index occasionally for replay/as-of
        now = time.time()
        if now - self._liq_history_loaded_at > 120.0 or not self._liq_history_cache:
            try:
                data_dir = Path(self.liq_path).resolve().parent.parent  # data/
                self._liq_history_cache = load_liq_history_from_data_dir(data_dir, limit=200)
                self._liq_history_loaded_at = now
            except Exception:
                pass

        return resolve_liq_snapshot(
            latest,
            as_of_ms=as_of_ms,
            history=self._liq_history_cache,
            allow_unstamped_live=True,
        )

    def _build_market_snapshot(self, as_of_ms=None):
        """Assemble prediction snapshot with closed-bar / as-of filters only."""
        t_ms = now_ms(as_of_ms)
        feed = self.feed
        price = getattr(feed, "price", None)

        get_closed = getattr(feed, "get_closed_klines", None)
        if callable(get_closed):
            kl_5m = get_closed("5m", as_of_ms=t_ms)
            kl_15m = get_closed("15m", as_of_ms=t_ms)
            kl_1h = get_closed("1h", as_of_ms=t_ms)
            kl_1d = get_closed("1d", as_of_ms=t_ms)
        else:
            kl_5m = filter_closed_klines(getattr(feed, "klines", []), as_of_ms=t_ms, interval="5m")
            kl_15m = filter_closed_klines(getattr(feed, "klines_15m", []), as_of_ms=t_ms, interval="15m")
            kl_1h = filter_closed_klines(getattr(feed, "klines_1h", []), as_of_ms=t_ms, interval="1h")
            kl_1d = filter_closed_klines(getattr(feed, "klines_1d", []), as_of_ms=t_ms, interval="1d")

        # Raw (may include forming bar) — lifecycle TP/SL tracking only
        kl_5m_raw = list(getattr(feed, "klines", []) or [])
        kl_15m_raw = list(getattr(feed, "klines_15m", []) or [])
        kl_1h_raw = list(getattr(feed, "klines_1h", []) or [])
        kl_1d_raw = list(getattr(feed, "klines_1d", []) or [])

        micro_fn = getattr(feed, "get_microstructure_asof", None)
        if callable(micro_fn):
            micro = micro_fn(as_of_ms=t_ms)
        else:
            micro = {
                "oi_current": getattr(feed, "oi_current", 0.0),
                "oi_delta_5m": getattr(feed, "oi_delta_5m", 0.0),
                "oi_delta_1h": getattr(feed, "oi_delta_1h", 0.0),
                "oi_delta_1d": getattr(feed, "oi_delta_1d", 0.0),
                "vol_ratio": getattr(feed, "vol_ratio", 1.0),
                "buy_sell_ratio_5m": getattr(feed, "buy_sell_ratio_5m", 1.0),
                "top_ls_ratio": getattr(feed, "top_ls_ratio", 1.0),
                "global_ls_ratio": getattr(feed, "global_ls_ratio", 1.0),
                "funding_rate": getattr(feed, "funding_rate", 0.0001),
            }

        vwap_fn = getattr(feed, "calc_daily_vwap", None)
        if callable(vwap_fn):
            try:
                vwap_daily = vwap_fn(as_of_ms=t_ms)
            except TypeError:
                vwap_daily = vwap_fn()
        else:
            vwap_daily = calc_daily_vwap(kl_5m, as_of_ms=t_ms, price=price)

        weekly_fn = getattr(feed, "calc_weekly_vwap", None)
        if callable(weekly_fn):
            try:
                vwap_weekly = weekly_fn(as_of_ms=t_ms)
            except TypeError:
                vwap_weekly = weekly_fn()
        else:
            vwap_weekly = calc_weekly_vwap(kl_1h, as_of_ms=t_ms, price=price)

        liq_data = self.load_liq_data(as_of_ms=t_ms)
        rt_liq = getattr(feed, "get_realtime_liquidation_stats", lambda: {})()
        wall_ms = int(time.time() * 1000)
        is_historical = as_of_ms is not None and abs(t_ms - wall_ms) > 5000
        # Realtime forceOrder stream: filter events with time <= as_of
        if isinstance(rt_liq, dict) and is_historical:
            rt_liq = {}

        # AUDIT_FIX_ASOF_001: BTC lead-lag must honor as_of_ms (closed bars / T-time prices)
        btc_fn = getattr(feed, "get_btc_lead_lag_stats", None)
        if callable(btc_fn):
            try:
                btc_lead = btc_fn(as_of_ms=t_ms)
            except TypeError:
                btc_lead = btc_fn()
        else:
            btc_lead = {}

        # Historical replay: forbid wall-clock spot — use last closed 5m close as T-time price
        if is_historical and kl_5m:
            price = safe_float(kl_5m[-1][4], price or 0.0)

        # AUDIT_FIX_ASOF_001: macro RSS must be as-of T (filter published_ts<=T or disable)
        if is_historical:
            try:
                macro_events = self.macro_manager.evaluate_composite_events(as_of_ms=t_ms)
            except TypeError:
                macro_events = {
                    "composite_event_score": 0.0,
                    "volatility_multiplier": 1.0,
                    "macro_disabled": True,
                    "macro_disabled_reason": "macro as-of unsupported; disabled for replay",
                    "as_of_ms": t_ms,
                    "recent_news": [],
                    "event_tags": [],
                    "summary": "macro disabled for historical as_of",
                }
        else:
            macro_events = self.macro_events

        return {
            "price": price,
            "as_of_ms": t_ms,
            "klines_5m": kl_5m,
            "klines_15m": kl_15m,
            "klines_1h": kl_1h,
            "klines_1d": kl_1d,
            "klines_5m_raw": kl_5m_raw,
            "klines_15m_raw": kl_15m_raw,
            "klines_1h_raw": kl_1h_raw,
            "klines_1d_raw": kl_1d_raw,
            "oi_current": micro.get("oi_current", 0.0),
            "oi_delta_5m": micro.get("oi_delta_5m", 0.0),
            "oi_delta_1h": micro.get("oi_delta_1h", 0.0),
            "oi_delta_1d": micro.get("oi_delta_1d", 0.0),
            "vol_ratio": micro.get("vol_ratio", getattr(feed, "vol_ratio", 1.0)),
            "buy_sell_ratio_5m": micro.get("buy_sell_ratio_5m", 1.0),
            "top_ls_ratio": micro.get("top_ls_ratio", 1.0),
            "global_ls_ratio": micro.get("global_ls_ratio", 1.0),
            "funding_rate": micro.get("funding_rate", 0.0001),
            "vwap_daily": vwap_daily,
            "vwap_weekly": vwap_weekly,
            "liq_raw_data": liq_data,
            "macro_events": macro_events,
            "realtime_liquidations": rt_liq,
            "btc_lead_lag": btc_lead,
        }

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

        # 1. 组装盘口数据快照 (closed-bar / as-of-T) 并由生命周期状态机驱动
        snapshot = self._build_market_snapshot()

        # 核心：实时状态机检验（追踪 TP1 -> 二次推算 -> 冲刺 TP2 / 止损 / 偏离核验）
        # Pass raw klines for intrabar TP/SL extreme tracking (outcome, not features).
        self.lifecycle.update_ticks(price, snapshot.get("klines_5m_raw") or getattr(self.feed, "klines", []), snapshot)
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
        snapshot = self._build_market_snapshot()
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

        # 清算地图核心档位摘要 (as-of now; disabled if no valid snapshot)
        liq_data = self.load_liq_data()
        from eth_predictor.indicators import calc_liquidation_gravity
        liq_summary = calc_liquidation_gravity(self.feed.price, liq_data if not liq_data.get("_liq_disabled") else {})

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
