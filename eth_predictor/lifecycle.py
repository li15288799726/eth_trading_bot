# -*- coding: utf-8 -*-
"""
ETH 预测系统 · 纯实时预测生命周期与时序评估管理器 (lifecycle.py)
============================================================
专注于纯粹的“实时走势预测与时间窗口映射”，彻底剥离模拟交易撮合与窄止损逻辑：
1. 纯事件驱动与解除硬性到期限制：
   - 移除原先 300s/3600s/86400s 的机械硬性到期截断，避免因行情微幅洗盘而在终盘被错误判负；
   - 走势以目标位 (TP1/TP2 达成) 或结构失效线 (SL 止损) 为最终结算驱动。
2. 关键辅助窗口验证趋势延续性 (Auxiliary Continuation Validation)：
   - 5M 预测：发单满 3 分钟执行关键核验，综合研判成交量买卖比、持仓量 OI Delta与象限、
              清算地图引力、VWAP偏离度及市场消息；若明显大幅度偏离，立即终止并退出；
              若如期发展或正常微波，则继续等待目标达成。
   - 1H 预测：以 5M 辅助窗口加入研判；
   - 1D 预测：以 1H 辅助窗口加入研判。
3. 偏离终止后等待明确开仓时机 (WAITING_SETUP)：
   - 预测提前偏离终止后，进入观望等待期，必须等待盘口量仓和指标重新出现强共振信号，
     才正式发布新一期预测，彻底杜绝震荡行情中反复开停磨损。
"""
import json
import time
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path

from eth_predictor.indicators import (
    calc_daily_vwap, calc_cvd, calc_oi_matrix, calc_liquidation_gravity, safe_float
)

TZ_BJT = timezone(timedelta(hours=8))
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "predictions"
ACTIVE_FILE = DATA_DIR / "active_predictions.json"

# 各周期周期性趋势核验与三道门配置 (5M每3分钟/最长45m，1H每15分钟/最长3h，1D每4小时/最长28h)
AUX_VALIDATION_CONFIG = {
    "5m": {
        "aux_window_sec": 180,        # 每 3 分钟 (180 秒) 周期性核验一次
        "aux_label": "3分钟",
        "sub_tf": "5m",               # 辅助评估使用的微观周期
        "label": "5 分钟",
        "max_horizon_sec": 2700,      # 三道门垂直时限: 45 分钟未完成且未偏离则落袋结算
    },
    "1h": {
        "aux_window_sec": 900,        # 每 15 分钟 (900 秒) 周期性核验一次 (彻底避免 5M 噪点误杀)
        "aux_label": "15分钟",
        "sub_tf": "15m",
        "label": "1 小时",
        "max_horizon_sec": 10800,     # 三道门垂直时限: 3 小时
    },
    "1d": {
        "aux_window_sec": 14400,      # 每 4 小时 (14400 秒) 周期性核验一次 (4H宏观平滑，消除小时级回踩误杀)
        "aux_label": "4小时",
        "sub_tf": "4h",
        "label": "24 小时 (1天)",
        "max_horizon_sec": 100800,    # 三道门垂直时限: 28 小时
    }
}
TIMEOUT_CONFIG = AUX_VALIDATION_CONFIG  # 保持向下兼容


class PredictionLifecycleManager:
    def __init__(self, predictor, storage, optimizer):
        self.predictor = predictor
        self.storage = storage
        self.optimizer = optimizer
        self.active_file = ACTIVE_FILE
        self.active_predictions = self._load_active()
        self._cleanup_stranded_predictions()

    def _load_active(self):
        """读取正在活跃运行中的多周期预测目标"""
        try:
            if self.active_file.exists():
                return json.loads(self.active_file.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[!] 读取 active_predictions.json 异常: {e}", flush=True)
        return {}

    def _save_active(self):
        """保存当前活跃目标状态"""
        try:
            def _json_default(obj):
                if hasattr(obj, "item"):
                    return obj.item()
                if hasattr(obj, "__float__"):
                    return float(obj)
                if hasattr(obj, "__int__"):
                    return int(obj)
                return str(obj)
            self.active_file.write_text(json.dumps(self.active_predictions, indent=2, ensure_ascii=False, default=_json_default), encoding="utf-8")
        except Exception as e:
            print(f"[!] 保存 active_predictions.json 异常: {e}", flush=True)

    def _cleanup_stranded_predictions(self):
        """清理历史库中因系统重启或旧代码顶替遗留的僵尸悬空单"""
        try:
            history = self.storage.load_history(limit=0)
            if not history:
                return
            active_ids = {v["pred_id"] for v in self.active_predictions.values() if v and isinstance(v, dict) and v.get("pred_id")}
            updated = False
            now_iso = datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")

            for p in history:
                if p.get("status") == "ACTIVE" and p.get("pred_id") not in active_ids:
                    # 历史遗留孤儿单，自动修复归档
                    base_px = p.get("base_price", 0.0)
                    p["status"] = "VERIFIED"
                    p["verified_result"] = {
                        "verified_at": now_iso,
                        "actual_close": base_px,
                        "max_price": p.get("highest_seen", base_px),
                        "min_price": p.get("lowest_seen", base_px),
                        "direction_correct": False,
                        "tp1_hit": p.get("tp1_status") == "REACHED",
                        "tp2_hit": p.get("tp2_status") == "REACHED",
                        "sl_hit": False,
                        "outcome": "ARCHIVED_CLEANUP",
                        "accuracy_score": 0.5 if p.get("tp1_status") == "REACHED" else 0.0,
                        "exit_reason": "系统初始化自动平滑归档历史悬空记录",
                        "pnl_pct": 0.0,
                        "mfe_pct": 0.0,
                        "mae_pct": 0.0
                    }
                    updated = True

            if updated:
                self.storage.save_history(history)
                print("[Lifecycle] 🧹 已成功整理历史库中的悬空记录", flush=True)

            # 顺带校准现有活跃预测的周期性趋势核验属性，确保热更新无缝承接
            act_updated = False
            for tf, act in self.active_predictions.items():
                if act and isinstance(act, dict):
                    cfg = AUX_VALIDATION_CONFIG.get(tf, {})
                    aux_sec = cfg.get("aux_window_sec", 180)
                    aux_lbl = cfg.get("aux_label", "3分钟")

                    if "aux_window_sec" not in act:
                        act["aux_window_sec"] = aux_sec
                        act["aux_label"] = aux_lbl
                        act_updated = True

                    if "next_trend_check_ts" not in act or "trend_check_count" not in act:
                        created_ts = act.get("created_ts", int(time.time()))
                        elapsed = max(0, int(time.time()) - created_ts)
                        rounds = max(0, elapsed // aux_sec)
                        act["trend_check_count"] = rounds
                        act["last_trend_check_ts"] = created_ts + rounds * aux_sec
                        act["next_trend_check_ts"] = created_ts + (rounds + 1) * aux_sec
                        act["next_trend_check_iso"] = datetime.fromtimestamp(act["next_trend_check_ts"], TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")
                        act_updated = True

                    # 移除遗留的硬超时限制
                    if act.get("timeout_ts", 0) > 0:
                        act["timeout_ts"] = 0
                        act_updated = True

            if act_updated:
                self._save_active()
        except Exception as e:
            print(f"[Lifecycle] 清理悬空记录异常: {e}", flush=True)

    def _build_waiting_setup_state(self, tf, price, raw_pred=None, reason="等待盘口量仓和指标出现强共振开仓时机"):
        """构建观望等待开仓状态对象 (WAITING_SETUP)，保障前端渲染与系统稳态"""
        now_ts = int(time.time())
        now_iso = datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")
        cfg = AUX_VALIDATION_CONFIG.get(tf, {})
        label = cfg.get("label", tf)
        aux_label = cfg.get("aux_label", "辅助")
        return {
            "status": "WAITING_SETUP",
            "timeframe": tf,
            "direction": "NEUTRAL",
            "dir_label": "震荡观望",
            "dir_icon": "⏸️",
            "wait_since_ts": now_ts,
            "wait_since_iso": now_iso,
            "cooldown_until_ts": 0,
            "wait_reason": reason,
            "timeout_remaining_fmt": "⏳ 观望待机中",
            "stage": "WAITING_SETUP",
            "stage_label": f"⏸️ [{label}] 处于窄幅震荡区间，暂无明确单边动能，保持观望中...",
            "base_price": price,
            "highest_seen": price,
            "lowest_seen": price,
            "tp1": 0.0,
            "tp2": 0.0,
            "sl": 0.0,
            "target_range": "--",
            "dist_tp1_u": 0.0,
            "dist_tp2_u": 0.0,
            "dist_sl_u": 0.0,
            "pct_to_tp1": 0.0,
            "pct_to_tp2": 0.0,
            "composite_score": 0.0,
            "confidence": 0.0,
            "attribution_tags": ["窄幅震荡观望", "观望防磨损"],
            "attribution_detail": f"[{label}] 盘口处于窄幅震荡整理中，量仓暂无单边强共振，系统坚决保持观望防磨损，待趋势出现时第一时间启动预测。",
            "aux_window_sec": cfg.get("aux_window_sec", 180),
            "aux_label": aux_label,
            "aux_checked": False,
            "aux_status": "WAITING",
            "scenario_tree": raw_pred.get("scenario_tree") if raw_pred else None,
            "market_regime": raw_pred.get("market_regime") if raw_pred else None,
            "btc_lead_lag": raw_pred.get("btc_lead_lag") if raw_pred else None,
            "realtime_liquidations": raw_pred.get("realtime_liquidations") if raw_pred else None,
        }

    def _is_setup_ready(self, tf, raw_pred, market_snapshot, act=None):
        """
        判断当前盘口量仓和多周期指标是否形成清晰、强共振的开仓时机：
        - 杜绝震荡死区无脑开仓；
        - 确保多空信号强度、置信度达标，且微观量仓不逆势。
        """
        if not raw_pred:
            return False

        score = safe_float(raw_pred.get("composite_score", 0.0))
        direction = raw_pred.get("direction", "NEUTRAL")
        confidence = safe_float(raw_pred.get("confidence", 0.0))

        # 1. 信号强度与明确多空倾向 (过滤微弱无序底噪，窄幅震荡坚决不开单)
        thresh_map = {"5m": 0.16, "1h": 0.15, "1d": 0.15}
        req_thresh = thresh_map.get(tf, 0.15)
        if abs(score) < req_thresh or direction not in ("UP", "DOWN"):
            return False

        # 2. 置信度门槛
        if confidence < 65.0:
            return False

        # 3. 冷却期判定 (若刚从偏离终止或止损退出来，强制要求冷却)
        if act and isinstance(act, dict):
            now_ts = int(time.time())
            cooldown_until = act.get("cooldown_until_ts", 0)
            if now_ts < cooldown_until:
                return False

        # 4. 微观订单流形态过滤 (严禁在多头踩踏时开多，或空头逼空时开空，但允许在见底恐慌抛盘反弹时接多、见顶冲高衰竭时接空)
        oi_info = raw_pred.get("oi_info") or {}
        regime = oi_info.get("regime", "CONSOLIDATION")
        dir_lbl_str = str(raw_pred.get("dir_label", ""))
        is_bottom_reversal = ("底部" in dir_lbl_str or "见底" in dir_lbl_str)
        is_top_reversal = ("顶部" in dir_lbl_str or "见顶" in dir_lbl_str)

        if direction == "UP" and regime in ("BEAR_ATTACK", "LONG_FLUSH") and not is_bottom_reversal:
            return False
        if direction == "DOWN" and regime in ("BULL_ATTACK", "SHORT_SQUEEZE") and not is_top_reversal:
            return False

        # 5. 层级共振对齐 (Hierarchical Alignment): 杜绝小周期与大周期打架
        if tf == "5m":
            act_1h = self.active_predictions.get("1h")
            dir_1h = act_1h.get("direction") if (act_1h and act_1h.get("status") == "ACTIVE") else None
            if dir_1h == "UP" and direction == "DOWN":
                # 1H 处于顺势看多时，5M 严禁顺手开空，除非触发顶部衰竭反转
                if score > -0.32 and not is_top_reversal:
                    return False
            elif dir_1h == "DOWN" and direction == "UP":
                # 1H 处于顺势看空时，5M 严禁顺手开多，除非触发底部恐慌见底反转
                if score < 0.32 and not is_bottom_reversal:
                    return False

        # 6. 空间门槛硬核校验 (自适应周内活跃日大波段 vs 周末休息日小波段)
        if direction in ("UP", "DOWN"):
            is_wk = raw_pred.get("is_weekend")
            if is_wk is None:
                as_of_ms = market_snapshot.get("as_of_ms")
                if as_of_ms:
                    dt = datetime.fromtimestamp(as_of_ms / 1000.0, tz=TZ_BJT)
                else:
                    dt = datetime.now(TZ_BJT)
                is_wk = (dt.weekday() in (5, 6))

            default_space = (3.2 if is_wk else 6.0) if tf == "5m" else ((10.0 if is_wk else 20.0) if tf == "1h" else 60.0)
            req_space = safe_float(raw_pred.get("min_space_req", default_space))
            base_p = safe_float(raw_pred.get("base_price", 0.0))
            tp1_p = safe_float(raw_pred.get("tp1", 0.0))
            tp2_p = safe_float(raw_pred.get("tp2", 0.0))
            max_space = max(abs(tp1_p - base_p), abs(tp2_p - base_p)) if base_p > 0 else 0.0
            if max_space < (req_space - 0.05):
                return False

        # 7. 宏观对冲门禁 (防止开单 3 分钟即被宏观利空/利好核验偏离秒杀)
        macro_events = market_snapshot.get("macro_events") or {}
        s_macro = safe_float(macro_events.get("composite_event_score", 0.0))
        if direction == "UP" and s_macro <= -0.25:
            return False
        if direction == "DOWN" and s_macro >= 0.25:
            return False

        return True

    def ensure_active_predictions(self, market_snapshot):
        """若某个时间框架无活跃预测或处于等待开仓观望状态，则在盘口出现明确共振信号时开启新预测"""
        price = market_snapshot.get("price", 0.0)
        if not price or price <= 0:
            return

        now_ts = int(time.time())
        all_preds = None

        for tf in ["5m", "1h", "1d"]:
            act = self.active_predictions.get(tf)
            # 如果当前预测正在有效运行中，继续保持
            if act and act.get("status") == "ACTIVE":
                continue

            # 检查是否满足冷却时间要求
            if act and act.get("status") == "WAITING_SETUP":
                cooldown_until = act.get("cooldown_until_ts", 0)
                if now_ts < cooldown_until:
                    # 尚在冷却观望中，更新当前参考价
                    act["base_price"] = price
                    continue

            # 惰性推算最新多周期预测信号
            if all_preds is None:
                all_preds = self.predictor.predict(market_snapshot)

            raw_pred = all_preds.get(tf)
            if not raw_pred:
                continue

            # 检验是否具备明确的量仓与指标强共振开仓条件
            if self._is_setup_ready(tf, raw_pred, market_snapshot, act):
                self._initialize_new_prediction(tf, raw_pred, price)
            elif not act or act.get("status") != "WAITING_SETUP":
                # 未达开仓条件且尚无等待结构，进入观望等待状态
                self.active_predictions[tf] = self._build_waiting_setup_state(tf, price, raw_pred)
            else:
                # 观望等待中，保持基准价与情景树动态同频
                act["base_price"] = price
                if raw_pred.get("scenario_tree"):
                    act["scenario_tree"] = raw_pred.get("scenario_tree")
                if raw_pred.get("market_regime"):
                    act["market_regime"] = raw_pred.get("market_regime")

        self._save_active()

    def _initialize_new_prediction(self, tf, raw_pred, current_price):
        """初始化一个新周期的实时预测任务 (删除硬止损，引入周期性趋势核验)"""
        now_ts = int(time.time())
        now_iso = datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")
        cfg = AUX_VALIDATION_CONFIG.get(tf, {})
        aux_window_sec = cfg.get("aux_window_sec", 180)
        aux_label = cfg.get("aux_label", "3分钟")
        max_horizon_sec = cfg.get("max_horizon_sec", 3600)

        active_obj = dict(raw_pred)
        active_obj.update({
            "stage": "STAGE_TP1",           # STAGE_TP1 | STAGE_TP2 | COMPLETED
            "stage_step": 1,                # 1: 追踪TP1, 2: 冲刺TP2, 3: 完成
            "stage_label": f"⏳ 正在追踪目标 1 ({raw_pred.get('tp1', 0.0):.2f})",
            "tp1_status": "PENDING",        # PENDING | REACHED
            "tp2_status": "PENDING",        # PENDING | REACHED
            "sl_breached": False,           # 记录是否曾突破失效线（仅作统计展示）
            "created_ts": now_ts,
            "created_iso": now_iso,
            "aux_window_sec": aux_window_sec,
            "aux_label": aux_label,
            "max_horizon_sec": max_horizon_sec,
            "expiry_timeout_ts": now_ts + max_horizon_sec,
            "expiry_timeout_iso": datetime.fromtimestamp(now_ts + max_horizon_sec, TZ_BJT).strftime("%Y-%m-%d %H:%M:%S"),
            "trend_check_count": 0,         # 趋势核验累计通过轮次
            "last_trend_check_ts": now_ts,
            "next_trend_check_ts": now_ts + aux_window_sec,
            "next_trend_check_iso": datetime.fromtimestamp(now_ts + aux_window_sec, TZ_BJT).strftime("%Y-%m-%d %H:%M:%S"),
            "aux_check_ts": now_ts + aux_window_sec,
            "aux_check_iso": datetime.fromtimestamp(now_ts + aux_window_sec, TZ_BJT).strftime("%Y-%m-%d %H:%M:%S"),
            "aux_checked": False,
            "aux_status": "PENDING",        # PENDING | PASSED | DEVIATED
            "aux_verified_at": None,
            "aux_deviation_detail": None,
            "base_price": current_price,
            "highest_seen": current_price,
            "lowest_seen": current_price,
            "secondary_eval": None,
            "exit_info": None,
            "status": "ACTIVE",
        })
        self.active_predictions[tf] = active_obj

        # 录入持久化历史数据库
        self.storage.record_prediction(dict(active_obj))
        print(f"[Lifecycle] 🚀 [{tf}] 发布新预测 (周期核验={aux_label}): 方向={active_obj.get('direction')} | 基准={current_price:.2f} | 目标带={active_obj.get('target_range')} | 参考防守={active_obj.get('sl')}", flush=True)

    def update_ticks(self, current_price, current_klines_5m, market_snapshot):
        """
        核心时序预测检验状态机：
        1. 纯事件驱动：以目标位 (TP1/TP2 达成) 或结构失效线 (SL) 为终极结算驱动，无机械到期时间截断；
        2. 辅助验证窗口：在发单满 3 分钟 (5m) / 5 分钟 (1h) / 1 小时 (1d) 时核验量仓、CVD、VWAP与消息延续性；
           若大幅偏离则立即终止并观望；若延续良好则继续保持运行；
        3. 终止后进入 WAITING_SETUP 观望，等待明确共振时机再开仓。
        """
        now_ts = int(time.time())
        now_iso = datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")
        self.ensure_active_predictions(market_snapshot)

        for tf in ["5m", "1h", "1d"]:
            act = self.active_predictions.get(tf)
            if not act or act.get("status") != "ACTIVE":
                continue

            direction = act.get("direction", "UP")
            base_px = act.get("base_price", current_price)
            tp1 = act.get("tp1", current_price)
            tp2 = act.get("tp2", current_price)
            sl = act.get("sl", current_price)
            created_ts_sec = act.get("created_ts", now_ts)

            # -------------------------------------------------------------
            # 1. 动态融合当前价格与有效 K 线极值 (杜绝历史插针倒流污染)
            # -------------------------------------------------------------
            candle_high = current_price
            candle_low = current_price

            # Prefer raw (incl. forming) bars for outcome extremes; features stay closed-only in predict().
            klines_5m = market_snapshot.get("klines_5m_raw") or market_snapshot.get("klines_5m") or current_klines_5m or []
            if klines_5m:
                for k in reversed(klines_5m[-48:]):
                    k_open_sec = float(k[0]) / 1000.0
                    if k_open_sec >= created_ts_sec:
                        candle_high = max(candle_high, float(k[2]))
                        candle_low = min(candle_low, float(k[3]))

            if tf in ("1h", "1d"):
                klines_1h = market_snapshot.get("klines_1h_raw") or market_snapshot.get("klines_1h") or []
                if klines_1h:
                    for k in reversed(klines_1h[-48:]):
                        k_open_sec = float(k[0]) / 1000.0
                        if k_open_sec >= created_ts_sec:
                            candle_high = max(candle_high, float(k[2]))
                            candle_low = min(candle_low, float(k[3]))

            if tf == "1d":
                klines_1d = market_snapshot.get("klines_1d_raw") or market_snapshot.get("klines_1d") or []
                if klines_1d:
                    for k in reversed(klines_1d[-14:]):
                        k_open_sec = float(k[0]) / 1000.0
                        if k_open_sec >= created_ts_sec:
                            candle_high = max(candle_high, float(k[2]))
                            candle_low = min(candle_low, float(k[3]))

            act["highest_seen"] = max(act.get("highest_seen", current_price), current_price, candle_high)
            act["lowest_seen"] = min(act.get("lowest_seen", current_price), current_price, candle_low)

            check_high = act["highest_seen"]
            check_low = act["lowest_seen"]

            # -------------------------------------------------------------
            # 2. 目标达成情况持续跟踪 (TP1 & TP2)
            # -------------------------------------------------------------
            is_tp1_hit = (check_high >= tp1) if direction == "UP" else (check_low <= tp1 if direction == "DOWN" else False)
            if is_tp1_hit and act.get("tp1_status") != "REACHED":
                act["tp1_status"] = "REACHED"
                act["tp1_hit_ts"] = now_ts
                act["tp1_hit_price"] = check_high if direction == "UP" else check_low
                act["stage"] = "STAGE_TP2"
                act["stage_step"] = 2
                # 移动止损至开仓保本线 (Breakeven Trailing Stop)，杜绝打满 TP1 赢钱变亏钱
                act["sl"] = base_px
                act["trailing_be"] = True

                # 到达 TP1 时立即以现价执行二次推算，研判动能延续性
                sec_eval = self._evaluate_continuation(tf, direction, current_price, market_snapshot)
                act["secondary_eval"] = sec_eval
                if sec_eval.get("should_continue"):
                    act["stage_label"] = f"🎯 目标 1 ({tp1:.2f}) 已达成！动能延续，冲刺目标 2 ({tp2:.2f})"
                else:
                    act["stage_label"] = f"🎯 目标 1 ({tp1:.2f}) 已达成！动能转弱，锁定利润保护"
                print(f"[Lifecycle] 🎯 [{tf}] 预测目标 1 达成！现价={current_price:.2f}, 极值={act['tp1_hit_price']:.2f}, TP1={tp1:.2f} | 二次推算: {sec_eval.get('reason')}", flush=True)

            is_tp2_hit = (check_high >= tp2) if direction == "UP" else (check_low <= tp2 if direction == "DOWN" else False)
            if is_tp2_hit and act.get("tp2_status") != "REACHED":
                act["tp2_status"] = "REACHED"
                act["tp2_hit_ts"] = now_ts
                act["tp2_hit_price"] = check_high if direction == "UP" else check_low
                act["stage_label"] = f"🎉 目标 1 ({tp1:.2f}) 与目标 2 ({tp2:.2f}) 双达标！"
                print(f"[Lifecycle] 🎉 [{tf}] 预测第二目标 2 双达标！现价={current_price:.2f}, 极值={act['tp2_hit_price']:.2f}, TP2={tp2:.2f}", flush=True)

            # -------------------------------------------------------------
            # 3. 结构失效线跟踪 (已移除硬止损，仅保留数据统计记录)
            # -------------------------------------------------------------
            is_sl_hit = (check_low <= sl) if direction == "UP" else (check_high >= sl if direction == "DOWN" else False)
            if is_sl_hit and not act.get("sl_breached"):
                act["sl_breached"] = True

            # -------------------------------------------------------------
            # 4. 纯事件驱动结算与周期性趋势动态核验
            # -------------------------------------------------------------
            # 事件 A: 双目标均圆满打满 -> 提前锁定全周期大胜
            if act.get("tp1_status") == "REACHED" and act.get("tp2_status") == "REACHED":
                self._finalize_prediction(
                    tf=tf,
                    act=act,
                    current_price=current_price,
                    outcome="FULL_WIN",
                    is_win=True,
                    tp1_hit=True,
                    tp2_hit=True,
                    sl_hit=act.get("sl_breached", False),
                    accuracy_score=1.0,
                    reason=f"🎉 目标 1 ({tp1:.2f}) 与目标 2 ({tp2:.2f}) 双达标，全周期大胜！",
                    market_snapshot=market_snapshot
                )
                continue

            # 事件 A2: 保本止盈保护触发 (打满目标 1 后，价格回踩至开仓保本线，锁定胜局退出，严禁由赢转亏)
            if act.get("tp1_status") == "REACHED" and act.get("trailing_be"):
                is_be_hit = (current_price <= act["sl"]) if direction == "UP" else (current_price >= act["sl"])
                if is_be_hit:
                    self._finalize_prediction(
                        tf=tf,
                        act=act,
                        current_price=current_price,
                        outcome="TP1_WIN",
                        is_win=True,
                        tp1_hit=True,
                        tp2_hit=False,
                        sl_hit=False,
                        accuracy_score=0.85,
                        reason=f"🎯 目标 1 ({tp1:.2f}) 达成后价格回踩保本线 ({act['sl']:.2f})，保本保护触发，锁定利润胜局！",
                        market_snapshot=market_snapshot
                    )
                    continue

            # (已完全移除原硬止损，普通回撤继续保持观测；严重偏离交由三道门与周期性核验驱动)

            # 事件 B: 三道门法则之垂直时间界 (Max Horizon Expiry)
            max_horizon_sec = act.get("max_horizon_sec")
            if not max_horizon_sec:
                max_horizon_sec = AUX_VALIDATION_CONFIG.get(tf, {}).get("max_horizon_sec", 3600)
                act["max_horizon_sec"] = max_horizon_sec

            if (now_ts - created_ts_sec) >= max_horizon_sec:
                had_tp1 = (act.get("tp1_status") == "REACHED")
                pnl_now = round(((current_price - base_px) / base_px * 100.0), 3) if direction == "UP" else round(((base_px - current_price) / base_px * 100.0), 3)
                if had_tp1:
                    outcome = "TP1_WIN"
                    is_win = True
                    acc_score = 0.85
                    reason_text = f"⏱️ 达到最大观测时间边界 ({max_horizon_sec // 60}分钟)，已打满目标 1，顺势锁定收官"
                elif pnl_now >= 0.05:
                    outcome = "DIR_WIN"
                    is_win = True
                    acc_score = 0.70
                    reason_text = f"⏱️ 达到最大观测时间边界 ({max_horizon_sec // 60}分钟)，价格顺向位移({pnl_now:+.2f}%)，到期方向正确"
                else:
                    outcome = "TIMEOUT_FAILED"
                    is_win = False
                    acc_score = 0.0
                    reason_text = f"⏱️ 达到最大观测时间边界 ({max_horizon_sec // 60}分钟)，未能在时效内打出动能({pnl_now:+.2f}%)，超时失效"

                print(f"[Lifecycle] ⏱️ [{tf}] {reason_text}", flush=True)
                self._finalize_prediction(
                    tf=tf,
                    act=act,
                    current_price=current_price,
                    outcome=outcome,
                    is_win=is_win,
                    tp1_hit=had_tp1,
                    tp2_hit=False,
                    sl_hit=act.get("sl_breached", False),
                    accuracy_score=acc_score,
                    reason=reason_text,
                    market_snapshot=market_snapshot
                )
                continue

            # 事件 C: 周期性趋势核验 (5M 每3分钟, 1H 每15分钟, 1D 每4小时)
            aux_sec = act.get("aux_window_sec", 180)
            next_chk_ts = act.get("next_trend_check_ts")
            if not next_chk_ts:
                next_chk_ts = created_ts_sec + aux_sec
                act["next_trend_check_ts"] = next_chk_ts

            if now_ts >= next_chk_ts:
                chk_count = act.get("trend_check_count", 0) + 1
                res = self._check_auxiliary_continuation(
                    tf=tf,
                    act=act,
                    current_price=current_price,
                    market_snapshot=market_snapshot
                )
                if len(res) == 6:
                    is_deviated, dev_score, dev_reason, is_extreme_reversal, reverse_dir, price_loss = res
                else:
                    is_deviated, dev_score, dev_reason = res[:3]
                    is_extreme_reversal, reverse_dir, price_loss = False, None, 0.0

                if is_extreme_reversal and reverse_dir:
                    # 8.19 极值反转机制：逆向超幅大单边直接反向看多/空
                    reason_text = f"⚡ 盘口遭遇极端反向单边冲击(-{price_loss:.1f}U)，触发8.19极值反转机制，直接反向看{'多' if reverse_dir == 'UP' else '空'}: {dev_reason}"
                    print(f"[Lifecycle] ⚡ [{tf}] {reason_text}", flush=True)
                    self._finalize_prediction(
                        tf=tf,
                        act=act,
                        current_price=current_price,
                        outcome="REVERSAL_FLIP",
                        is_win=False,
                        tp1_hit=(act.get("tp1_status") == "REACHED"),
                        tp2_hit=False,
                        sl_hit=True,
                        accuracy_score=0.0,
                        reason=reason_text,
                        market_snapshot=market_snapshot
                    )
                    # 立即无缝反向开启相反方向预测，捕捉大单边行情
                    forced_pred = self.predictor.build_forced_prediction(tf, reverse_dir, current_price, market_snapshot)
                    self._initialize_new_prediction(tf, forced_pred, current_price)
                    self._save_active()
                    continue

                elif is_deviated:
                    # 周期核验判定趋势严重偏离！立即终止预测，进入观望等待状态
                    had_tp1 = (act.get("tp1_status") == "REACHED")
                    dir_val = act.get("direction", "UP")
                    base_px = act.get("base_price", current_price)
                    pnl_now = round(((current_price - base_px) / base_px * 100.0), 3) if dir_val == "UP" else round(((base_px - current_price) / base_px * 100.0), 3)
                    is_favorable = (pnl_now >= 0.08)

                    if had_tp1:
                        outcome = "TP1_WIN"
                        is_win = True
                        acc_score = 0.85
                    elif is_favorable:
                        outcome = "DEV_PROFIT_EXIT"
                        is_win = True
                        acc_score = 0.65
                    else:
                        outcome = "DEVIATION_STOP"
                        is_win = False
                        acc_score = 0.0

                    reason_text = (
                        f"🎯 目标 1 已达成，第 {chk_count} 次趋势核验判定动能偏离锁利离场: {dev_reason}"
                        if had_tp1 else (
                            f"🛡️ 顺向浮盈({pnl_now:+.2f}%)，第 {chk_count} 次趋势核验判定动能减弱保利离场: {dev_reason}"
                            if is_favorable else
                            f"⚠️ 第 {chk_count} 次趋势核验判定严重偏离 (偏离度={dev_score:.2f} >= 0.55)，及时风控离场: {dev_reason}"
                        )
                    )
                    print(f"[Lifecycle] ⚠️ [{tf}] 周期性趋势核验触发偏离终止！{reason_text}", flush=True)
                    self._finalize_prediction(
                        tf=tf,
                        act=act,
                        current_price=current_price,
                        outcome=outcome,
                        is_win=is_win,
                        tp1_hit=had_tp1,
                        tp2_hit=False,
                        sl_hit=act.get("sl_breached", False),
                        accuracy_score=acc_score,
                        reason=reason_text,
                        market_snapshot=market_snapshot
                    )
                    continue
                else:
                    # 周期核验通过，趋势在就继续等待！
                    act["trend_check_count"] = chk_count
                    act["last_trend_check_ts"] = now_ts
                    act["next_trend_check_ts"] = now_ts + aux_sec
                    act["next_trend_check_iso"] = datetime.fromtimestamp(now_ts + aux_sec, TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")
                    act["last_trend_check_iso"] = now_iso
                    act["last_trend_dev_score"] = dev_score
                    act["aux_checked"] = True
                    act["aux_status"] = "PASSED"
                    act["aux_verified_at"] = now_iso
                    if act.get("tp1_status") == "REACHED":
                        act["stage_label"] = f"✅ 第 {chk_count} 次核验通过 · 动能良好，冲刺目标 2 ({tp2:.2f})"
                    elif price_loss > 0:
                        act["stage_label"] = f"✅ 第 {chk_count} 次核验通过 · 顺势正常回踩蓄势中(-{price_loss:.1f}U)，保持追踪目标 1 ({tp1:.2f})"
                    else:
                        act["stage_label"] = f"✅ 第 {chk_count} 次核验通过 · 动能良好延续(+{abs(price_loss):.1f}U)，追踪目标 1 ({tp1:.2f})"
                    print(f"[Lifecycle] ✅ [{tf}] 第 {chk_count} 次趋势核验通过！盘口量仓良好延续 (偏离度={dev_score:.2f} < 0.55)，继续等待！下次核验: {act['next_trend_check_iso']}", flush=True)

        self._save_active()
        # 仅每 15 秒做一次低频持久化对齐，大幅减少磁盘 I/O 磨损
        if now_ts - getattr(self, "_last_history_sync", 0) >= 15:
            self._last_history_sync = now_ts
            for tf in ["5m", "1h", "1d"]:
                act = self.active_predictions.get(tf)
                if act and act.get("status") == "ACTIVE":
                    self.storage.sync_active_prediction(act)

    def _check_auxiliary_continuation(self, tf, act, current_price, market_snapshot):
        """
        辅助验证窗口核心研判：在关键节点综合审视 6 大因子：
        1. 成交量与买卖比 / CVD 累积主动买卖偏离 (根据周期平滑底噪)
        2. 持仓量 (OI Delta) 与量价四象限形态 (1H/1D 使用1小时OI，杜绝5M微观假信号)
        3. 清算地图引力池反转与实时爆仓踩踏 (@forceOrder)
        4. 价格位移容忍度与 Daily VWAP 偏离 (顺势正常回踩不扣分，极端异动触发8.19极值反转)
        5. 突发宏观消息、基金会与 ETF 冲击
        6. 最新多周期模型综合推算得分 (震荡中性不扣分，突破反向阈值才扣分)

        返回: (is_deviated: bool, dev_score: float, reason_str: str, is_extreme_reversal: bool, reverse_dir: str, price_loss: float)
        """
        direction = act.get("direction", "UP")
        base_px = act.get("base_price", current_price)
        atr = act.get("atr", 5.0)
        aux_label = act.get("aux_label", "辅助窗口")
        reasons = []
        dev_score = 0.0

        kl_5m = market_snapshot.get("klines_5m", [])
        reverse_dir = "DOWN" if direction == "UP" else "UP"
        is_extreme_reversal = False

        # 各时间框架对应的正常回踩容忍度与极端异动阈值 (8.19单边爆发模式)
        if tf == "5m":
            normal_pullback_limit = max(5.0, atr * 1.0)
            extreme_surge_limit = max(11.0, atr * 2.2)
        elif tf == "1h":
            normal_pullback_limit = max(18.0, atr * 1.5)
            extreme_surge_limit = max(35.0, atr * 2.8)
        else: # 1d
            normal_pullback_limit = max(55.0, atr * 2.2)
            extreme_surge_limit = max(95.0, atr * 3.8)

        # -------------------------------------------------------------
        # 因子 1: 成交量与 CVD / Taker 买卖比 (针对1H/1D扩展回溯窗口并放宽微观门槛)
        # -------------------------------------------------------------
        kl_1h = market_snapshot.get("klines_1h", [])
        if tf == "1d" and kl_1h:
            # 1D 使用最近 12 根 1H K 线综合买卖比与 CVD，彻底杜绝短线微波噪点误杀
            recent_1h = kl_1h[-12:] if len(kl_1h) >= 12 else kl_1h
            tot_buy = sum(safe_float(k[9]) for k in recent_1h if len(k) > 9)
            tot_vol = sum(safe_float(k[5]) for k in recent_1h if len(k) > 5)
            tot_sell = max(0.001, tot_vol - tot_buy)
            taker_ratio = round(tot_buy / tot_sell, 3) if tot_sell > 0 else 1.0
            cvd_info = calc_cvd(kl_1h, 24)
        elif tf == "1h" and kl_5m:
            # 1H 使用最近 12 根 5M K 线 (1小时累积) 平滑买卖比
            recent_5m = kl_5m[-12:] if len(kl_5m) >= 12 else kl_5m
            tot_buy = sum(safe_float(k[9]) for k in recent_5m if len(k) > 9)
            tot_vol = sum(safe_float(k[5]) for k in recent_5m if len(k) > 5)
            tot_sell = max(0.001, tot_vol - tot_buy)
            taker_ratio = round(tot_buy / tot_sell, 3) if tot_sell > 0 else 1.0
            cvd_info = calc_cvd(kl_5m, 24)
        else:
            taker_ratio = safe_float(market_snapshot.get("buy_sell_ratio_5m", 1.0))
            cvd_info = calc_cvd(kl_5m, 12)

        cvd_delta = cvd_info.get("cvd_delta", 0.0)
        cvd_div = cvd_info.get("divergence", "NONE")
        cvd_s = cvd_info.get("score", 0.0)

        t_heavy = 0.60 if tf in ("1h", "1d") else 0.72
        t_light = 0.72 if tf in ("1h", "1d") else 0.85
        t_heavy_down = 1.65 if tf in ("1h", "1d") else 1.38
        t_light_down = 1.38 if tf in ("1h", "1d") else 1.18

        if direction == "UP":
            if taker_ratio < t_heavy:
                dev_score += 0.22
                reasons.append(f"买卖比严重失衡(买/卖={taker_ratio:.2f})")
            elif taker_ratio < t_light:
                dev_score += 0.10
                reasons.append(f"主动卖盘偏多(买/卖={taker_ratio:.2f})")

            if cvd_div == "BEARISH_DIV" or cvd_s <= -0.30:
                dev_score += 0.20
                reasons.append(f"主动卖盘持续压制(CVD={cvd_delta:.0f})")
        else: # DOWN
            if taker_ratio > t_heavy_down:
                dev_score += 0.22
                reasons.append(f"买卖比严重偏多(买/卖={taker_ratio:.2f})")
            elif taker_ratio > t_light_down:
                dev_score += 0.10
                reasons.append(f"主动买盘偏多(买/卖={taker_ratio:.2f})")

            if cvd_div == "BULLISH_DIV" or cvd_s >= 0.30:
                dev_score += 0.20
                reasons.append(f"主动买盘持续涌入(CVD=+{cvd_delta:.0f})")

        # -------------------------------------------------------------
        # 因子 2: 持仓量 (OI Delta) 与量价四象限 (对齐各周期真实尺度)
        # -------------------------------------------------------------
        oi_curr = safe_float(market_snapshot.get("oi_current", 0.0))
        oi_d5 = safe_float(market_snapshot.get("oi_delta_5m", 0.0))
        oi_d1h = safe_float(market_snapshot.get("oi_delta_1h", 0.0))
        oi_d1d = safe_float(market_snapshot.get("oi_delta_1d", 0.0))
        px_chg = current_price - base_px

        if tf == "1d":
            oi_mat = calc_oi_matrix(oi_curr, oi_d5, oi_d1h, px_chg, px_chg, tf="1d", oi_delta_1d=oi_d1d, price_change_1d=px_chg)
            chosen_doi = oi_d1d if oi_d1d != 0.0 else oi_d1h
        elif tf == "1h":
            oi_mat = calc_oi_matrix(oi_curr, oi_d5, oi_d1h, px_chg, px_chg, tf="1h")
            chosen_doi = oi_d1h
        else:
            oi_mat = calc_oi_matrix(oi_curr, oi_d5, oi_d1h, px_chg, px_chg, tf="5m")
            chosen_doi = oi_d5

        regime = oi_mat.get("regime", "CONSOLIDATION")

        if direction == "UP":
            if regime == "BEAR_ATTACK":
                dev_score += 0.28
                reasons.append(f"空头主动增仓强攻(ΔOI={chosen_doi:+.0f})")
            elif regime == "LONG_FLUSH":
                dev_score += 0.22
                reasons.append(f"多头受挫踩踏平仓(ΔOI={chosen_doi:+.0f})")
        else: # DOWN
            if regime == "BULL_ATTACK":
                dev_score += 0.28
                reasons.append(f"多头主动增仓强攻(ΔOI={chosen_doi:+.0f})")
            elif regime == "SHORT_SQUEEZE":
                dev_score += 0.22
                reasons.append(f"空头被逼空出逃(ΔOI={chosen_doi:+.0f})")

        # -------------------------------------------------------------
        # 因子 3: 清算地图引力池反转与实时爆仓踩踏 (@forceOrder)
        # -------------------------------------------------------------
        liq_raw = market_snapshot.get("liq_raw_data", {})
        liq_gravity = calc_liquidation_gravity(current_price, liq_raw)
        liq_net = liq_gravity.get("net_score", 0.0)

        if direction == "UP" and liq_net <= -0.30:
            dev_score += 0.18
            reasons.append(f"清算引力反向下移(引力分={liq_net:+.2f})")
        elif direction == "DOWN" and liq_net >= 0.30:
            dev_score += 0.18
            reasons.append(f"清算引力反向上移(引力分={liq_net:+.2f})")

        rt_liq = market_snapshot.get("realtime_liquidations") or {}
        long_liq = safe_float(rt_liq.get("long_liq_vol_eth", 0.0))
        short_liq = safe_float(rt_liq.get("short_liq_vol_eth", 0.0))
        cascade = bool(rt_liq.get("cascade_alert", False))
        net_bias = safe_float(rt_liq.get("net_bias", 0.0))

        if direction == "UP":
            if cascade and net_bias < -0.30:
                dev_score += 0.25
                reasons.append(f"多头连环强平踩踏(多爆={long_liq:.1f}ETH)")
            elif long_liq > 100.0 and net_bias < -0.50:
                dev_score += 0.15
                reasons.append(f"多头强平放量压制(多爆={long_liq:.1f}ETH vs 空爆={short_liq:.1f}ETH)")
        else: # DOWN
            if cascade and net_bias > 0.30:
                dev_score += 0.25
                reasons.append(f"空头连环强平逼空(空爆={short_liq:.1f}ETH)")
            elif short_liq > 100.0 and net_bias > 0.50:
                dev_score += 0.15
                reasons.append(f"空头强平放量轧空(空爆={short_liq:.1f}ETH vs 多爆={long_liq:.1f}ETH)")

        # -------------------------------------------------------------
        # 因子 4: 价格位移容忍度与 Daily VWAP 偏离 (顺势正常回踩不扣分，极端暴动进入8.19判定)
        # -------------------------------------------------------------
        price_loss = (base_px - current_price) if direction == "UP" else (current_price - base_px)
        vwap_data = market_snapshot.get("vwap_daily") or calc_daily_vwap(kl_5m)

        if price_loss <= 0:
            # 顺势浮盈状态下，盘口底噪偏离实施大幅折减保护
            if tf == "1d":
                dev_score = dev_score * 0.30
            elif tf == "1h":
                dev_score = dev_score * 0.50
            elif tf == "5m":
                dev_score = dev_score * 0.50
        elif price_loss <= normal_pullback_limit:
            reasons.append(f"顺势正常微幅回踩(-{price_loss:.2f}U在容忍度{normal_pullback_limit:.1f}U内)")
            # 关键风控保护：当价格仅处于大周期正常回踩区间时，对微观指标的偏离打分实施平滑折减，彻底消除因微观正常回踩被误杀的致命缺陷
            if tf == "1d":
                dev_score = dev_score * 0.40
            elif tf == "1h":
                dev_score = dev_score * 0.70
            elif tf == "5m":
                dev_score = dev_score * 0.65
        elif price_loss < extreme_surge_limit:
            dev_score += 0.25
            reasons.append(f"回踩偏深(-{price_loss:.2f}U超出正常容忍度{normal_pullback_limit:.1f}U)")
        else:
            dev_score += 0.50
            reasons.append(f"逆向暴动(-{price_loss:.2f}U达到极端阈值{extreme_surge_limit:.1f}U)")

        if vwap_data:
            vwap_val = vwap_data.get("vwap", current_price)
            slope = vwap_data.get("slope", 0.0)
            sigma = vwap_data.get("sigma", 1.0)
            if direction == "UP":
                if current_price < (vwap_val - sigma * 0.8) and slope < -0.3:
                    dev_score += 0.15
                    reasons.append("跌破日内VWAP下轨且斜率下行")
            else: # DOWN
                if current_price > (vwap_val + sigma * 0.8) and slope > 0.3:
                    dev_score += 0.15
                    reasons.append("升破日内VWAP上轨且斜率上行")

        # -------------------------------------------------------------
        # 因子 5: 突发宏观消息与事件冲击
        # -------------------------------------------------------------
        macro_events = market_snapshot.get("macro_events") or {}
        s_macro = safe_float(macro_events.get("composite_event_score", 0.0))
        if direction == "UP" and s_macro <= -0.25:
            dev_score += 0.25
            reasons.append(f"突发宏观利空冲击(评分={s_macro:+.2f})")
        elif direction == "DOWN" and s_macro >= 0.25:
            dev_score += 0.25
            reasons.append(f"突发宏观利好冲击(评分={s_macro:+.2f})")

        # -------------------------------------------------------------
        # 因子 6: 最新多周期模型综合推算得分 (震荡中性不扣分，突破反向阈值才扣分)
        # -------------------------------------------------------------
        latest_score = 0.0
        try:
            latest_preds = self.predictor.predict(market_snapshot)
            latest_pred = latest_preds.get(tf, {})
            latest_score = safe_float(latest_pred.get("composite_score", 0.0))
            latest_dir = latest_pred.get("direction", "NEUTRAL")
            req_rev_thresh = 0.16 if tf == "5m" else 0.15

            if direction == "UP" and (latest_dir == "DOWN" or latest_score <= -req_rev_thresh):
                dev_score += 0.25
                reasons.append(f"最新综合推算反向看空({latest_score:+.2f})")
            elif direction == "DOWN" and (latest_dir == "UP" or latest_score >= req_rev_thresh):
                dev_score += 0.25
                reasons.append(f"最新综合推算反向看多({latest_score:+.2f})")
        except Exception:
            pass

        # -------------------------------------------------------------
        # 8.19 极端单边暴拉/暴跌反转判定机制 (用户明确要求直接反向看多或空)
        # -------------------------------------------------------------
        if price_loss >= extreme_surge_limit:
            counter_confirmed = False
            if direction == "UP":
                if regime in ("BEAR_ATTACK", "LONG_FLUSH") or taker_ratio < 0.72 or cvd_s <= -0.25 or cascade or latest_score <= -0.15:
                    counter_confirmed = True
            else: # DOWN
                if regime in ("BULL_ATTACK", "SHORT_SQUEEZE") or taker_ratio > 1.35 or cvd_s >= 0.25 or cascade or latest_score >= 0.15:
                    counter_confirmed = True

            if counter_confirmed or price_loss >= (extreme_surge_limit * 1.4):
                is_extreme_reversal = True
                dev_score = 1.0
                reason_str = f"⚡ 触发8.19极端单边暴动反转！逆向冲击 -{price_loss:.2f}U (>= {extreme_surge_limit:.1f}U)，盘口动力彻底反转: {'; '.join(reasons)}"
                return True, dev_score, reason_str, is_extreme_reversal, reverse_dir, price_loss

        dev_score = round(dev_score, 3)
        dev_thresh = 0.75 if tf == "1d" else (0.62 if tf == "1h" else 0.60)
        is_deviated = (dev_score >= dev_thresh)
        if is_deviated:
            reason_str = f"⚠️ {aux_label}辅助窗口判定趋势严重偏离 (偏离度={dev_score:.2f} >= {dev_thresh:.2f}): {'; '.join(reasons)}"
        else:
            reason_str = f"✅ {aux_label}辅助窗口核验通过 (偏离度={dev_score:.2f} < {dev_thresh:.2f})"

        return is_deviated, dev_score, reason_str, is_extreme_reversal, reverse_dir, price_loss

    def _evaluate_continuation(self, tf, direction, current_price, market_snapshot):
        """
        到达 TP1 时的关键动作：以当前市价重新推算盘口动能是否支持冲刺 TP2
        """
        try:
            # 拷贝当前快照并将价格更新为到达 TP1 时的实际现价
            eval_snapshot = dict(market_snapshot)
            eval_snapshot["price"] = current_price
            all_preds = self.predictor.predict(eval_snapshot)
            new_pred = all_preds.get(tf, {})
            new_dir = new_pred.get("direction", "NEUTRAL")
            score = new_pred.get("composite_score", 0.0)

            # 判定是否延续同一方向
            if direction == "UP":
                should_continue = (new_dir == "UP") or (score >= 0.05)
            elif direction == "DOWN":
                should_continue = (new_dir == "DOWN") or (score <= -0.05)
            else:
                should_continue = False

            return {
                "evaluated_at": datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S"),
                "current_price": current_price,
                "continuation_direction": new_dir,
                "composite_score": score,
                "should_continue": should_continue,
                "reason": f"现价二次推算得分: {score:+.2f} ({'继续看涨' if score > 0 else '看跌/减弱'})"
            }
        except Exception as e:
            return {
                "evaluated_at": datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S"),
                "current_price": current_price,
                "continuation_direction": direction,
                "composite_score": 0.2 if direction == "UP" else -0.2,
                "should_continue": True,
                "reason": f"默认动能延续 ({e})"
            }

    def _finalize_prediction(self, tf, act, current_price, outcome, is_win,
                             tp1_hit, tp2_hit, sl_hit, accuracy_score, reason, market_snapshot):
        """
        终结当前周期的时序预测并转入 WAITING_SETUP 观望冷却状态：
        1. 归档到历史数据库，记录 MFE / MAE / 实际收盘与终止原因；
        2. 将当前活跃槽位设置为 WAITING_SETUP 状态，等待盘口重新出现强共振时机再开新单。
        """
        pred_id = act.get("pred_id")
        base_px = act.get("base_price", current_price)
        dir_val = act.get("direction", "UP")
        now_ts = int(time.time())
        now_iso = datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")

        highest_seen = act.get("highest_seen", current_price)
        lowest_seen = act.get("lowest_seen", current_price)

        # 统计真实价格位移与最大顺向/逆向偏移 (MFE / MAE)
        if dir_val == "UP":
            pnl_pct = round(((current_price - base_px) / base_px * 100.0), 3)
            mfe_pct = max(0.0, round(((highest_seen - base_px) / base_px * 100.0), 3))
            mae_pct = max(0.0, round(((base_px - lowest_seen) / base_px * 100.0), 3))
        else:
            pnl_pct = round(((base_px - current_price) / base_px * 100.0), 3)
            mfe_pct = max(0.0, round(((base_px - lowest_seen) / base_px * 100.0), 3))
            mae_pct = max(0.0, round(((highest_seen - base_px) / base_px * 100.0), 3))

        verified_result = {
            "verified_at": now_iso,
            "actual_close": current_price,
            "max_price": highest_seen,
            "min_price": lowest_seen,
            "direction_correct": is_win,
            "tp1_hit": tp1_hit,
            "tp2_hit": tp2_hit,
            "sl_hit": sl_hit,
            "outcome": outcome,
            "accuracy_score": accuracy_score,
            "exit_reason": reason,
            "pnl_pct": pnl_pct,
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
        }

        # 更新历史数据库记录
        if pred_id:
            self.storage.update_prediction(pred_id, verified_result, act_obj=act)

        # 设置冷却观望时长 (偏离终止或止损时多观望防晃动磨损)
        cooldown_map = {"5m": 60, "1h": 300, "1d": 1200}
        cooldown_sec = cooldown_map.get(tf, 60) if outcome in ("DEVIATION_STOP", "SL_FAILED") else 15

        # 转入 WAITING_SETUP 观望状态
        self.active_predictions[tf] = self._build_waiting_setup_state(
            tf=tf,
            price=current_price,
            reason=f"前序预测已终止 [{outcome}] ({reason})，正在观望盘口等待下一个明确共振开仓时机"
        )
        self.active_predictions[tf]["cooldown_until_ts"] = now_ts + cooldown_sec
        self._save_active()

        print(f"[Lifecycle] 🏁 [{tf}] 预测已结算归档: 结果={outcome} | 胜负={'胜' if is_win else '负'} | 收益={pnl_pct:+.2f}% | 原因={reason} -> 转入观望等待", flush=True)

        # 尝试检查是否具备新开仓条件
        self.ensure_active_predictions(market_snapshot)

    # 保持向下兼容的别名方法
    _finalize_and_spawn_next = _finalize_prediction

    def get_display_state(self, current_price):
        """
        生成供给前端 Web 界面展示的高清动态状态字典
        """
        now_ts = int(time.time())
        display_map = {}

        for tf in ["5m", "1h", "1d"]:
            act = self.active_predictions.get(tf)
            if not act:
                display_map[tf] = None
                continue

            disp = dict(act)
            status = disp.get("status", "ACTIVE")

            # 针对 WAITING_SETUP 观望等待状态或中性无方向状态的规范化输出
            if status == "WAITING_SETUP" or disp.get("direction") == "NEUTRAL":
                disp.update({
                    "current_price": current_price,
                    "direction": "NEUTRAL",
                    "dir_label": "震荡观望",
                    "dir_icon": "⏸️",
                    "tp1": 0.0,
                    "tp2": 0.0,
                    "sl": 0.0,
                    "target_range": "--",
                    "dist_tp1_u": 0.0,
                    "dist_tp2_u": 0.0,
                    "dist_sl_u": 0.0,
                    "pct_to_tp1": 0.0,
                    "pct_to_tp2": 0.0,
                    "timeout_remaining_sec": 0,
                    "timeout_remaining_fmt": "⏳ 观望待机中",
                    "stage_label": disp.get("stage_label", f"⏸️ [{tf}] 处于窄幅震荡区间，暂无明确单边动能，保持观望中..."),
                })
                display_map[tf] = disp
                continue

            # ACTIVE 状态处理
            base_px = disp.get("base_price", current_price)
            direction = disp.get("direction", "UP")
            tp1 = disp.get("tp1", current_price)
            tp2 = disp.get("tp2", current_price)
            sl = disp.get("sl", current_price)
            created_ts_sec = disp.get("created_ts", now_ts)
            aux_sec = disp.get("aux_window_sec", 180)
            aux_lbl = disp.get("aux_label", "辅助")

            # 补齐多情景走势概率树 (针对热更新前旧记录或等待状态平滑兜底)
            if not disp.get("scenario_tree"):
                from eth_predictor.scenarios import generate_scenario_tree
                disp["scenario_tree"] = generate_scenario_tree(
                    tf=tf,
                    direction=direction,
                    base_price=base_px,
                    tp1=tp1 if tp1 > 0 else (base_px + 3.0 if direction == "UP" else base_px - 3.0),
                    tp2=tp2 if tp2 > 0 else (base_px + 6.0 if direction == "UP" else base_px - 6.0),
                    sl=sl if sl > 0 else (base_px - 4.0 if direction == "UP" else base_px + 4.0),
                    atr=disp.get("atr", 4.0),
                    confidence=disp.get("confidence", 70.0)
                )

            if not disp.get("market_regime"):
                disp["market_regime"] = {
                    "regime": "RANGE_CONSOLIDATION",
                    "label": "区间震荡整理",
                    "weight_multipliers": {}
                }

            elapsed_sec = max(0, now_ts - created_ts_sec)
            next_chk_ts = disp.get("next_trend_check_ts")
            if not next_chk_ts:
                next_chk_ts = created_ts_sec + aux_sec
            rem_chk_sec = max(0, next_chk_ts - now_ts)
            chk_count = disp.get("trend_check_count", 0)

            if chk_count == 0:
                rem_fmt = f"⏳ 首次核验: {rem_chk_sec // 60:02d}分{rem_chk_sec % 60:02d}秒"
            else:
                rem_fmt = f"🔄 第{chk_count}次核验已过 · 下次: {rem_chk_sec // 60:02d}分{rem_chk_sec % 60:02d}秒"

            # 计算与目标位的即时点数差距与达成进度
            favorable_px = disp.get("highest_seen", current_price) if direction == "UP" else disp.get("lowest_seen", current_price)

            if direction == "UP":
                dist_tp1 = tp1 - current_price
                dist_tp2 = tp2 - current_price
                dist_sl = current_price - sl
                if disp.get("tp1_status") == "REACHED":
                    pct_to_tp1 = 100.0
                else:
                    pct_to_tp1 = min(100.0, max(0.0, ((favorable_px - base_px) / (tp1 - base_px) * 100.0))) if tp1 > base_px else 0.0

                if disp.get("tp2_status") == "REACHED":
                    pct_to_tp2 = 100.0
                else:
                    pct_to_tp2 = min(100.0, max(0.0, ((favorable_px - base_px) / (tp2 - base_px) * 100.0))) if tp2 > base_px else 0.0
            else:
                dist_tp1 = current_price - tp1
                dist_tp2 = current_price - tp2
                dist_sl = sl - current_price
                if disp.get("tp1_status") == "REACHED":
                    pct_to_tp1 = 100.0
                else:
                    pct_to_tp1 = min(100.0, max(0.0, ((base_px - favorable_px) / (base_px - tp1) * 100.0))) if base_px > tp1 else 0.0

                if disp.get("tp2_status") == "REACHED":
                    pct_to_tp2 = 100.0
                else:
                    pct_to_tp2 = min(100.0, max(0.0, ((base_px - favorable_px) / (base_px - tp2) * 100.0))) if base_px > tp2 else 0.0

            disp.update({
                "current_price": current_price,
                "dist_tp1_u": round(dist_tp1, 2),
                "dist_tp2_u": round(dist_tp2, 2),
                "dist_sl_u": round(dist_sl, 2),
                "pct_to_tp1": round(pct_to_tp1, 1),
                "pct_to_tp2": round(pct_to_tp2, 1),
                "timeout_remaining_sec": rem_chk_sec,
                "timeout_remaining_fmt": rem_fmt,
                "trend_check_count": chk_count,
                "next_trend_check_ts": next_chk_ts,
            })
            display_map[tf] = disp

        return display_map
