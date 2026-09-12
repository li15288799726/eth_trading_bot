# -*- coding: utf-8 -*-
"""
ETH 多周期预测模型引擎 (models.py)
==================================
负责未来 5分钟 (5m)、1小时 (1h)、1日 (1d) 的涨跌方向、目标价格 (TP1, TP2)、
止损位 (SL)、置信度概率及核心驱动指标归因的生成。
每个周期拥有独立的多因子权重配置，支持自适应微调与自优化。
"""
import copy
import math
import uuid
from datetime import datetime, timezone, timedelta

from eth_predictor.indicators import (
    calc_daily_vwap, calc_atr, calc_rsi, calc_bollinger_bands,
    calc_ema_ribbon, calc_cvd, calc_pivot_levels,
    calc_oi_matrix, calc_positioning_sentiment, calc_liquidation_gravity,
    calc_volume_profile, safe_float,
    calc_rolling_quantiles, calc_adaptive_volume_regime,
    calc_adaptive_vwap_extremes, calc_adaptive_exhaustion
)
from eth_predictor.regime import detect_market_regime
from eth_predictor.scenarios import generate_scenario_tree
from eth_predictor.feature_cross import FeatureCrossEngine

TZ_BJT = timezone(timedelta(hours=8))


# 默认基准模型参数配置 (Default Baseline Weights - Normalized)
DEFAULT_TIMEFRAME_WEIGHTS = {
    "5m": {
        "w_vwap": 0.20,        # 5m 关注 VWAP 均值回踩与偏离
        "w_oi": 0.20,          # 5m 依赖实时 OI 突增突降与量仓象限
        "w_pos": 0.15,         # Taker 买卖比与盘口吃单
        "w_liq": 0.15,         # 近端微观清算单扫损
        "w_tech": 0.15,        # RSI 超买超卖、布林带挤压与防追涨杀跌
        "w_macro": 0.05,       # 突发快讯异动冲击
        "w_hier": 0.15,        # 大中周期层级共振 (强化 1H/1D 顺势引导，杜绝逆向打架)
        "w_cross": 0.12,       # 【阶段二新增】LightGBM 微观特征交互项权重 (限定在特征层)
        "neutral_thresh": 0.16,# 判定中性震荡门槛 (窄幅震荡严禁发假多空，坚决保持观望)
        "min_directional_space": 8.0,  # 5M 最小期望空间底线 (不足 8~10 USDT 保持观望，杜绝鸡肋刷单)
        "min_tp1_dist": 6.0,   # 5M TP1 最低目标距离 (确保覆盖双向手续费与微观滑点)
        "min_tp2_dist": 10.0,  # 5M TP2 冲刺目标空间 (满足完整 10 点波段空间)
        "atr_tp1_mult": 1.10,  # 目标 1 ATR 乘数 (约 4.5~6.5 USDT)
        "atr_tp2_mult": 2.20,  # 目标 2 ATR 乘数 (约 9.0~14.0 USDT)
        "atr_sl_mult": 1.45,   # 结构失效边界乘数 (1.45 ATR，保留微观抗扰空间)
        "min_confidence": 65.0,# 基础置信度起步
    },
    "1h": {
        "w_vwap": 0.20,        # VWAP 轨道与日内趋势斜率
        "w_oi": 0.20,          # 1h OI 累积增减与主力建仓
        "w_pos": 0.15,         # 大户持仓量多空比
        "w_liq": 0.20,         # 显著清算密集带磁吸驱动
        "w_tech": 0.10,        # EMA 彩带趋势排列
        "w_macro": 0.05,       # 宏观资金面
        "w_hier": 0.12,        # 1d 宏观趋势层级引导
        "neutral_thresh": 0.14,# 1H 判定中性震荡门槛 (低于此阈值坚决保持观望)
        "min_directional_space": 30.0, # 1H 最小期望空间底线 (不足 30 USDT 保持观望，不发多空)
        "min_tp1_dist": 18.0,  # 1H TP1 最低目标距离
        "min_tp2_dist": 30.0,  # 1H TP2 冲刺目标空间 (满足完整 30 点日内波段)
        "atr_tp1_mult": 1.10,  # 目标 1 ATR 乘数 (约 16~22 USDT，贴合日内波段现实)
        "atr_tp2_mult": 2.00,  # 目标 2 ATR 乘数 (约 30~42 USDT)
        "atr_sl_mult": 1.50,   # 结构失效边界乘数 (1.50 ATR)
        "min_confidence": 68.0,
    },
    "1d": {
        "w_vwap": 0.20,        # 宏观日线 VWAP 锚定
        "w_oi": 0.20,          # 24h OI 宏观趋势
        "w_pos": 0.20,         # 机构大户底仓多空偏好与资金费率
        "w_liq": 0.20,         # 三所聚合巨额清算深水区引力
        "w_tech": 0.10,        # 大周期日线结构支撑阻力
        "w_macro": 0.10,       # 宏观流动性与突发事件
        "neutral_thresh": 0.15,# 1D 判定中性震荡门槛 (低于此阈值坚决保持观望)
        "min_directional_space": 60.0, # 1D 最小期望空间底线 (不足 50~70 USDT 保持观望，不发大势)
        "min_tp1_dist": 35.0,  # 1D TP1 最低目标距离
        "min_tp2_dist": 60.0,  # 1D TP2 冲刺目标空间 (满足 60 点宏观大波段)
        "atr_tp1_mult": 0.75,  # 目标 1 ATR 乘数 (约 45~65 USDT，匹配当前日内振幅)
        "atr_tp2_mult": 1.45,  # 目标 2 ATR 乘数 (约 80~120 USDT)
        "atr_sl_mult": 1.60,   # 结构失效边界乘数 (1.60 ATR，合理止损防守空间)
        "min_confidence": 70.0,
    }
}


class ETHPredictor:
    def __init__(self, weights=None):
        self.weights = copy.deepcopy(weights or DEFAULT_TIMEFRAME_WEIGHTS)
        self.top_ls_history = []  # 滚动保存近端 top_ls_ratio 样本以计算自适应分位数中位数 (改动3)
        self.cross_engine = FeatureCrossEngine()  # 阶段二：LightGBM 特征交换层引擎 (无缝双模推断)

    def get_dynamic_ls_baseline(self, current_top_ls):
        """自适应大户多空比动态基准 (改动3: 替换写死的 1.30)"""
        val = safe_float(current_top_ls)
        if val > 0:
            self.top_ls_history.append(val)
            if len(self.top_ls_history) > 144:  # 保留最多 12 小时 5m 样本 (144根)
                self.top_ls_history.pop(0)
        if len(self.top_ls_history) >= 12:
            sorted_ls = sorted(self.top_ls_history)
            return sorted_ls[len(sorted_ls) // 2]
        return 1.25  # 冷启动默认平滑基准

    def update_weights(self, new_weights):
        """更新模型超参数权重 (自优化器调用)"""
        for tf, w in new_weights.items():
            if tf in self.weights:
                self.weights[tf].update(w)

    def predict(self, market_snapshot):
        """
        根据当前全市场快照，生成 5m / 1h / 1d 三大周期的预测报告
        market_snapshot 包含:
          - price: float
          - klines_5m: list
          - klines_1h: list
          - klines_1d: list
          - oi_current, oi_delta_5m, oi_delta_1h
          - taker_buy_vol_5m, taker_sell_vol_5m, buy_sell_ratio_5m
          - top_long_pct, top_short_pct, top_ls_ratio
          - global_ls_ratio, funding_rate
          - liq_raw_data: dict
          - vwap_daily: dict (optional)
        """
        price = safe_float(market_snapshot.get("price"))
        if not price or price <= 0:
            return {}

        kl_5m = market_snapshot.get("klines_5m", [])
        kl_1h = market_snapshot.get("klines_1h", [])
        kl_1d = market_snapshot.get("klines_1d", [])
        liq_raw = market_snapshot.get("liq_raw_data", {})

        # 1. 基础指标计算
        vwap_daily = market_snapshot.get("vwap_daily") or calc_daily_vwap(kl_5m)
        atr_5m = calc_atr(kl_5m, 14)
        atr_1h = calc_atr(kl_1h, 14) if kl_1h else max(10.0, atr_5m * 2.5)
        atr_1d = calc_atr(kl_1d, 14) if kl_1d else max(40.0, atr_1h * 3.5)

        rsi_5m = calc_rsi(kl_5m, 14)
        rsi_1h = calc_rsi(kl_1h, 14) if kl_1h else 50.0

        bb_5m = calc_bollinger_bands(kl_5m, 20, 2.0)
        ema_ribbon_5m = calc_ema_ribbon(kl_5m, (9, 21, 55))
        ema_ribbon_1h = calc_ema_ribbon(kl_1h, (9, 21, 55)) if kl_1h else {"score": 0.0, "alignment": "NEUTRAL"}
        cvd_5m = calc_cvd(kl_5m, 24)

        # 机构级筹码分布与拍卖市场价值区 (Volume Profile: POC, VAH, VAL)
        vp_1h = calc_volume_profile(kl_1h, 48) if kl_1h else {}
        vp_5m = calc_volume_profile(kl_5m, 48) if kl_5m else {}

        # 2. 微观量仓与持仓情绪
        oi_current = safe_float(market_snapshot.get("oi_current", 0.0))
        oi_delta_5m = safe_float(market_snapshot.get("oi_delta_5m", 0.0))
        oi_delta_1h = safe_float(market_snapshot.get("oi_delta_1h", 0.0))
        oi_delta_1d = safe_float(market_snapshot.get("oi_delta_1d", 0.0))
        vol_ratio = safe_float(market_snapshot.get("vol_ratio", 1.0))
        taker_ratio_5m = safe_float(market_snapshot.get("buy_sell_ratio_5m", 1.0))
        top_ls_ratio = safe_float(market_snapshot.get("top_ls_ratio", 1.0))
        global_ls_ratio = safe_float(market_snapshot.get("global_ls_ratio", 1.0))
        funding_rate = safe_float(market_snapshot.get("funding_rate", 0.0001))

        # 价格变化量
        px_chg_5m = (price - safe_float(kl_5m[-2][4])) if len(kl_5m) >= 2 else 0.0
        px_chg_1h = (price - safe_float(kl_1h[-2][4])) if len(kl_1h) >= 2 else 0.0
        px_chg_1d = (price - safe_float(kl_1d[-2][4])) if (kl_1d and len(kl_1d) >= 2) else px_chg_1h

        oi_matrix_5m = calc_oi_matrix(oi_current, oi_delta_5m, oi_delta_1h, px_chg_5m, px_chg_1h, tf="5m")
        oi_matrix_1h = calc_oi_matrix(oi_current, oi_delta_5m, oi_delta_1h, px_chg_5m, px_chg_1h, tf="1h")
        oi_matrix_1d = calc_oi_matrix(oi_current, oi_delta_5m, oi_delta_1h, px_chg_5m, px_chg_1h, tf="1d", oi_delta_1d=oi_delta_1d, price_change_1d=px_chg_1d)
        # 1. 动态自适应大户持仓基准 (改动3: 替换写死的 1.30，消除常态化看多偏差)
        dyn_ls_baseline = self.get_dynamic_ls_baseline(top_ls_ratio)
        pos_sentiment = calc_positioning_sentiment(top_ls_ratio, 1.0, global_ls_ratio, taker_ratio_5m, funding_rate, top_ls_baseline=dyn_ls_baseline)
        liq_gravity = calc_liquidation_gravity(price, liq_raw, gamma=1.2)

        # 动态自适应量能范式与目标乘数 (改动3: 动态量比)
        vol_regime_5m = calc_adaptive_volume_regime(kl_5m)
        vol_ratio_5m = vol_regime_5m["vol_ratio"]
        vol_target_mult_5m = vol_regime_5m["target_multiplier"]

        # 动态自适应 VWAP 偏离极值与过热边界 (改动3: 动态替换硬编码的 ±1.6σ 和 ±2.2σ)
        vwap_ext_5m = calc_adaptive_vwap_extremes(kl_5m, vwap_daily)

        # 动态自适应布林带与 RSI 耗竭 (改动3: 动态替换硬编码的 62/38)
        exhaustion_5m = calc_adaptive_exhaustion(bb_5m, rsi_5m)

        # 宏观突发事件、以太坊基金会与 ETF 资金流因子
        macro_events = market_snapshot.get("macro_events") or {}
        s_macro = safe_float(macro_events.get("composite_event_score", 0.0))
        vol_mult = safe_float(macro_events.get("volatility_multiplier", 1.0))

        # 0. 市场微观范式状态机识别 (Market Regime Classification)
        market_regime = detect_market_regime(kl_5m, kl_1h, vwap_daily, cvd_5m, bb_5m, atr_5m, oi_matrix_5m)
        wm = market_regime.get("weight_multipliers", {})

        rt_liq = market_snapshot.get("realtime_liquidations") or {}
        btc_lead = market_snapshot.get("btc_lead_lag") or {}
        pivots_5m = calc_pivot_levels(kl_5m)
        pivots_1h = calc_pivot_levels(kl_1h)
        pivots_1d = calc_pivot_levels(kl_1d)
        bb_1h = calc_bollinger_bands(kl_1h, 20, 2.0) if kl_1h else {}
        bb_1d = calc_bollinger_bands(kl_1d, 20, 2.0) if kl_1d else {}

        results = {}
        now_ts = int(datetime.now().timestamp())
        now_iso = datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")

        # =============================================================
        # 周期一优先计算：未来 1 日 (1d) 宏观趋势（大周期定总基调）
        # =============================================================
        w1d = dict(self.weights["1d"])
        w1d["w_tech"] = w1d["w_tech"] * wm.get("w_tech", 1.0)
        w1d["w_oi"] = w1d["w_oi"] * wm.get("w_oi", 1.0)
        w1d["w_vwap"] = w1d["w_vwap"] * wm.get("w_vwap", 1.0)
        w1d["w_liq"] = w1d["w_liq"] * wm.get("w_liq", 1.0)
        w1d["w_pos"] = w1d["w_pos"] * wm.get("w_pos", 1.0)

        s_oi_1d = oi_matrix_1d["signal"]

        # 1D 大户持仓比：以动态滚动中位数为基准点，消除常态化无脑看多偏差 (改动3)
        top_centered = (top_ls_ratio / dyn_ls_baseline) if (dyn_ls_baseline and dyn_ls_baseline > 0) else 1.0
        s_pos_1d = (math.log(top_centered) / math.log(1.4)) if top_centered > 0 else 0.0
        s_pos_1d = max(-1.0, min(1.0, s_pos_1d))

        s_liq_1d = liq_gravity["net_score"]

        # 核心逻辑：1D 真实突破 vs 箱体震荡判定 (结合 48H 筹码分布 VAH/VAL 机制)
        pct_b_1d = safe_float(bb_1d.get("pct_b", 0.5)) if bb_1d else 0.5
        z_1d = safe_float(vwap_daily.get("z_score", 0.0)) if vwap_daily else 0.0

        poc_1d = vp_1h.get("poc", price)
        vah_1d = vp_1h.get("vah", price + 35.0)
        val_1d = vp_1h.get("val", price - 35.0)

        recent_3d_high = max(safe_float(k[2]) for k in kl_1d[-4:-1]) if (kl_1d and len(kl_1d) >= 4) else (price + 60.0)
        recent_3d_low = min(safe_float(k[3]) for k in kl_1d[-4:-1]) if (kl_1d and len(kl_1d) >= 4) else (price - 60.0)
        is_1d_breakout_up = (price >= recent_3d_high) and (s_oi_1d > 0.25 or vol_ratio >= 1.25)
        is_1d_breakout_down = (price <= recent_3d_low) and (s_oi_1d < -0.25 or vol_ratio >= 1.25)

        is_1d_at_ceiling = (not is_1d_breakout_up) and (price >= poc_1d) and (
            (price >= vah_1d - 5.0) or
            (price >= recent_3d_high - 12.0) or
            (pct_b_1d >= 0.75) or
            (z_1d >= 1.2)
        )
        is_1d_at_floor = (not is_1d_breakout_down) and (price <= poc_1d) and (
            (price <= val_1d + 5.0) or
            (price <= recent_3d_low + 12.0) or
            (pct_b_1d <= 0.25) or
            (z_1d <= -1.2)
        )

        if is_1d_breakout_up:
            # 真实大周期向上突破顺势主升
            s_vwap_1d = min(1.0, max(0.2, z_1d / 2.0))
            if kl_1d and len(kl_1d) >= 2:
                prev_close_1d = safe_float(kl_1d[-2][4])
                pct_1d = ((price - prev_close_1d) / prev_close_1d * 100.0) if prev_close_1d > 0 else 0.0
                s_tech_1d = min(1.0, max(0.25, pct_1d / 1.5))
            else:
                s_tech_1d = 0.3
        elif is_1d_breakout_down:
            # 真实大周期向下击穿顺势主跌
            s_vwap_1d = max(-1.0, min(-0.2, z_1d / 2.0))
            if kl_1d and len(kl_1d) >= 2:
                prev_close_1d = safe_float(kl_1d[-2][4])
                pct_1d = ((price - prev_close_1d) / prev_close_1d * 100.0) if prev_close_1d > 0 else 0.0
                s_tech_1d = max(-1.0, min(-0.25, pct_1d / 1.5))
            else:
                s_tech_1d = -0.3
        elif is_1d_at_ceiling:
            # 日线顶部接空区 (Range Ceiling Short Zone)：
            # 价格逼近日内/波段阻力天花板 VAH 与近期高点，买盘遇阻滞涨，锁定向下均值回归引力
            s_vwap_1d = -0.75
            s_tech_1d = -0.60
            s_oi_1d = -0.20
        elif is_1d_at_floor:
            # 日线底部接多区 (Range Floor Long Zone)：
            # 价格逼近日内/波段支撑地板 VAL 与近期低点，抛压出清吸筹，锁定向上均值回归反弹
            s_vwap_1d = 0.75
            s_tech_1d = 0.60
            s_oi_1d = 0.20
        else:
            s_vwap_1d = 0.10 if (vwap_daily and vwap_daily.get("slope", 0) > 0) else -0.10
            if kl_1d and len(kl_1d) >= 2:
                prev_close_1d = safe_float(kl_1d[-2][4])
                pct_1d = ((price - prev_close_1d) / prev_close_1d * 100.0) if prev_close_1d > 0 else 0.0
                s_tech_1d = max(-0.25, min(0.25, pct_1d / 3.0))
            else:
                s_tech_1d = 0.0

        composite_1d = (
            w1d["w_vwap"] * s_vwap_1d +
            w1d["w_oi"] * s_oi_1d +
            w1d["w_pos"] * s_pos_1d +
            w1d["w_liq"] * s_liq_1d +
            w1d["w_tech"] * s_tech_1d +
            w1d.get("w_macro", 0.10) * s_macro
        )
        composite_1d = max(-1.0, min(1.0, composite_1d))
        atr_1d_scaled = atr_1d * vol_mult

        pred_1d = self._build_prediction_record(
            pred_id=f"pred_1d_{now_ts}_{uuid.uuid4().hex[:6]}",
            tf="1d",
            price=price,
            composite_score=composite_1d,
            atr=atr_1d_scaled,
            weights=w1d,
            liq_gravity=liq_gravity,
            vwap_daily=vwap_daily,
            oi_info=oi_matrix_1d,
            pos_info=pos_sentiment,
            macro_events=macro_events,
            created_ts=now_ts,
            created_iso=now_iso,
            expiry_seconds=86400,
            parent_bias=0.0,
            market_regime=market_regime,
            bb_bands=bb_1d,
            pivot_levels=pivots_1d,
            btc_lead_lag=btc_lead,
            realtime_liquidations=rt_liq,
            volume_profile=vp_1h,
            is_ceiling_short=is_1d_at_ceiling,
            is_floor_long=is_1d_at_floor
        )
        results["1d"] = pred_1d

        # =============================================================
        # 周期二其次计算：未来 1 小时 (1h) 日内波段（融入 1d 宏观共振引导）
        # =============================================================
        w1h = dict(self.weights["1h"])
        w1h["w_tech"] = w1h["w_tech"] * wm.get("w_tech", 1.0)
        w1h["w_oi"] = w1h["w_oi"] * wm.get("w_oi", 1.0)
        w1h["w_vwap"] = w1h["w_vwap"] * wm.get("w_vwap", 1.0)
        w1h["w_liq"] = w1h["w_liq"] * wm.get("w_liq", 1.0)
        w1h["w_pos"] = w1h["w_pos"] * wm.get("w_pos", 1.0)
        # 1H 筹码分布与拍卖价值区核心参数 (Volume Profile 48h)
        poc_1h = vp_1h.get("poc", price)
        vah_1h = vp_1h.get("vah", price + 20.0)
        val_1h = vp_1h.get("val", price - 20.0)
        rhigh_1h = vp_1h.get("range_high", vah_1h)
        rlow_1h = vp_1h.get("range_low", val_1h)

        pct_b_1h = safe_float(bb_1h.get("pct_b", 0.5)) if bb_1h else 0.5
        bw_1h = safe_float(bb_1h.get("bandwidth_pct", 3.0)) if bb_1h else 3.0
        regime_code = market_regime.get("regime", "RANGE_CONSOLIDATION")
        is_1h_consolidating = (bw_1h <= 3.2) and (regime_code == "RANGE_CONSOLIDATION")

        is_1h_breakout_up = (price >= rhigh_1h - 2.0) and (vol_ratio >= 1.4) and (oi_matrix_1h["signal"] >= 0.15)
        is_1h_breakout_down = (price <= rlow_1h + 2.0) and (vol_ratio >= 1.4) and (oi_matrix_1h["signal"] <= -0.15)

        # 1H 顶部接空判定 (Range Ceiling Short Zone):
        # 排除 1D 宏观主升突破 (composite_1d < 0.25 且 not is_1d_breakout_up) 且现价位于 POC 之上
        is_1h_at_ceiling = (not is_1h_breakout_up) and (composite_1d < 0.25) and (not is_1d_breakout_up) and (price >= poc_1h) and (
            (price >= vah_1h - 3.0) or
            (pct_b_1h >= 0.80) or
            (pivots_1h and price >= pivots_1h.get("r1", 99999) - 2.0)
        )

        # 1H 底部接多判定 (Range Floor Long Zone):
        # 排除 1D 宏观主跌击穿 (composite_1d > -0.25 且 not is_1d_breakout_down) 且现价位于 POC 之下
        is_1h_at_floor = (not is_1h_breakout_down) and (composite_1d > -0.25) and (not is_1d_breakout_down) and (price <= poc_1h) and (
            (price <= val_1h + 3.0) or
            (pct_b_1h <= 0.20) or
            (pivots_1h and price <= pivots_1h.get("s1", 0) + 2.0)
        )

        s_oi_1h = oi_matrix_1h["signal"]
        s_pos_1h = pos_sentiment["composite_score"]
        s_liq_1h = liq_gravity["net_score"]
        rsi_score_1h = max(-1.0, min(1.0, (rsi_1h - 50.0) / 20.0))

        if is_1h_breakout_up:
            s_vwap_1h = min(1.0, max(0.3, (price - poc_1h) / max(1.0, atr_1h)))
            s_tech_1h = 0.50
        elif is_1h_breakout_down:
            s_vwap_1h = max(-1.0, min(-0.3, (price - poc_1h) / max(1.0, atr_1h)))
            s_tech_1h = -0.50
        elif is_1h_at_ceiling:
            # 1H 顶部接空：向日内筹码核心 POC / VWAP 均值回归
            s_vwap_1h = -0.75
            s_tech_1h = -0.60
            s_oi_1h = -0.25
            if s_pos_1h > 0:
                s_pos_1h = -0.10
        elif is_1h_at_floor:
            # 1H 底部接多：向日内筹码核心 POC / VWAP 均值回归
            s_vwap_1h = 0.75
            s_tech_1h = 0.60
            s_oi_1h = 0.25
            if s_pos_1h < 0:
                s_pos_1h = 0.10
        else:
            if vwap_daily:
                s_vwap_1h = min(0.3, max(-0.3, (vwap_daily.get("slope", 0) / 4.0)))
            else:
                s_vwap_1h = 0.0
            s_tech_1h = (ema_ribbon_1h.get("score", 0.0) * 0.5) + (rsi_score_1h * 0.3)

        composite_1h = (
            w1h["w_vwap"] * s_vwap_1h +
            w1h["w_oi"] * s_oi_1h +
            w1h["w_pos"] * s_pos_1h +
            w1h["w_liq"] * s_liq_1h +
            w1h["w_tech"] * s_tech_1h +
            w1h.get("w_macro", 0.05) * s_macro +
            w1h.get("w_hier", 0.10) * composite_1d  # 宏观趋势层级引导
        )
        composite_1h = max(-1.0, min(1.0, composite_1h))
        atr_1h_scaled = atr_1h * vol_mult

        pred_1h = self._build_prediction_record(
            pred_id=f"pred_1h_{now_ts}_{uuid.uuid4().hex[:6]}",
            tf="1h",
            price=price,
            composite_score=composite_1h,
            atr=atr_1h_scaled,
            weights=w1h,
            liq_gravity=liq_gravity,
            vwap_daily=vwap_daily,
            oi_info=oi_matrix_1h,
            pos_info=pos_sentiment,
            macro_events=macro_events,
            created_ts=now_ts,
            created_iso=now_iso,
            expiry_seconds=3600,
            parent_bias=composite_1d,
            market_regime=market_regime,
            bb_bands=bb_1h,
            pivot_levels=pivots_1h,
            btc_lead_lag=btc_lead,
            realtime_liquidations=rt_liq,
            volume_profile=vp_1h,
            is_ceiling_short=is_1h_at_ceiling,
            is_floor_long=is_1h_at_floor
        )
        results["1h"] = pred_1h

        # =============================================================
        # 周期三最终计算：未来 5 分钟 (5m) 战术执行捕捉器 (Execution Timing Trigger)
        # 【改动 1】以 1H 战略主轴锚定，5M 专职寻找高盈亏比顺势回踩/反弹入场点
        # =============================================================
        w5 = dict(self.weights["5m"])
        w5["w_tech"] = w5["w_tech"] * wm.get("w_tech", 1.0)
        w5["w_oi"] = w5["w_oi"] * wm.get("w_oi", 1.0)
        w5["w_vwap"] = w5["w_vwap"] * wm.get("w_vwap", 1.0)
        w5["w_liq"] = w5["w_liq"] * wm.get("w_liq", 1.0)
        w5["w_pos"] = w5["w_pos"] * wm.get("w_pos", 1.0)

        # 1. 5M 动态微观订单流与吸收形态分析 (结合改动3动态量比与自适应耗竭)
        c_open_5m = safe_float(kl_5m[-1][1]) if kl_5m else price
        c_high_5m = safe_float(kl_5m[-1][2]) if kl_5m else price
        c_low_5m = safe_float(kl_5m[-1][3]) if kl_5m else price
        c_close_5m = safe_float(kl_5m[-1][4]) if kl_5m else price
        c_range_5m = max(0.5, c_high_5m - c_low_5m)
        lower_wick_ratio = (min(c_open_5m, c_close_5m) - c_low_5m) / c_range_5m
        upper_wick_ratio = (c_high_5m - max(c_open_5m, c_close_5m)) / c_range_5m
        pct_b_5m = safe_float(bb_5m.get("pct_b", 0.5)) if bb_5m else 0.5

        # 5M 筹码分布与拍卖价值区核心参数 (Volume Profile 4h)
        poc_5m = vp_5m.get("poc", price)
        vah_5m = vp_5m.get("vah", price + 5.0)
        val_5m = vp_5m.get("val", price - 5.0)
        rhigh_5m = vp_5m.get("range_high", vah_5m)
        rlow_5m = vp_5m.get("range_low", val_5m)

        s_oi_5m = oi_matrix_5m["signal"]
        s_pos_5m = pos_sentiment["composite_score"]
        s_liq_5m = liq_gravity["net_score"]

        # 融入毫秒级实时爆仓流 (forceOrder)
        rt_total_vol = safe_float(rt_liq.get("total_liq_vol_eth", 0.0))
        rt_bias = safe_float(rt_liq.get("net_bias", 0.0))
        if rt_total_vol >= 20.0:
            s_liq_5m = round(s_liq_5m * 0.65 + rt_bias * 0.35, 3)

        # 融入 BTC 领先滞后先导 Alpha (BTC Lead-Lag Factor)
        s_btc_lead = safe_float(btc_lead.get("spillover_score", 0.0))
        btc_sig = btc_lead.get("lead_signal", "SYNCHRONIZED")
        w_btc = 0.12 if btc_sig != "SYNCHRONIZED" else 0.05

        # 动态自适应耗竭抑制与吸收加权 (改动3: 动态替换硬编码)
        exhaustion_penalty = exhaustion_5m["penalty"]

        # 阶段二：LightGBM 微观特征交互层 (仅作用于特征交叉层，严禁进入决策层)
        cross_input = {
            "oi_delta_5m": oi_delta_5m,
            "oi_delta_1h": oi_delta_1h,
            "cvd_5m": cvd_5m,
            "liq_gravity": liq_gravity,
            "vwap_daily": vwap_daily,
            "pos_info": {"top_ls_ratio": top_ls_ratio},
            "dyn_ls_baseline": dyn_ls_baseline,
            "taker_ratio_5m": taker_ratio_5m,
            "funding_rate": funding_rate,
            "vol_regime_5m": vol_regime_5m,
            "lower_wick_ratio": lower_wick_ratio,
            "upper_wick_ratio": upper_wick_ratio,
            "bb_5m": bb_5m,
            "btc_lead": btc_lead,
        }
        cross_res = self.cross_engine.evaluate(cross_input)
        s_cross_mom = cross_res.get("s_cross_momentum", 0.0)
        s_cross_abs = cross_res.get("s_cross_absorption", 0.0)
        gamma_elasticity = safe_float(cross_res.get("gamma_elasticity", 1.0))
        cross_regime = cross_res.get("cross_regime_tag", "BALANCED")

        # 1H 战略方向基准裁决 (【改动 1】核心战略锚定)
        strategic_1h = pred_1h.get("direction", "NEUTRAL")
        score_1h = safe_float(pred_1h.get("composite_score", 0.0))

        # 特例：极端单边放量真突破跟随 (Explosive Breakout)
        is_5m_explosive_up = (vol_regime_5m["is_surge"]) and (price >= rhigh_5m - 0.5) and (s_oi_5m > 0.25)
        is_5m_explosive_down = (vol_regime_5m["is_surge"]) and (price <= rlow_5m + 0.5) and (s_oi_5m < -0.25)

        custom_5m_dir = None
        custom_5m_label = None
        is_5m_at_ceiling = False
        is_5m_at_floor = False
        composite_5m = 0.0

        if is_5m_explosive_up:
            custom_5m_dir = "UP"
            custom_5m_label = "⚡ 极端放量真突破跟随追多"
            composite_5m = 0.60
            is_5m_at_floor = True
        elif is_5m_explosive_down:
            custom_5m_dir = "DOWN"
            custom_5m_label = "⚡ 极端放量真破位跟随追空"
            composite_5m = -0.60
            is_5m_at_ceiling = True
        elif strategic_1h == "UP":
            # -----------------------------------------------------------------
            # 1H 战略看多：5M 坚决不开空！专职寻找回踩支撑的极佳接多入场点
            # -----------------------------------------------------------------
            is_near_poc = abs(price - poc_5m) <= (atr_5m * 0.8)
            is_near_val = price <= (val_5m + atr_5m * 0.8)
            is_near_vwap = vwap_ext_5m["current_z"] <= 0.3
            is_wick_absorption = (lower_wick_ratio >= 0.30 and pct_b_5m <= 0.35) or (exhaustion_penalty > 0.2) or (s_cross_abs >= 0.35)

            is_pullback_ready = (is_near_poc or is_near_val or is_near_vwap or is_wick_absorption)
            is_flow_healthy = (cvd_5m["score"] >= -0.35) and (s_oi_5m >= -0.35) and (s_cross_mom >= -0.45)
            # 空间核验：向上至阻力区需至少具备 6.5~8 点可用波段空间，严禁顶在天花板上接多
            has_upward_space = (vah_5m - price >= 6.5) or (price >= rhigh_5m - 0.5)

            if is_pullback_ready and is_flow_healthy and has_upward_space:
                custom_5m_dir = "UP"
                custom_5m_label = "🎯 1H顺势·5M回踩吸筹接多"
                composite_5m = max(0.24, round(score_1h * 0.60 + 0.22 + 0.12 * s_cross_mom, 3))
                is_5m_at_floor = True
            else:
                custom_5m_dir = "NEUTRAL"
                custom_5m_label = "⏳ 1H顺势看涨·5M空间受阻(接近阻力)" if not has_upward_space else "⏳ 1H顺势看涨·5M脉冲观望(等待回踩)"
                composite_5m = 0.05
        elif strategic_1h == "DOWN":
            # -----------------------------------------------------------------
            # 1H 战略看空：5M 坚决不开多！专职寻找反弹阻力的极佳接空入场点
            # -----------------------------------------------------------------
            is_near_poc = abs(price - poc_5m) <= (atr_5m * 0.8)
            is_near_vah = price >= (vah_5m - atr_5m * 0.8)
            is_near_vwap = vwap_ext_5m["current_z"] >= -0.3
            is_wick_exhaustion = (upper_wick_ratio >= 0.30 and pct_b_5m >= 0.65) or (exhaustion_penalty < -0.2) or (s_cross_abs <= -0.35)

            is_rally_ready = (is_near_poc or is_near_vah or is_near_vwap or is_wick_exhaustion)
            is_flow_healthy = (cvd_5m["score"] <= 0.35) and (s_oi_5m <= 0.35) and (s_cross_mom <= 0.45)
            # 空间核验：向下至支撑区需至少具备 6.5~8 点可用波段空间，严禁砸在地板上接空
            has_downward_space = (price - val_5m >= 6.5) or (price <= rlow_5m + 0.5)

            if is_rally_ready and is_flow_healthy and has_downward_space:
                custom_5m_dir = "DOWN"
                custom_5m_label = "🎯 1H顺势·5M反弹阻力接空"
                composite_5m = min(-0.24, round(score_1h * 0.60 - 0.22 + 0.12 * s_cross_mom, 3))
                is_5m_at_ceiling = True
            else:
                custom_5m_dir = "NEUTRAL"
                custom_5m_label = "⏳ 1H顺势看跌·5M空间受阻(接近支撑)" if not has_downward_space else "⏳ 1H顺势看跌·5M下探观望(等待反弹)"
                composite_5m = -0.05
        else:
            # -----------------------------------------------------------------
            # 1H 震荡观望 (NEUTRAL)：5M 严格执行拍卖市场箱体边缘高抛低吸
            # -----------------------------------------------------------------
            if (price <= val_5m + 0.8) and (lower_wick_ratio >= 0.30 or exhaustion_penalty > 0.2 or s_cross_abs >= 0.35):
                custom_5m_dir = "UP"
                custom_5m_label = "1H震荡·5M箱体底部接多"
                composite_5m = round(0.28 + 0.10 * s_cross_mom, 3)
                is_5m_at_floor = True
            elif (price >= vah_5m - 0.8) and (upper_wick_ratio >= 0.30 or exhaustion_penalty < -0.2 or s_cross_abs <= -0.35):
                custom_5m_dir = "DOWN"
                custom_5m_label = "1H震荡·5M箱体顶部接空"
                composite_5m = round(-0.28 + 0.10 * s_cross_mom, 3)
                is_5m_at_ceiling = True
            else:
                custom_5m_dir = "NEUTRAL"
                custom_5m_label = "1H震荡·5M中轴观望"
                composite_5m = 0.0

        hier_bias = composite_1h * 0.70 + composite_1d * 0.30
        atr_5m_scaled = atr_5m * vol_mult * vol_target_mult_5m * gamma_elasticity

        pred_5m = self._build_prediction_record(
            pred_id=f"pred_5m_{now_ts}_{uuid.uuid4().hex[:6]}",
            tf="5m",
            price=price,
            composite_score=composite_5m,
            atr=atr_5m_scaled,
            weights=w5,
            liq_gravity=liq_gravity,
            vwap_daily=vwap_daily,
            oi_info=oi_matrix_5m,
            pos_info=pos_sentiment,
            macro_events=macro_events,
            created_ts=now_ts,
            created_iso=now_iso,
            expiry_seconds=300,
            parent_bias=hier_bias,
            market_regime=market_regime,
            bb_bands=bb_5m,
            pivot_levels=pivots_5m,
            btc_lead_lag=btc_lead,
            realtime_liquidations=rt_liq,
            volume_profile=vp_5m,
            is_ceiling_short=is_5m_at_ceiling,
            is_floor_long=is_5m_at_floor,
            custom_dir_label=custom_5m_label,
            custom_direction=custom_5m_dir,
            feature_cross=cross_res
        )
        results["5m"] = pred_5m
        results["feature_cross"] = cross_res

        return results

    def _build_prediction_record(self, pred_id, tf, price, composite_score, atr, weights,
                                 liq_gravity, vwap_daily, oi_info, pos_info,
                                 created_ts, created_iso, expiry_seconds, macro_events=None, parent_bias=0.0,
                                 market_regime=None, bb_bands=None, pivot_levels=None,
                                 btc_lead_lag=None, realtime_liquidations=None, volume_profile=None,
                                 is_ceiling_short=False, is_floor_long=False,
                                 custom_dir_label=None, custom_direction=None,
                                 feature_cross=None):
        """生成单周期实时预测记录，含方向、预期目标带、结构失效线、置信度、驱动归因及多情景概率树"""
        thresh = weights.get("neutral_thresh", 0.08)
        k_tp1 = weights.get("atr_tp1_mult", 0.45)
        k_tp2 = weights.get("atr_tp2_mult", 0.90)
        k_sl = weights.get("atr_sl_mult", 1.25)
        base_conf = weights.get("min_confidence", 65.0)
        attribution_tags = []
        is_counter_trend = False

        # 市场范式自适应系数调节
        if market_regime:
            wm = market_regime.get("weight_multipliers", {})
            k_tp1 = k_tp1 * wm.get("tp_multiplier_boost", 1.0)
            k_tp2 = k_tp2 * wm.get("tp_multiplier_boost", 1.0)
            base_conf = max(55.0, min(80.0, base_conf + wm.get("confidence_boost", 0.0)))
            reg_lbl = market_regime.get("label", "")
            if reg_lbl:
                attribution_tags.append(reg_lbl)

        # 判定方向与死区防抖 (优先支持高阶周期战术指令 custom_direction，其次响应拍卖市场价值区结构)
        if custom_direction is not None:
            direction = custom_direction
            dir_icon = "🚀" if direction == "UP" else ("🔻" if direction == "DOWN" else "⏸️")
            dir_label = custom_dir_label or ("顺势看涨" if direction == "UP" else ("顺势看跌" if direction == "DOWN" else "震荡观望"))
            if direction == "NEUTRAL":
                confidence = base_conf
                attribution_tags.append("观望待机")
            else:
                intensity = min(1.0, abs(composite_score) / 0.6)
                confidence = round(max(base_conf + 5.0, 76.0) + intensity * (94.0 - max(base_conf + 5.0, 76.0)), 1)
                attribution_tags.append("1H顺势战术点位")
        elif is_ceiling_short:
            direction = "DOWN"
            dir_icon = "🔻"
            dir_label = "顶部接空 (VAH遇阻)"
            confidence = max(base_conf + 8.0, 78.0)
            attribution_tags.append("顶部接空")
        elif is_floor_long:
            direction = "UP"
            dir_icon = "🚀"
            dir_label = "底部接多 (VAL吸筹)"
            confidence = max(base_conf + 8.0, 78.0)
            attribution_tags.append("底部接多")
        elif abs(composite_score) < thresh:
            direction = "NEUTRAL"
            dir_icon = "⏸️"
            dir_label = "震荡观望"
            confidence = base_conf
            attribution_tags.append("窄幅震荡观望")
        else:
            if composite_score >= 0.0:
                direction = "UP"
                dir_icon = "🚀"
            else:
                direction = "DOWN"
                dir_icon = "🔻"

            # 计算置信度 (Confidence %: 65% ~ 94%)
            intensity = min(1.0, abs(composite_score) / 0.7)
            confidence = round(base_conf + intensity * (94.0 - base_conf), 1)

            # 层级共振与逆势修正标注
            if parent_bias != 0.0:
                if (direction == "UP" and parent_bias < -0.15) or (direction == "DOWN" and parent_bias > 0.15):
                    is_counter_trend = True
                    dir_label = "超卖反弹 (逆势)" if direction == "UP" else "冲高回踩 (逆势)"
                    confidence = max(55.0, round(confidence - 7.0, 1))
                    attribution_tags.append("逆势微观修正")
                elif (direction == "UP" and parent_bias > 0.15) or (direction == "DOWN" and parent_bias < -0.15):
                    dir_label = "顺势看涨" if direction == "UP" else "顺势看跌"
                    confidence = min(94.0, round(confidence + 4.0, 1))
                    attribution_tags.append("多周期共振")
                else:
                    dir_label = "看涨" if direction == "UP" else "看跌"
            else:
                dir_label = "宏观偏多" if direction == "UP" else "宏观偏空"

        # 动态目标位置与结构失效线计算 (结合用户要求：5M>=8~10点, 1H>=30点, 1D>=60点)
        min_space_req = weights.get("min_directional_space", 8.0 if tf == "5m" else (30.0 if tf == "1h" else 60.0))
        cfg_min_tp1 = weights.get("min_tp1_dist", 6.0 if tf == "5m" else (18.0 if tf == "1h" else 35.0))
        cfg_min_tp2 = weights.get("min_tp2_dist", 10.0 if tf == "5m" else (30.0 if tf == "1h" else 60.0))

        min_tp1_dist = max(atr * k_tp1 * 0.70, cfg_min_tp1)
        min_tp2_step = max(round(atr * 0.40, 2), 3.5 if tf == "5m" else (10.0 if tf == "1h" else 20.0))
        min_sl_dist = max(round(atr * 0.50, 2), 3.0 if tf == "5m" else (10.0 if tf == "1h" else 22.0))

        vp = volume_profile or {}
        poc = safe_float(vp.get("poc"))
        vah = safe_float(vp.get("vah"))
        val = safe_float(vp.get("val"))
        rhigh = safe_float(vp.get("range_high"))
        rlow = safe_float(vp.get("range_low"))

        if direction == "UP":
            tp1_atr = price + atr * k_tp1
            # 底部接多核心锚定：若筹码核心 POC 位于现价上方且距离达标，POC 为极高概率均值回归第一目标
            if poc > 0 and poc >= price + min_tp1_dist:
                tp1 = round(poc, 2)
                attribution_tags.append("POC价值中枢锚定")
            else:
                tp_up_liq = liq_gravity.get("primary_tp_up")
                if tp_up_liq and (price + min_tp1_dist * 0.8) < tp_up_liq <= (price + atr * 1.5):
                    tp1 = round((tp1_atr * 0.4 + tp_up_liq * 0.6), 2)
                else:
                    tp1 = round(tp1_atr, 2)
            tp1 = max(tp1, round(price + min_tp1_dist, 2))

            tp2_atr = price + atr * k_tp2
            if vah > 0 and vah >= tp1 + min_tp2_step:
                tp2 = round(vah, 2)
                attribution_tags.append("VAH价值区顶锚定")
            else:
                tp2_liq = liq_gravity.get("secondary_tp_up")
                max_tp2_reach = price + atr * (k_tp2 + 0.6)
                if tp2_liq and (tp1 + 1.2) < tp2_liq <= max_tp2_reach:
                    tp2 = round((tp2_atr * 0.4 + tp2_liq * 0.6), 2)
                else:
                    tp2 = round(tp2_atr, 2)
            tp2 = min(tp2, round(price + atr * (k_tp2 * 1.4), 2))
            tp2 = max(tp2, round(tp1 + min_tp2_step, 2))
            tp2 = max(tp2, round(price + cfg_min_tp2, 2))

            # 结构失效线：底部接多紧贴支撑下沿防守，形成非对称高盈亏比
            support_floor = min(val, rlow) if (val > 0 and rlow > 0) else (price - min_sl_dist)
            buf = 0.5 if tf == "5m" else (2.5 if tf == "1h" else 8.0)
            sl_cand = round(support_floor - buf, 2)
            k_sl_eff = (k_sl * 0.70) if (tf == "5m" and is_floor_long) else k_sl
            sl_atr = round(price - atr * k_sl_eff, 2)
            sl = max(sl_atr, sl_cand)
            sl = min(sl, round(price - min_sl_dist, 2))

            target_range = f"{tp1:.2f} ~ {tp2:.2f}"
            expected_change_pct = round(((tp1 - price) / price) * 100.0, 2)

        elif direction == "DOWN":
            tp1_atr = price - atr * k_tp1
            # 顶部接空核心锚定：若筹码核心 POC 位于现价下方且距离达标，POC 为极高概率均值回归第一目标
            if poc > 0 and poc <= price - min_tp1_dist:
                tp1 = round(poc, 2)
                attribution_tags.append("POC价值中枢锚定")
            else:
                tp_down_liq = liq_gravity.get("primary_tp_down")
                if tp_down_liq and (price - min_tp1_dist * 0.8) > tp_down_liq >= (price - atr * 1.5):
                    tp1 = round((tp1_atr * 0.4 + tp_down_liq * 0.6), 2)
                else:
                    tp1 = round(tp1_atr, 2)
            tp1 = min(tp1, round(price - min_tp1_dist, 2))

            tp2_atr = price - atr * k_tp2
            if val > 0 and val <= tp1 - min_tp2_step:
                tp2 = round(val, 2)
                attribution_tags.append("VAL价值区底锚定")
            else:
                tp2_liq = liq_gravity.get("secondary_tp_down")
                min_tp2_reach = price - atr * (k_tp2 + 0.6)
                if tp2_liq and (tp1 - 1.2) > tp2_liq >= min_tp2_reach:
                    tp2 = round((tp2_atr * 0.4 + tp2_liq * 0.6), 2)
                else:
                    tp2 = round(tp2_atr, 2)
            tp2 = max(tp2, round(price - atr * (k_tp2 * 1.4), 2))
            tp2 = min(tp2, round(tp1 - min_tp2_step, 2))
            tp2 = min(tp2, round(price - cfg_min_tp2, 2))

            # 结构失效线：顶部接空紧贴阻力上沿防守，形成非对称高盈亏比
            resist_ceiling = max(vah, rhigh) if (vah > 0 and rhigh > 0) else (price + min_sl_dist)
            buf = 0.5 if tf == "5m" else (2.5 if tf == "1h" else 8.0)
            sl_cand = round(resist_ceiling + buf, 2)
            k_sl_eff = (k_sl * 0.70) if (tf == "5m" and is_ceiling_short) else k_sl
            sl_atr = round(price + atr * k_sl_eff, 2)
            sl = min(sl_atr, sl_cand)
            sl = max(sl, round(price + min_sl_dist, 2))

            target_range = f"{tp2:.2f} ~ {tp1:.2f}"
            expected_change_pct = round(((tp1 - price) / price) * 100.0, 2)
        else: # NEUTRAL
            tp1 = 0.0
            tp2 = 0.0
            sl = 0.0
            target_range = "--"
            expected_change_pct = 0.0

        # 空间门槛硬核核验 (5M>=8~10点, 1H>=30点, 1D>=60点)：
        # 若潜在波段空间达不到空间门槛，坚决判定为 NEUTRAL 观望，杜绝低盈亏比鸡肋交易！
        total_space = abs(tp2 - price) if direction in ("UP", "DOWN") else 0.0
        if direction in ("UP", "DOWN") and total_space < min_space_req:
            direction = "NEUTRAL"
            dir_icon = "⏸️"
            dir_label = f"空间受限观望 (<{min_space_req:.0f}点)"
            confidence = base_conf
            tp1 = 0.0
            tp2 = 0.0
            sl = 0.0
            target_range = "--"
            expected_change_pct = 0.0
            attribution_tags.append(f"空间受限(<{min_space_req:.0f}点)")

        # 核心驱动因子归因标签与详细诊断
        attribution_detail = []

        # 宏观突发事件融入
        if macro_events:
            ev_tags = macro_events.get("event_tags", [])
            for t in ev_tags[:2]:
                if t not in attribution_tags:
                    attribution_tags.append(t)
            ev_summary = macro_events.get("summary")
            if ev_summary:
                attribution_detail.append(ev_summary)

        if vwap_daily:
            z = vwap_daily.get("z_score", 0.0)
            if abs(z) > 1.5:
                attribution_tags.append(f"VWAP偏离{z:+.1f}σ")
                attribution_detail.append(f"价格偏离日内VWAP达 {z:+.1f} 倍标准差，存在{'回踩' if z>0 else '超跌反弹'}需求")
            else:
                attribution_tags.append("VWAP均值带内")

        if oi_info.get("regime") != "CONSOLIDATION":
            attribution_tags.append(oi_info.get("regime"))
            attribution_detail.append(oi_info.get("text", ""))
        else:
            attribution_tags.append("OI均衡")

        if pos_info.get("top_ls_ratio", 1.0) > 1.25:
            attribution_tags.append(f"大户看多({pos_info['top_ls_ratio']:.2f})")
            attribution_detail.append(f"大户多空持仓比达 {pos_info['top_ls_ratio']:.2f}，聪明钱底仓偏多")
        elif pos_info.get("top_ls_ratio", 1.0) < 0.85:
            attribution_tags.append(f"大户看空({pos_info['top_ls_ratio']:.2f})")
            attribution_detail.append(f"大户多空持仓比为 {pos_info['top_ls_ratio']:.2f}，机构空单占优")

        if liq_gravity.get("net_direction") != "NEUTRAL":
            liq_tag = f"清算磁吸{'上方' if liq_gravity['net_direction']=='BULLISH' else '下方'}"
            attribution_tags.append(liq_tag)
            attribution_detail.append(f"清算引力净偏向 {liq_gravity['net_direction']}，吸引价格奔赴爆仓池")

        # BTC 领先先行与爆仓冲击归因标签
        if btc_lead_lag:
            lead_sig = btc_lead_lag.get("lead_signal", "SYNCHRONIZED")
            div_pct = safe_float(btc_lead_lag.get("divergence_pct", 0.0))
            if lead_sig == "BULLISH_CATCHUP":
                attribution_tags.append(f"BTC领先补涨(+{div_pct:+.2f}%)")
                attribution_detail.append(f"BTC率先放量拉升，剪刀差达 +{div_pct:.2f}%，提供多头补涨溢出")
            elif lead_sig == "BEARISH_DRAG":
                attribution_tags.append(f"BTC领先拖拽({div_pct:+.2f}%)")
                attribution_detail.append(f"BTC率先放量跳水，剪刀差达 {div_pct:.2f}%，对ETH形成向下拖拽")

        if realtime_liquidations:
            rt_cascade = bool(realtime_liquidations.get("cascade_alert", False))
            rt_bias = safe_float(realtime_liquidations.get("net_bias", 0.0))
            rt_total = safe_float(realtime_liquidations.get("total_liq_vol_eth", 0.0))
            if rt_cascade:
                if rt_bias > 0:
                    attribution_tags.append(f"空头逼空强平({rt_total:.0f}E)")
                    attribution_detail.append(f"监测到空头连续强平逼空，累计爆仓 {rt_total:.0f} ETH")
                else:
                    attribution_tags.append(f"多头踩踏强平({rt_total:.0f}E)")
                    attribution_detail.append(f"监测到多头连续强平踩踏，累计爆仓 {rt_total:.0f} ETH")

        # LightGBM 微观特征交互层定性归因 (限定在特征层输出，辅助归因分析)
        if feature_cross:
            cg_tag = feature_cross.get("cross_regime_tag", "BALANCED")
            if cg_tag and cg_tag != "BALANCED":
                cg_map = {
                    "SQUEEZE_LONG": "GBM动量多头共振",
                    "SQUEEZE_SHORT": "GBM动量空头共振",
                    "ABSORPTION_LONG": "GBM底部暗流吸收",
                    "ABSORPTION_SHORT": "GBM顶部暗流派发",
                    "VOLATILITY_EXPANSION": "GBM波动非线性扩张"
                }
                attribution_tags.append(cg_map.get(cg_tag, f"GBM:[{cg_tag}]"))
            cg_summary = feature_cross.get("interaction_summary")
            if cg_summary:
                attribution_detail.append(cg_summary)

        # 生成多情景走势概率树 (Scenario Probability Tree: 主路径 / 洗盘反抽 / 破位失效)
        scenario_tree = generate_scenario_tree(
            tf=tf,
            direction=direction,
            base_price=price,
            tp1=tp1,
            tp2=tp2,
            sl=sl,
            atr=atr,
            market_regime=market_regime,
            bb_bands=bb_bands,
            pivot_levels=pivot_levels,
            liq_gravity=liq_gravity,
            btc_lead_lag=btc_lead_lag,
            confidence=confidence
        )

        return {
            "pred_id": pred_id,
            "timeframe": tf,
            "created_ts": created_ts,
            "created_iso": created_iso,
            "expiry_ts": created_ts + expiry_seconds,
            "base_price": price,
            "direction": direction,
            "dir_label": dir_label,
            "dir_icon": dir_icon,
            "composite_score": round(composite_score, 3),
            "confidence": confidence,
            "tp1": tp1,
            "tp2": tp2,
            "sl": sl,
            "target_range": target_range,
            "expected_change_pct": expected_change_pct,
            "atr": atr,
            "attribution_tags": attribution_tags[:4],
            "attribution_detail": "；".join(attribution_detail) if attribution_detail else "多空技术指标均衡，区间震荡整固",
            "status": "PENDING",  # PENDING, VERIFIED
            "verified_result": None,
            "market_regime": market_regime,
            "scenario_tree": scenario_tree,
            "btc_lead_lag": btc_lead_lag,
            "realtime_liquidations": realtime_liquidations,
            "oi_info": oi_info,
            "pos_info": pos_info,
            "liq_gravity": liq_gravity,
            "vwap_daily": vwap_daily,
            "volume_profile": vp,
            "feature_cross": feature_cross
        }

    def build_forced_prediction(self, tf, direction, price, market_snapshot):
        """在发生极端单边突变 (如8.19式大暴拉/大暴跌) 时直接构建高置信度的反向单边预测"""
        all_preds = self.predict(market_snapshot)
        cand = all_preds.get(tf)
        if cand and cand.get("direction") == direction:
            return cand

        kl_tf = market_snapshot.get(f"klines_{tf}", market_snapshot.get("klines_5m", []))
        atr = calc_atr(kl_tf, 14) if kl_tf else 5.0
        w = self.weights.get(tf, self.weights["5m"])
        k_tp1 = w.get("atr_tp1_mult", 0.6)
        k_tp2 = w.get("atr_tp2_mult", 1.2)
        k_sl = w.get("atr_sl_mult", 1.5)
        now_ts = int(datetime.now().timestamp())
        now_iso = datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")

        if direction == "UP":
            tp1 = round(price + atr * k_tp1, 2)
            tp2 = round(price + atr * k_tp2, 2)
            sl = round(price - atr * k_sl, 2)
            dir_label = "⚡ 极端暴涨动能反转追多"
            dir_icon = "🚀"
        else:
            tp1 = round(price - atr * k_tp1, 2)
            tp2 = round(price - atr * k_tp2, 2)
            sl = round(price + atr * k_sl, 2)
            dir_label = "⚡ 极端暴跌破位反转追空"
            dir_icon = "🔻"

        return {
            "pred_id": f"pred_{tf}_{now_ts}_{uuid.uuid4().hex[:6]}",
            "timeframe": tf,
            "created_ts": now_ts,
            "created_iso": now_iso,
            "base_price": price,
            "direction": direction,
            "dir_label": dir_label,
            "dir_icon": dir_icon,
            "composite_score": 0.45 if direction == "UP" else -0.45,
            "confidence": 85.0,
            "tp1": tp1,
            "tp2": tp2,
            "sl": sl,
            "target_range": f"{tp1:.2f} ~ {tp2:.2f}" if direction == "UP" else f"{tp2:.2f} ~ {tp1:.2f}",
            "expected_change_pct": round(((tp1 - price) / price) * 100.0, 2),
            "atr": atr,
            "attribution_tags": ["极端动能反转", "8.19大单边动能模式"],
            "attribution_detail": "盘口检测到极端单边爆发动力（逆向超幅大单边），系统触发直接反向反转策略",
            "status": "ACTIVE",
            "scenario_tree": None,
            "market_regime": {"regime": "EXPLOSIVE_TREND", "label": "极端爆发大单边"},
        }
