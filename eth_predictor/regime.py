# -*- coding: utf-8 -*-
"""
ETH 预测系统 · 市场范式状态机 (regime.py)
=========================================
核心功能：
1. 市场范式自动分类 (Market Regime Classification):
   - BULL_TREND: 单边强主升 (均线多头排列、CVD强劲买盘、VWAP上方顺势、考夫曼效率比高)
   - BEAR_TREND: 单边强主跌 (均线空头排列、CVD强劲卖盘、VWAP下方顺势、考夫曼效率比高)
   - RANGE_CONSOLIDATION: 箱体震荡与均值回归 (波动率收敛、价格贴合VWAP、布林带挤压、效率比低)
   - EXPANSION_WHIPSAW: 扩张剧烈洗盘与假突破 (ATR突增、布林带超宽、价格插针扫荡、量仓矛盾)
2. 动态权重与参数自适应调度 (Dynamic Adaptive Weights):
   - 单边趋势期: 权重 70%~80% 倾斜向趋势与订单流 (EMA Ribbon + CVD + OI Attack)，强制抑制逆势猜顶/抄底；
   - 震荡收敛期: 权重 70%~80% 倾斜向日内 VWAP 均值回归与通道边界 (VWAP + BB S/R)，压制假突破追涨杀跌；
   - 剧烈洗盘期: 放大目标带与失效容忍度，权重向清算密集池引力倾斜，防止被微观毛刺插针扫损。
"""
import math
from datetime import datetime, timezone, timedelta
from eth_predictor.indicators import safe_float

TZ_BJT = timezone(timedelta(hours=8))


def calc_kaufman_efficiency_ratio(klines, period=14):
    """
    计算考夫曼价格效率比 (Kaufman Efficiency Ratio, ER)：
    ER = |净方向位移| / 总价格运动路径
    - ER 接近 1.0: 纯单边极强趋势 (极少回撤，路径近乎直线)；
    - ER 接近 0.0: 纯无序锯齿震荡 (无方向性晃动)。
    """
    if not klines or len(klines) < period + 1:
        return 0.35

    subset = klines[-(period + 1):]
    closes = [safe_float(k[4]) for k in subset]

    net_change = abs(closes[-1] - closes[0])
    total_path = sum(abs(closes[i] - closes[i - 1]) for i in range(1, len(closes)))

    if total_path <= 0:
        return 0.0

    er = net_change / total_path
    return round(min(1.0, max(0.0, er)), 3)


def detect_market_regime(klines_5m, klines_1h, vwap_daily, cvd_5m, bb_5m, atr_5m, oi_info=None):
    """
    综合多周期量价特征，判定当前市场微观范式状态
    """
    price = safe_float(klines_5m[-1][4]) if klines_5m else 2500.0

    # 1. 考夫曼效率比 (5m 与 1h)
    er_5m = calc_kaufman_efficiency_ratio(klines_5m, 12)
    er_1h = calc_kaufman_efficiency_ratio(klines_1h, 12) if klines_1h else er_5m
    composite_er = er_5m * 0.6 + er_1h * 0.4

    # 2. 布林带宽度与挤压度
    bw = safe_float(bb_5m.get("bandwidth_pct", 1.0)) if bb_5m else 1.0
    pct_b = safe_float(bb_5m.get("pct_b", 0.5)) if bb_5m else 0.5

    # 3. 日内 VWAP 偏离与斜率
    z_score = safe_float(vwap_daily.get("z_score", 0.0)) if vwap_daily else 0.0
    vwap_slope = safe_float(vwap_daily.get("slope", 0.0)) if vwap_daily else 0.0

    # 4. CVD 买卖差与背离
    cvd_score = safe_float(cvd_5m.get("score", 0.0)) if cvd_5m else 0.0

    # 5. OI 量价四象限
    regime_oi = (oi_info.get("regime", "CONSOLIDATION") if oi_info else "CONSOLIDATION")

    # 判定核心逻辑
    regime = "RANGE_CONSOLIDATION"
    label = "箱体震荡 (均值回归)"
    trend_strength = round(composite_er, 2)
    icon = "⚖️"

    # 条件 A: 单边强主升
    # 效率比高、CVD多头主导、价格在VWAP上方且斜率向上、非空头增仓砸盘
    is_bull_trend = (
        (composite_er >= 0.52 and cvd_score >= 0.15 and z_score >= 0.4) or
        (regime_oi == "BULL_ATTACK" and z_score >= 0.6 and cvd_score > 0) or
        (er_5m >= 0.65 and cvd_score >= 0.20)
    )

    # 条件 B: 单边强主跌
    is_bear_trend = (
        (composite_er >= 0.52 and cvd_score <= -0.15 and z_score <= -0.4) or
        (regime_oi == "BEAR_ATTACK" and z_score <= -0.6 and cvd_score < 0) or
        (er_5m >= 0.65 and cvd_score <= -0.20)
    )

    # 条件 C: 剧烈洗盘/假突破 (扩张抽搐)
    # 布林带极宽 (bw > 2.2%) 或价格击穿外轨同时量价严重背离
    is_expansion_whipsaw = (
        (bw >= 2.0 and composite_er <= 0.35) or
        (atr_5m >= 7.5 and composite_er <= 0.30) or
        (abs(z_score) >= 2.4 and composite_er <= 0.38)
    )

    if is_bull_trend and not is_bear_trend:
        regime = "BULL_TREND"
        label = "单边强主升 (顺势冲刺)"
        icon = "🚀"
        trend_strength = max(0.65, min(0.98, composite_er + 0.2))
    elif is_bear_trend and not is_bull_trend:
        regime = "BEAR_TREND"
        label = "单边强主跌 (顺势下行)"
        icon = "🔻"
        trend_strength = max(0.65, min(0.98, composite_er + 0.2))
    elif is_expansion_whipsaw:
        regime = "EXPANSION_WHIPSAW"
        label = "剧烈洗盘 (高波抽搐)"
        icon = "⚡"
        trend_strength = round(composite_er * 0.7, 2)
    else:
        regime = "RANGE_CONSOLIDATION"
        label = "箱体震荡 (均值回归)"
        icon = "⚖️"
        trend_strength = min(0.45, composite_er)

    # -------------------------------------------------------------
    # 动态自适应权重系数调度 (Adaptive Weight Multipliers)
    # -------------------------------------------------------------
    if regime in ("BULL_TREND", "BEAR_TREND"):
        weight_multipliers = {
            "w_tech": 1.45,       # 均线动量与 CVD 大幅加权 (+45%)
            "w_oi": 1.35,         # 量仓主动进攻大幅加权 (+35%)
            "w_vwap": 0.55,       # 均值回归抑制 (-45%)，严禁摸顶抄底
            "w_pos": 0.90,        # 情绪中性
            "w_liq": 1.10,        # 清算磁吸顺势引导
            "allow_counter_trend": False, # 严禁逆势修正
            "confidence_boost": 4.0,      # 单边共振置信度红利
            "tp_multiplier_boost": 1.20   # 顺势目标带适当延伸 (+20%)
        }
    elif regime == "RANGE_CONSOLIDATION":
        weight_multipliers = {
            "w_tech": 0.65,       # 动量指标降权 (-35%)，防追涨杀跌
            "w_oi": 0.85,         # 量仓微弱
            "w_vwap": 1.55,       # 日内 VWAP 均值回归大幅加权 (+55%)
            "w_pos": 1.20,        # 大户与散户情绪对手盘加权
            "w_liq": 1.30,        # 清算密集区作为强支撑/阻力位
            "allow_counter_trend": True,  # 允许箱体边界高抛低吸
            "confidence_boost": -2.0,     # 震荡市审慎保守
            "tp_multiplier_boost": 0.85   # 目标带收敛在箱体内 (-15%)
        }
    else: # EXPANSION_WHIPSAW
        weight_multipliers = {
            "w_tech": 0.70,       # 均线常态失效
            "w_oi": 1.10,
            "w_vwap": 0.80,
            "w_pos": 1.00,
            "w_liq": 1.60,        # 洗盘本质是掠夺清算流动性，清算引力权重大增 (+60%)
            "allow_counter_trend": False,
            "confidence_boost": -4.0,     # 剧烈洗盘期降低预测置信度防诱导
            "tp_multiplier_boost": 1.35   # 宽幅波幅下防守与目标外扩
        }

    return {
        "regime": regime,
        "label": label,
        "icon": icon,
        "trend_strength": trend_strength,
        "efficiency_ratio_5m": er_5m,
        "efficiency_ratio_1h": er_1h,
        "bandwidth_pct": bw,
        "weight_multipliers": weight_multipliers,
        "evaluated_at": datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S")
    }
