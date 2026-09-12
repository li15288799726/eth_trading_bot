# -*- coding: utf-8 -*-
"""
ETH 预测系统 · 多维量化微观指标计算引擎 (indicators.py)
======================================================
核心指标维度：
1. VWAP 体系：日内北京 00:00 Daily VWAP、±1σ/±2σ/±3σ 轨道、偏离度 (Z-Score)、VWAP 斜率
2. OI 与订单流体系：实时总持仓量、5m/1h OI Delta、Taker 买卖比、量价四象限形态 (真多/逼空/真砸/踩踏)
3. 持仓与大户情绪：大户持仓多空比、大户账户多空比、散户全局多空比、资金费率 (Funding Rate)
4. 清算地图与磁吸引力模型：上下多空爆仓密集带识别、清算引力积分 (Gravity Pull)、磁吸目标价提取
5. 扩展高阶指标：ATR (动态波动率与目标止盈位计算)、RSI (动能与超买超卖)、CVD (累计买卖量差背离)、
   布林带 (波动挤压突破)、EMA Ribbon (趋势共振滤网)、枢轴支撑阻力 (Pivot S/R)
"""
import math
from datetime import datetime, timezone, timedelta

TZ_BJT = timezone(timedelta(hours=8))


def safe_float(val, default=0.0):
    try:
        if val is None:
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def calc_rolling_quantiles(values, quantiles=(0.10, 0.25, 0.50, 0.75, 0.90)):
    """计算时序数值序列的自适应滚动分位数字典 (改动3: 替代硬编码常数)"""
    if not values:
        return {q: 0.0 for q in quantiles}
    cleaned = [safe_float(x) for x in values if x is not None]
    if not cleaned:
        return {q: 0.0 for q in quantiles}
    sorted_v = sorted(cleaned)
    n = len(sorted_v)
    res = {}
    for q in quantiles:
        idx = int(round(q * (n - 1)))
        idx = max(0, min(n - 1, idx))
        res[q] = sorted_v[idx]
    return res


def calc_adaptive_volume_regime(klines, period=30):
    """
    基于滚动窗口分位数与 Z-Score 计算动态成交量范式与弹性目标乘数 (改动3: 动态量比)
    """
    if not klines or len(klines) < 5:
        return {
            "vol_ratio": 1.0,
            "vol_z_score": 0.0,
            "vol_regime": "NORMAL",
            "target_multiplier": 1.0,
            "is_surge": False,
            "is_dry": False
        }

    recent_ks = klines[-period:] if len(klines) >= period else klines
    vols = [safe_float(k[5]) for k in recent_ks]
    cur_vol = vols[-1]
    hist_vols = vols[:-1] if len(vols) > 1 else vols

    avg_vol = sum(hist_vols) / len(hist_vols) if hist_vols else 1.0
    vol_ratio = round(cur_vol / max(0.01, avg_vol), 2)

    variance = sum((x - avg_vol) ** 2 for x in hist_vols) / len(hist_vols) if len(hist_vols) > 1 else 1.0
    std_vol = math.sqrt(variance) if variance > 0 else 1.0
    vol_z = round((cur_vol - avg_vol) / std_vol, 2) if std_vol > 0 else 0.0

    q_dict = calc_rolling_quantiles(hist_vols, (0.20, 0.50, 0.80))
    q20 = q_dict.get(0.20, avg_vol * 0.6)
    q80 = q_dict.get(0.80, avg_vol * 1.5)

    is_surge = (cur_vol >= q80 and vol_ratio >= 1.35) or vol_z >= 1.75
    is_dry = (cur_vol <= q20 or vol_ratio <= 0.65) and vol_z <= -0.75

    if is_surge:
        regime = "SURGE"
        mult = min(1.75, 1.0 + (vol_ratio - 1.0) * 0.3)
    elif is_dry:
        regime = "DRY"
        mult = max(0.70, 0.65 + vol_ratio * 0.3)
    else:
        regime = "NORMAL"
        mult = 1.0

    return {
        "vol_ratio": vol_ratio,
        "vol_z_score": vol_z,
        "vol_regime": regime,
        "target_multiplier": round(mult, 2),
        "is_surge": is_surge,
        "is_dry": is_dry,
        "q80": q80,
        "q20": q20
    }


def calc_adaptive_vwap_extremes(klines_5m, vwap_daily, period=72):
    """
    计算 VWAP 偏离度的滚动分位数与自适应极端边界 (动态替换硬编码的 ±1.6σ 和 ±2.2σ)

    AUDIT_FIX_ASOF_001:
      For each historical bar, z uses cumulative VWAP/σ as-of THAT bar's close
      (BJT day-start reset), never the final-T VWAP/σ applied retroactively.
    """
    cur_z = safe_float((vwap_daily or {}).get("z_score", 0.0))
    if not vwap_daily or not klines_5m or len(klines_5m) < 12:
        return {
            "upper_extreme_z": 2.0,
            "lower_extreme_z": -2.0,
            "upper_normal_z": 1.2,
            "lower_normal_z": -1.2,
            "current_z": cur_z,
            "is_overbought": abs(cur_z) >= 2.0,
            "is_oversold": cur_z <= -2.0
        }

    # Walk closed bars chronologically; at each bar compute day-cumulative VWAP/σ then z.
    all_z = []
    cur_day_start = None
    cum_vol = 0.0
    cum_tp_vol = 0.0
    kl_data = []

    for k in klines_5m:
        open_ms = int(safe_float(k[0]))
        dt = datetime.fromtimestamp(open_ms / 1000.0, tz=TZ_BJT)
        day_start_ms = int(datetime(dt.year, dt.month, dt.day, tzinfo=TZ_BJT).timestamp() * 1000)
        if cur_day_start != day_start_ms:
            cur_day_start = day_start_ms
            cum_vol = 0.0
            cum_tp_vol = 0.0
            kl_data = []

        h = safe_float(k[2])
        l = safe_float(k[3])
        c = safe_float(k[4])
        v = safe_float(k[5])
        tp = (h + l + c) / 3.0
        cum_vol += v
        cum_tp_vol += tp * v
        kl_data.append((tp, v))

        if cum_vol <= 0:
            all_z.append(0.0)
            continue
        vwap_i = cum_tp_vol / cum_vol
        sum_sq = sum(vv * ((ttp - vwap_i) ** 2) for ttp, vv in kl_data)
        sigma_i = math.sqrt(sum_sq / cum_vol) if cum_vol > 0 else 0.0
        if sigma_i <= 1e-12:
            all_z.append(0.0)
        else:
            all_z.append((c - vwap_i) / sigma_i)

    z_hist = all_z[-period:] if len(all_z) >= period else all_z
    if len(z_hist) < 12:
        return {
            "upper_extreme_z": 2.0,
            "lower_extreme_z": -2.0,
            "upper_normal_z": 1.2,
            "lower_normal_z": -1.2,
            "current_z": cur_z,
            "is_overbought": abs(cur_z) >= 2.0,
            "is_oversold": cur_z <= -2.0
        }

    q = calc_rolling_quantiles(z_hist, (0.08, 0.20, 0.80, 0.92))
    lower_extreme_z = round(min(-1.5, q.get(0.08, -1.8)), 2)
    upper_extreme_z = round(max(1.5, q.get(0.92, 1.8)), 2)
    lower_normal_z = round(min(-0.8, q.get(0.20, -1.0)), 2)
    upper_normal_z = round(max(0.8, q.get(0.80, 1.0)), 2)

    return {
        "upper_extreme_z": upper_extreme_z,
        "lower_extreme_z": lower_extreme_z,
        "upper_normal_z": upper_normal_z,
        "lower_normal_z": lower_normal_z,
        "current_z": cur_z,
        "is_overbought": cur_z >= upper_extreme_z,
        "is_oversold": cur_z <= lower_extreme_z
    }


def calc_adaptive_exhaustion(bb_bands, rsi):
    """
    根据布林带带宽与当前波动率，动态自适应判定超买/超卖耗竭门槛 (改动3: 替换硬编码的 RSI 62/38)
    - 窄幅震荡时 (Bandwidth < 2.5%): RSI 58 即可视为超买边界，RSI 42 为超卖
    - 宽幅单边时 (Bandwidth > 4.5%): 允许动能延展至 RSI 68 / 32
    """
    pct_b = safe_float(bb_bands.get("pct_b", 0.5)) if bb_bands else 0.5
    bw = safe_float(bb_bands.get("bandwidth_pct", 3.0)) if bb_bands else 3.0

    if bw < 2.2:
        rsi_ob = 58.0
        rsi_os = 42.0
        pct_b_ob = 0.82
        pct_b_os = 0.18
    elif bw > 4.5:
        rsi_ob = 68.0
        rsi_os = 32.0
        pct_b_ob = 0.92
        pct_b_os = 0.08
    else:
        rsi_ob = 62.0
        rsi_os = 38.0
        pct_b_ob = 0.86
        pct_b_os = 0.14

    is_exhausted_long = (pct_b >= pct_b_ob and rsi >= rsi_ob)
    is_exhausted_short = (pct_b <= pct_b_os and rsi <= rsi_os)

    penalty = 0.0
    if is_exhausted_long:
        penalty = -0.35
    elif is_exhausted_short:
        penalty = 0.35

    return {
        "is_exhausted_long": is_exhausted_long,
        "is_exhausted_short": is_exhausted_short,
        "penalty": penalty,
        "rsi_ob": rsi_ob,
        "rsi_os": rsi_os,
        "pct_b_ob": pct_b_ob,
        "pct_b_os": pct_b_os
    }


def calc_daily_vwap(klines_5m, tz_offset_hours=8, as_of_ms=None, price=None):
    """
    计算北京时间 00:00 起算的高精度日内 Daily VWAP 及其多层标准差轨道与斜率
    klines_5m: [[open_ms, o, h, l, c, vol, close_ms, q_vol, trades, taker_base, taker_quote], ...]

    AUDIT_FIX_ASOF_001:
      - Only closed bars (close_time <= as_of_ms) contribute H/L/C/V.
      - Accumulate from day 00:00 (BJT) to as_of T — never use end-of-window VWAP for earlier T.
    """
    if not klines_5m or len(klines_5m) < 3:
        return None

    from eth_predictor.asof import (
        filter_closed_klines, day_start_ms_bjt, now_ms,
    )

    t_ms = now_ms(as_of_ms)
    closed = filter_closed_klines(klines_5m, as_of_ms=t_ms, interval="5m")
    if len(closed) < 3:
        return None

    today_start_ms = day_start_ms_bjt(t_ms)
    today_ks = [k for k in closed if safe_float(k[0]) >= today_start_ms]
    if len(today_ks) < 12:
        # 如果当天开盘时间不足 1 小时 (例如凌晨刚开盘)，回退补充最近 48 根已收盘 5m K 线作为平滑过渡
        today_ks = closed[-48:]

    cum_vol = 0.0
    cum_tp_vol = 0.0
    kl_data = []

    # 记录 VWAP 随时间的演变，用于计算近期 VWAP 斜率
    vwap_series = []

    for k in today_ks:
        h = safe_float(k[2])
        l = safe_float(k[3])
        c = safe_float(k[4])
        v = safe_float(k[5])
        tp = (h + l + c) / 3.0
        cum_vol += v
        cum_tp_vol += tp * v
        cur_vwap = (cum_tp_vol / cum_vol) if cum_vol > 0 else c
        vwap_series.append(cur_vwap)
        kl_data.append((tp, v))

    if cum_vol <= 0:
        return None

    vwap = round(cum_tp_vol / cum_vol, 2)
    sum_sq = sum(v * ((tp - vwap) ** 2) for tp, v in kl_data)
    sigma = math.sqrt(sum_sq / cum_vol) if cum_vol > 0 else 1.0

    # z-score vs live/as-of price when provided; else last closed close
    current_price = safe_float(price) if price is not None else safe_float(today_ks[-1][4])
    if current_price <= 0:
        current_price = safe_float(today_ks[-1][4])
    z_score = round((current_price - vwap) / sigma, 3) if sigma > 0 else 0.0

    # 计算近 6 根 K 线 (30分钟) 的 VWAP 斜率
    slope = 0.0
    if len(vwap_series) >= 6:
        slope = round(vwap_series[-1] - vwap_series[-6], 2)

    u1 = round(vwap + sigma, 2)
    l1 = round(vwap - sigma, 2)
    u2 = round(vwap + 2 * sigma, 2)
    l2 = round(vwap - 2 * sigma, 2)
    u3 = round(vwap + 3 * sigma, 2)
    l3 = round(vwap - 3 * sigma, 2)

    return {
        "vwap": vwap,
        "sigma": round(sigma, 2),
        "z_score": z_score,
        "slope": slope,
        "u1": u1, "l1": l1,
        "u2": u2, "l2": l2,
        "u3": u3, "l3": l3,
        "mid_upper": round((u1 + u2) / 2.0, 2),
        "mid_lower": round((l1 + l2) / 2.0, 2),
        "n_candles": len(today_ks),
        "price": current_price,
        "as_of_ms": t_ms,
    }


def calc_atr(klines, period=14):
    """计算真实波幅均值 (Average True Range)"""
    if not klines or len(klines) < 2:
        return 5.0
    tr_list = []
    for i in range(1, len(klines)):
        h = safe_float(klines[i][2])
        l = safe_float(klines[i][3])
        prev_c = safe_float(klines[i - 1][4])
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        tr_list.append(tr)

    p = min(period, len(tr_list))
    if p == 0:
        return 5.0
    atr = sum(tr_list[-p:]) / p
    return round(atr, 2)


def calc_rsi(klines, period=14):
    """计算相对强弱指标 RSI"""
    if not klines or len(klines) <= period:
        return 50.0
    closes = [safe_float(k[4]) for k in klines]
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))

    if len(gains) < period:
        return 50.0

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(rsi, 2)


def calc_bollinger_bands(klines, period=20, num_std=2.0):
    """计算布林带 (Bollinger Bands) 与带宽挤压指标"""
    if not klines or len(klines) < period:
        c = safe_float(klines[-1][4]) if klines else 2500.0
        return {"mb": c, "ub": c + 20, "lb": c - 20, "bandwidth_pct": 1.0, "pct_b": 0.5}

    closes = [safe_float(k[4]) for k in klines[-period:]]
    mb = sum(closes) / len(closes)
    variance = sum((x - mb) ** 2 for x in closes) / len(closes)
    std = math.sqrt(variance)

    ub = mb + num_std * std
    lb = mb - num_std * std
    cur_c = closes[-1]
    bandwidth = ((ub - lb) / mb * 100.0) if mb > 0 else 0.0
    pct_b = ((cur_c - lb) / (ub - lb)) if (ub - lb) > 0 else 0.5

    return {
        "mb": round(mb, 2),
        "ub": round(ub, 2),
        "lb": round(lb, 2),
        "bandwidth_pct": round(bandwidth, 3),
        "pct_b": round(pct_b, 3)
    }


def calc_ema(values, period):
    """计算指数移动平均线 EMA"""
    if not values:
        return 0.0
    k = 2.0 / (period + 1.0)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1.0 - k)
    return ema


def calc_ema_ribbon(klines, periods=(9, 21, 55)):
    """计算 EMA 均线彩带与多空排列共振得分 (支持完整 3 线与平滑回退)"""
    if not klines or len(klines) < 14:
        return {"ema9": 0.0, "ema21": 0.0, "ema55": 0.0, "alignment": "NEUTRAL", "score": 0.0}

    closes = [safe_float(k[4]) for k in klines]
    n = len(closes)
    active_periods = [p for p in periods if n >= p]
    if not active_periods or len(active_periods) < 2:
        return {"ema9": 0.0, "ema21": 0.0, "ema55": 0.0, "alignment": "NEUTRAL", "score": 0.0}

    ema_vals = {}
    for p in periods:
        if n >= p:
            ema_vals[f"ema{p}"] = round(calc_ema(closes[-p * 3:], p), 2)
        else:
            ema_vals[f"ema{p}"] = 0.0

    e9 = ema_vals.get("ema9", 0.0)
    e21 = ema_vals.get("ema21", 0.0)
    e55 = ema_vals.get("ema55", 0.0)

    if e55 > 0:
        if e9 > e21 > e55:
            alignment = "BULLISH_STACK"
            score = 1.0
        elif e9 < e21 < e55:
            alignment = "BEARISH_STACK"
            score = -1.0
        else:
            alignment = "CONVERGING"
            score = 0.0
    else:
        # 降级模式 (当样本满足 21 但不足 55 时采用双均线金叉/死叉判定)
        if e9 > e21:
            alignment = "BULLISH_CROSS"
            score = 0.6
        elif e9 < e21:
            alignment = "BEARISH_CROSS"
            score = -0.6
        else:
            alignment = "CONVERGING"
            score = 0.0

    return {
        "ema9": e9,
        "ema21": e21,
        "ema55": e55,
        "alignment": alignment,
        "score": score
    }


def calc_cvd(klines, lookback=24):
    """计算累计买卖量差 CVD (Cumulative Volume Delta) 与量价背离"""
    if not klines or len(klines) < 2:
        return {"cvd_delta": 0.0, "divergence": "NONE", "score": 0.0}

    recent = klines[-lookback:] if len(klines) >= lookback else klines
    deltas = []
    prices = []
    for k in recent:
        vol = safe_float(k[5]) if len(k) > 5 else 0.0
        taker_buy = safe_float(k[9]) if len(k) > 9 else (vol * 0.5)  # taker buy base asset volume
        taker_sell = vol - taker_buy
        delta = taker_buy - taker_sell
        deltas.append(delta)
        prices.append(safe_float(k[4]) if len(k) > 4 else 0.0)

    cvd_recent = sum(deltas)
    price_change = prices[-1] - prices[0]

    divergence = "NONE"
    score = 0.0
    if price_change < 0 and cvd_recent > 0:
        divergence = "BULLISH_DIV"  # 价格跌但主动买盘悄悄累积 -> 底背离
        score = 0.8
    elif price_change > 0 and cvd_recent < 0:
        divergence = "BEARISH_DIV"  # 价格涨但主动卖盘暗中抛售 -> 顶背离
        score = -0.8
    elif cvd_recent > 0:
        score = min(1.0, cvd_recent / 10000.0)
    else:
        score = max(-1.0, cvd_recent / 10000.0)

    return {
        "cvd_delta": round(cvd_recent, 2),
        "divergence": divergence,
        "score": round(score, 2)
    }


def calc_pivot_levels(klines):
    """基于近期行情计算经典枢轴支撑阻力位 (Pivot Point, S1-S3, R1-R3)"""
    if not klines or len(klines) < 12:
        return {}
    subset = klines[-48:]  # 过去 4 小时区间高低
    high = max(safe_float(k[2]) for k in subset)
    low = min(safe_float(k[3]) for k in subset)
    close = safe_float(subset[-1][4])

    pivot = (high + low + close) / 3.0
    r1 = 2 * pivot - low
    s1 = 2 * pivot - high
    r2 = pivot + (high - low)
    s2 = pivot - (high - low)
    r3 = high + 2 * (pivot - low)
    s3 = low - 2 * (high - pivot)

    return {
        "pivot": round(pivot, 2),
        "r1": round(r1, 2), "s1": round(s1, 2),
        "r2": round(r2, 2), "s2": round(s2, 2),
        "r3": round(r3, 2), "s3": round(s3, 2)
    }


def calc_volume_profile(klines, period=48, bins=30):
    """
    计算机构级筹码分布与拍卖市场价值区 (Volume Profile: POC, VAH, VAL)
    - POC (Point of Control): 筹码最密集的日内/波段核心公平价 (均值回归第一目标)
    - VAH (Value Area High): 70% 筹码成交价值区上沿 (强阻力位 / 顶部接空关键位)
    - VAL (Value Area Low): 70% 筹码成交价值区下沿 (强支撑位 / 底部接多关键位)
    """
    if not klines or len(klines) < 10:
        return {}

    subset = klines[-period:] if len(klines) >= period else klines
    highs = [safe_float(k[2]) for k in subset]
    lows = [safe_float(k[3]) for k in subset]

    min_p = min(lows)
    max_p = max(highs)
    if max_p <= min_p:
        return {}

    bin_width = (max_p - min_p) / float(bins)
    bin_vols = [0.0] * bins

    for k in subset:
        h = safe_float(k[2])
        l = safe_float(k[3])
        v = safe_float(k[5])
        b_start = max(0, min(bins - 1, int((l - min_p) / bin_width)))
        b_end = max(0, min(bins - 1, int((h - min_p) / bin_width)))
        cnt = b_end - b_start + 1
        for b in range(b_start, b_end + 1):
            bin_vols[b] += v / cnt

    max_v = max(bin_vols)
    poc_idx = bin_vols.index(max_v)
    poc_price = min_p + (poc_idx + 0.5) * bin_width
    total_vol = sum(bin_vols)
    target_va = total_vol * 0.70

    va_low_idx = poc_idx
    va_high_idx = poc_idx
    cur_vol = bin_vols[poc_idx]

    while cur_vol < target_va and (va_low_idx > 0 or va_high_idx < bins - 1):
        next_above = bin_vols[va_high_idx + 1] if va_high_idx < bins - 1 else -1.0
        next_below = bin_vols[va_low_idx - 1] if va_low_idx > 0 else -1.0
        if next_above >= next_below:
            va_high_idx += 1
            cur_vol += bin_vols[va_high_idx]
        else:
            va_low_idx -= 1
            cur_vol += bin_vols[va_low_idx]

    vah = round(min_p + (va_high_idx + 1) * bin_width, 2)
    val = round(min_p + va_low_idx * bin_width, 2)
    poc = round(poc_price, 2)

    return {
        "poc": poc,
        "vah": vah,
        "val": val,
        "range_high": round(max_p, 2),
        "range_low": round(min_p, 2)
    }


def calc_oi_matrix(oi_current, oi_delta_5m, oi_delta_1h=0.0, price_change_5m=0.0, price_change_1h=0.0, tf="5m", oi_delta_1d=0.0, price_change_1d=0.0):
    """
    量价与持仓量 (OI) 四象限多周期动力学：
    支持 5m、1h 与 1d 专属量仓敏感度、真实周期跨度对齐与动态归因描述
    """
    if tf == "1h":
        dp = price_change_1h
        doi = oi_delta_1h
        oi_thresh = 1500.0   # 1 小时内 1500 ETH 变化作为显著阈值
        price_thresh = 2.5   # 2.5 USDT 变化作为 1h 方向基准 (提升抗噪度)
        scale_denom = 15000.0
        period_name = "1小时"
    elif tf == "1d":
        dp = price_change_1d if price_change_1d != 0.0 else price_change_1h
        doi = oi_delta_1d if oi_delta_1d != 0.0 else oi_delta_1h
        oi_thresh = 5000.0   # 跨日机构级 5000 ETH 宏观持仓壁垒
        price_thresh = 8.0   # 8.0 USDT 变化作为日线显著方向基准 (杜绝微观噪点绑架宏观)
        scale_denom = 40000.0
        period_name = "日线级别"
    else:
        # 默认 5m 微观
        dp = price_change_5m
        doi = oi_delta_5m
        oi_thresh = 500.0
        price_thresh = 0.6
        scale_denom = 5000.0
        period_name = "5分钟"

    if abs(doi) < oi_thresh and abs(dp) < price_thresh:
        regime = "CONSOLIDATION"
        signal = 0.0
        text = f"{period_name}量仓中性均衡震荡，主力观望"
    elif dp > price_thresh and doi >= oi_thresh:
        regime = "BULL_ATTACK"
        signal = min(1.0, 0.4 + (doi / scale_denom))
        text = f"{period_name}多头主动增仓强攻 (ΔOI +{doi:.0f} ETH)，主力真金白银建多"
    elif dp > price_thresh and doi <= -oi_thresh:
        regime = "SHORT_SQUEEZE"
        # 1H/1D 级别空头平仓往往为诱空后的被动回抽，动能有限，审慎赋分
        signal = 0.15 if tf in ("1h", "1d") else 0.30
        text = f"{period_name}空头平仓回补推高 (ΔOI {doi:.0f} ETH)，被动抽升谨防追高"
    elif dp < -price_thresh and doi >= oi_thresh:
        regime = "BEAR_ATTACK"
        signal = -min(1.0, 0.4 + (abs(doi) / scale_denom))
        text = f"{period_name}空头主动增仓砸盘 (ΔOI +{doi:.0f} ETH)，主力大单打压"
    elif dp < -price_thresh and doi <= -oi_thresh:
        regime = "LONG_FLUSH"
        # 1H/1D 级别多头爆仓踩踏出清后，常伴随抛压耗竭
        signal = -0.15 if tf in ("1h", "1d") else -0.30
        text = f"{period_name}多头踩踏平仓出清 (ΔOI {doi:.0f} ETH)，被动清算谨防杀跌"
    else:
        regime = "CONSOLIDATION"
        signal = 0.0
        text = f"{period_name}量价无明显背离，中性震荡"

    return {
        "regime": regime,
        "signal": round(signal, 3),
        "text": text,
        "oi_current": oi_current,
        "oi_delta_5m": oi_delta_5m,
        "oi_delta_1h": oi_delta_1h,
        "timeframe": tf
    }


def calc_positioning_sentiment(top_ls_ratio, top_acc_ratio, global_ls_ratio, taker_ratio, funding_rate, top_ls_baseline=1.25):
    """
    大户持仓、散户人数比、吃单偏好与资金费率综合多空情绪模型
    支持动态自适应基准 (top_ls_baseline)，消除常态化看多偏见
    """
    top_score = 0.0
    if top_ls_ratio > 0:
        base = max(0.5, safe_float(top_ls_baseline, 1.25))
        top_centered = top_ls_ratio / base
        top_score = (math.log(top_centered) / math.log(1.4))
        top_score = max(-1.0, min(1.0, top_score))

    contrarian_score = 0.0
    if global_ls_ratio > 0:
        # 散户人数多空比对手盘：加密市场正常态散户略偏多 (1.5 ~ 2.2)
        # 仅当散户极端盲目追多 (> 2.4) 或极度恐慌割肉 (< 0.8) 时触发逆向调节
        if global_ls_ratio > 2.4:
            contrarian_score = -min(0.5, (global_ls_ratio - 2.4) * 0.5)
        elif global_ls_ratio < 0.8:
            contrarian_score = min(0.5, (0.8 - global_ls_ratio) * 0.8)

    taker_score = 0.0
    if taker_ratio > 0:
        taker_score = (math.log(taker_ratio) / math.log(1.5))
        taker_score = max(-1.0, min(1.0, taker_score))

    fr_score = 0.0
    if funding_rate > 0.0003:
        fr_score = -0.4
    elif funding_rate < -0.0001:
        fr_score = 0.4

    composite = round(0.45 * top_score + 0.35 * taker_score + 0.1 * contrarian_score + 0.1 * fr_score, 3)

    return {
        "top_ls_ratio": top_ls_ratio,
        "global_ls_ratio": global_ls_ratio,
        "taker_ratio": taker_ratio,
        "funding_rate": funding_rate,
        "composite_score": composite,
        "sentiment_label": "偏多" if composite > 0.15 else ("偏空" if composite < -0.15 else "中性")
    }


def calc_liquidation_gravity(current_price, liq_raw_data, gamma=1.2):
    """
    清算地图重力引力模型 (Liquidation Gravitational Attraction Engine)
    AUDIT_FIX_ASOF_001: if snapshot disabled / missing data list, return neutral (no invented future).
    """
    if not current_price or not liq_raw_data or liq_raw_data.get("_liq_disabled") or not isinstance(liq_raw_data.get("data"), list):
        return {
            "net_direction": "NEUTRAL",
            "net_score": 0.0,
            "up_pull": 0.0,
            "down_pull": 0.0,
            "primary_tp_up": current_price + 15.0 if current_price else 2520.0,
            "primary_tp_down": current_price - 15.0 if current_price else 2480.0,
            "top_short_clusters": [],
            "top_long_clusters": []
        }

    agg = {}
    for ex in liq_raw_data.get("data", []):
        for p_str, rows in (ex.get("liqMapV2") or {}).items():
            p = safe_float(p_str)
            vol = sum(safe_float(r[1]) for r in rows) if isinstance(rows, list) else 0.0
            agg[p] = agg.get(p, 0.0) + vol

    long_clusters = {p: v for p, v in agg.items() if p < current_price and v > 0}
    short_clusters = {p: v for p, v in agg.items() if p > current_price and v > 0}

    up_pull = 0.0
    for p, v in short_clusters.items():
        dist = max(1.0, abs(p - current_price))
        up_pull += v / (dist ** gamma)

    down_pull = 0.0
    for p, v in long_clusters.items():
        dist = max(1.0, abs(current_price - p))
        down_pull += v / (dist ** gamma)

    total_pull = up_pull + down_pull
    if total_pull > 0:
        net_score = (up_pull - down_pull) / total_pull
    else:
        net_score = 0.0

    # 筛选显著清算密集区并按离现价由近到远阶梯排序
    top_vol_shorts = sorted(short_clusters.items(), key=lambda x: x[1], reverse=True)[:10]
    top_vol_longs = sorted(long_clusters.items(), key=lambda x: x[1], reverse=True)[:10]

    # 上方空头清算池：按价格升序 (由近及远阶梯推进)
    sorted_shorts_by_dist = sorted(top_vol_shorts, key=lambda x: x[0])
    # 下方多头清算池：按价格降序 (由近及远阶梯推进)
    sorted_longs_by_dist = sorted(top_vol_longs, key=lambda x: -x[0])

    top_shorts = [{"price": p, "vol": round(v, 2), "dist": round(p - current_price, 1)} for p, v in sorted(short_clusters.items(), key=lambda x: x[1], reverse=True)[:5]]
    top_longs = [{"price": p, "vol": round(v, 2), "dist": round(current_price - p, 1)} for p, v in sorted(long_clusters.items(), key=lambda x: x[1], reverse=True)[:5]]

    if sorted_shorts_by_dist:
        primary_tp_up = sorted_shorts_by_dist[0][0]
        further_shorts = [x[0] for x in sorted_shorts_by_dist if x[0] > primary_tp_up + 5.0]
        secondary_tp_up = further_shorts[0] if further_shorts else round(primary_tp_up + 15.0, 2)
    else:
        primary_tp_up = round(current_price + 15.0, 2)
        secondary_tp_up = round(primary_tp_up + 15.0, 2)

    if sorted_longs_by_dist:
        primary_tp_down = sorted_longs_by_dist[0][0]
        further_longs = [x[0] for x in sorted_longs_by_dist if x[0] < primary_tp_down - 5.0]
        secondary_tp_down = further_longs[0] if further_longs else round(primary_tp_down - 15.0, 2)
    else:
        primary_tp_down = round(current_price - 15.0, 2)
        secondary_tp_down = round(primary_tp_down - 15.0, 2)

    net_dir = "BULLISH" if net_score > 0.12 else ("BEARISH" if net_score < -0.12 else "NEUTRAL")

    return {
        "net_direction": net_dir,
        "net_score": round(net_score, 3),
        "up_pull": round(up_pull, 1),
        "down_pull": round(down_pull, 1),
        "primary_tp_up": primary_tp_up,
        "secondary_tp_up": secondary_tp_up,
        "primary_tp_down": primary_tp_down,
        "secondary_tp_down": secondary_tp_down,
        "top_short_clusters": top_shorts,
        "top_long_clusters": top_longs
    }
