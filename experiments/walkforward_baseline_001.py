#!/usr/bin/env python3
"""BASELINE_EVALUATION_001 — Walk-Forward OOS on AUDIT_FIX_ASOF_001 (2660ac2).

Read-only evaluation harness. Does not modify prediction weights / UP-DOWN-NEUTRAL defs.
Features honor as_of_ms; liquidation map disabled without PIT snapshots; macro disabled historically.
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from eth_predictor.models import ETHPredictor  # noqa: E402
from eth_predictor.asof import filter_closed_klines, now_ms  # noqa: E402

EXPERIMENT_ID = "BASELINE_EVALUATION_001"
CODE_VERSION = "2660ac2 / v2.5.2-asof-AUDIT_FIX_ASOF_001"
BASELINE_VERSION = "NONE (establishing first baseline)"

FAPI = "https://fapi.binance.com"
FDATA = "https://fapi.binance.com/futures/data"

MIN_SPACE = {"5m": 8.0, "1h": 30.0, "1d": 60.0}
HORIZON_MS = {"5m": 5 * 60 * 1000, "1h": 60 * 60 * 1000, "1d": 24 * 60 * 60 * 1000}
INTERVAL_MS = {"5m": 5 * 60 * 1000, "1h": 60 * 60 * 1000, "1d": 24 * 60 * 60 * 1000}


def http_get(url: str, params: dict, retries: int = 4) -> Any:
    qs = urllib.parse.urlencode(params)
    full = f"{url}?{qs}"
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(full, headers={"User-Agent": "ETH-PREDICTOR-baseline/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            last = e
            time.sleep(0.4 * (i + 1))
    raise RuntimeError(f"GET failed {full}: {last}")


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = 1500) -> List[list]:
    out: List[list] = []
    cursor = start_ms
    while cursor < end_ms:
        batch = http_get(f"{FAPI}/fapi/v1/klines", {
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": limit,
        })
        if not batch:
            break
        out.extend(batch)
        last_open = int(batch[-1][0])
        nxt = last_open + INTERVAL_MS[interval]
        if nxt <= cursor:
            break
        cursor = nxt
        if len(batch) < limit:
            break
        time.sleep(0.05)
    # dedupe by open time
    seen = {}
    for k in out:
        seen[int(k[0])] = k
    return [seen[k] for k in sorted(seen)]


def fetch_hist(path: str, symbol: str, period: str, start_ms: int, end_ms: int) -> List[dict]:
    out: List[dict] = []
    cursor = start_ms
    while cursor < end_ms:
        batch = http_get(f"{FDATA}/{path}", {
            "symbol": symbol,
            "period": period,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 500,
        })
        if not batch:
            break
        out.extend(batch)
        last_ts = int(batch[-1]["timestamp"])
        nxt = last_ts + INTERVAL_MS.get(period, 5 * 60 * 1000)
        if nxt <= cursor:
            break
        cursor = nxt
        if len(batch) < 500:
            break
        time.sleep(0.08)
    seen = {}
    for r in out:
        seen[int(r["timestamp"])] = r
    return [seen[k] for k in sorted(seen)]


def to_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def close_at_or_before(klines: List[list], t_ms: int) -> Optional[float]:
    """Last closed bar with close_time <= t_ms."""
    best = None
    for k in klines:
        ct = int(k[6])
        if ct <= t_ms:
            best = to_float(k[4])
        else:
            break
    return best


def close_at_or_after_horizon(klines: List[list], t_ms: int, horizon_ms: int) -> Optional[Tuple[float, int]]:
    """Close of first bar whose close_time >= t_ms + horizon (or last available at/after target)."""
    target = t_ms + horizon_ms
    for k in klines:
        ct = int(k[6])
        if ct >= target:
            return to_float(k[4]), ct
    # fallback: last bar if it ends after t
    if klines and int(klines[-1][6]) > t_ms:
        return to_float(klines[-1][4]), int(klines[-1][6])
    return None


def actual_label(move: float, min_space: float) -> str:
    if abs(move) < min_space:
        return "FLAT"
    return "UP" if move > 0 else "DOWN"


def map_pred_dir(d: str) -> str:
    if d == "NEUTRAL":
        return "FLAT"
    return d


@dataclass
class PredRow:
    tf: str
    as_of_ms: int
    base_price: float
    pred_dir: str  # UP/DOWN/FLAT
    tp1: float
    actual_dir: str
    actual_close: float
    target_err_pct: Optional[float]
    window_id: str
    split: str  # IS or OOS


def build_micro_at(t_ms: int, oi_hist: List[dict], top_ls: List[dict], global_ls: List[dict],
                   taker: List[dict], eth_5m: List[list], funding_hist: List[dict]) -> dict:
    def last_le(rows, key="timestamp"):
        best = None
        for r in rows:
            ts = int(r[key])
            if ts + 5 * 60 * 1000 - 1 <= t_ms:
                best = r
            else:
                break
        return best

    oi = last_le(oi_hist)
    def oi_at_offset(offset_ms):
        target = t_ms - offset_ms
        best = None
        for r in oi_hist:
            ts = int(r["timestamp"])
            if ts + 5 * 60 * 1000 - 1 <= target:
                best = r
            else:
                break
        return best

    oi_now = to_float(oi.get("sumOpenInterest")) if oi else 0.0
    oi_5 = oi_at_offset(5 * 60 * 1000)
    oi_1h = oi_at_offset(60 * 60 * 1000)
    oi_1d = oi_at_offset(24 * 60 * 60 * 1000)
    oi_5v = to_float(oi_5.get("sumOpenInterest")) if oi_5 else oi_now
    oi_1hv = to_float(oi_1h.get("sumOpenInterest")) if oi_1h else oi_now
    oi_1dv = to_float(oi_1d.get("sumOpenInterest")) if oi_1d else oi_now

    tls = last_le(top_ls)
    gls = last_le(global_ls)
    tk = last_le(taker)
    buy = to_float(tk.get("buyVol")) if tk else 0.0
    sell = to_float(tk.get("sellVol")) if tk else 0.0
    bs = (buy / sell) if sell > 0 else 1.0

    return {
        "oi_current": oi_now,
        "oi_delta_5m": oi_now - oi_5v,
        "oi_delta_1h": oi_now - oi_1hv,
        "oi_delta_1d": oi_now - oi_1dv,
        "vol_ratio": vol_ratio_at(t_ms, eth_5m),
        "buy_sell_ratio_5m": bs,
        "top_ls_ratio": to_float(tls.get("longShortRatio"), 1.0) if tls else 1.0,
        "global_ls_ratio": to_float(gls.get("longShortRatio"), 1.0) if gls else 1.0,
        "funding_rate": funding_at(t_ms, funding_hist),
    }


def btc_lead_at(t_ms: int, eth_5m: List[list], btc_5m: List[list]) -> dict:
    """Mirror binance_futures_feed.get_btc_lead_lag_stats historical path."""
    eth = filter_closed_klines(eth_5m, as_of_ms=t_ms, interval="5m")
    btc = filter_closed_klines(btc_5m, as_of_ms=t_ms, interval="5m")
    btc_px = to_float(btc[-1][4]) if btc else 0.0
    eth_px = to_float(eth[-1][4]) if eth else 0.0
    if not btc_px or not eth_px:
        return {
            "btc_price": btc_px or 0.0,
            "eth_price": eth_px or 0.0,
            "btc_change_5m_pct": 0.0,
            "eth_change_5m_pct": 0.0,
            "divergence_pct": 0.0,
            "lead_signal": "SYNCHRONIZED",
            "spillover_score": 0.0,
            "status_text": "BTC 先导数据采集中",
            "as_of_ms": t_ms,
        }
    btc_change_5m = 0.0
    if len(btc) >= 2:
        prev_btc = to_float(btc[-2][4])
        if prev_btc > 0:
            btc_change_5m = round(((btc_px - prev_btc) / prev_btc) * 100.0, 3)
    elif btc:
        open_btc = to_float(btc[-1][1])
        if open_btc > 0:
            btc_change_5m = round(((btc_px - open_btc) / open_btc) * 100.0, 3)
    eth_change_5m = 0.0
    if len(eth) >= 2:
        prev_eth = to_float(eth[-2][4])
        if prev_eth > 0:
            eth_change_5m = round(((eth_px - prev_eth) / prev_eth) * 100.0, 3)
    elif eth:
        open_eth = to_float(eth[-1][1])
        if open_eth > 0:
            eth_change_5m = round(((eth_px - open_eth) / open_eth) * 100.0, 3)
    div = round(btc_change_5m - eth_change_5m, 3)
    spillover_score = max(-1.0, min(1.0, round(div / 0.45, 3)))
    if div >= 0.18 and btc_change_5m > 0.12:
        signal = "BULLISH_CATCHUP"
    elif div <= -0.18 and btc_change_5m < -0.12:
        signal = "BEARISH_DRAG"
    elif btc_change_5m > 0.30 and eth_change_5m > 0.25:
        signal = "BULLISH_RESONANCE"
    elif btc_change_5m < -0.30 and eth_change_5m < -0.25:
        signal = "BEARISH_RESONANCE"
    else:
        signal = "SYNCHRONIZED"
    return {
        "btc_price": round(btc_px, 2),
        "eth_price": round(eth_px, 2),
        "btc_change_5m_pct": btc_change_5m,
        "eth_change_5m_pct": eth_change_5m,
        "divergence_pct": div,
        "lead_signal": signal,
        "spillover_score": spillover_score,
        "status_text": f"BTC {btc_change_5m:+.2f}% / ETH {eth_change_5m:+.2f}%",
        "as_of_ms": t_ms,
    }


def vol_ratio_at(t_ms: int, eth_5m: List[list]) -> float:
    closed = filter_closed_klines(eth_5m, as_of_ms=t_ms, interval="5m")
    if len(closed) >= 20:
        vols = [to_float(item[5]) for item in closed[-20:]]
        ma = sum(vols) / len(vols) if vols else 1.0
        cur = to_float(closed[-1][5])
        return round(cur / ma, 2) if ma > 0 else 1.0
    if len(closed) >= 2:
        vols = [to_float(item[5]) for item in closed[:-1]]
        ma = (sum(vols) / len(vols)) if vols else 1.0
        cur = to_float(closed[-1][5])
        return round(cur / ma, 2) if ma > 0 else 1.0
    return 1.0


def funding_at(t_ms: int, funding_hist: List[dict]) -> float:
    best = None
    for r in funding_hist:
        ft = int(r.get("fundingTime") or r.get("timestamp") or 0)
        if ft <= t_ms:
            best = r
        else:
            break
    if not best:
        return 0.0001
    return round(to_float(best.get("fundingRate"), 0.0001), 6)


def make_snapshot(t_ms: int, eth5, eth1h, eth1d, btc5, micro, btc_lead) -> dict:
    kl5 = filter_closed_klines(eth5, as_of_ms=t_ms, interval="5m")
    kl1h = filter_closed_klines(eth1h, as_of_ms=t_ms, interval="1h")
    kl1d = filter_closed_klines(eth1d, as_of_ms=t_ms, interval="1d")
    price = to_float(kl5[-1][4]) if kl5 else 0.0
    return {
        "price": price,
        "as_of_ms": t_ms,
        "klines_5m": kl5,
        "klines_1h": kl1h,
        "klines_1d": kl1d,
        "klines_5m_raw": kl5,
        "klines_1h_raw": kl1h,
        "klines_1d_raw": kl1d,
        **micro,
        "vwap_daily": None,  # computed inside predict from closed bars
        "liq_raw_data": {"_liq_disabled": True, "_liq_reason": "no PIT liquidation snapshots for replay"},
        "macro_events": {
            "composite_event_score": 0.0,
            "volatility_multiplier": 1.0,
            "macro_disabled": True,
            "macro_disabled_reason": "historical replay: macro disabled",
            "as_of_ms": t_ms,
            "recent_news": [],
            "event_tags": [],
            "summary": "macro disabled for historical as_of",
        },
        "realtime_liquidations": {},
        "btc_lead_lag": btc_lead,
    }


def metrics(rows: List[PredRow]) -> dict:
    if not rows:
        return {
            "n": 0,
            "direction_accuracy": None,
            "up_acc": None,
            "down_acc": None,
            "flat_acc": None,
            "target_price_mean_err_pct": None,
            "target_price_median_err_pct": None,
        }
    n = len(rows)
    correct = sum(1 for r in rows if r.pred_dir == r.actual_dir)
    def class_acc(cls):
        sub = [r for r in rows if r.actual_dir == cls]
        if not sub:
            return None
        return sum(1 for r in sub if r.pred_dir == cls) / len(sub)
    errs = [r.target_err_pct for r in rows if r.target_err_pct is not None]
    mean_err = sum(errs) / len(errs) if errs else None
    med_err = sorted(errs)[len(errs) // 2] if errs else None
    return {
        "n": n,
        "direction_accuracy": correct / n,
        "up_acc": class_acc("UP"),
        "down_acc": class_acc("DOWN"),
        "flat_acc": class_acc("FLAT"),
        "target_price_mean_err_pct": mean_err,
        "target_price_median_err_pct": med_err,
        "pred_dist": {
            "UP": sum(1 for r in rows if r.pred_dir == "UP"),
            "DOWN": sum(1 for r in rows if r.pred_dir == "DOWN"),
            "FLAT": sum(1 for r in rows if r.pred_dir == "FLAT"),
        },
        "actual_dist": {
            "UP": sum(1 for r in rows if r.actual_dir == "UP"),
            "DOWN": sum(1 for r in rows if r.actual_dir == "DOWN"),
            "FLAT": sum(1 for r in rows if r.actual_dir == "FLAT"),
        },
    }


def pct(x):
    return None if x is None else round(100.0 * x, 2)


def assign_windows(times: List[int], n_windows: int = 4) -> List[Tuple[str, int, int]]:
    """Return list of (window_id, oos_start, oos_end).
    Contiguous non-overlapping OOS folds. IS for Wi = all t < oos_start (expanding),
    evaluated per-window independently so later windows are not starved of IS.
    """
    if len(times) < n_windows * 10:
        n_windows = max(2, min(n_windows, max(2, len(times) // 20)))
    t0, t1 = times[0], times[-1]
    span = t1 - t0
    fold = span / n_windows
    windows = []
    for i in range(n_windows):
        oos_s = int(t0 + i * fold)
        oos_e = int(t0 + (i + 1) * fold) if i < n_windows - 1 else t1
        # Require non-empty prior IS: skip labeling IS for W1's first instant by using min history cut
        windows.append((f"W{i+1}", oos_s, oos_e))
    return windows


def main():
    out_dir = os.path.join(ROOT, "experiments", "BASELINE_EVALUATION_001")
    os.makedirs(out_dir, exist_ok=True)

    # Data range: last 21 days ending now (wall clock on server)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - 21 * 24 * 60 * 60 * 1000
    # Warmup extra 3 days for indicators
    fetch_start = start_ms - 3 * 24 * 60 * 60 * 1000

    print(f"[load] fetching klines {fetch_start} -> {end_ms}", flush=True)
    eth5 = fetch_klines("ETHUSDT", "5m", fetch_start, end_ms)
    eth1h = fetch_klines("ETHUSDT", "1h", fetch_start, end_ms)
    eth1d = fetch_klines("ETHUSDT", "1d", fetch_start, end_ms)
    btc5 = fetch_klines("BTCUSDT", "5m", fetch_start, end_ms)
    print(f"[load] eth5={len(eth5)} eth1h={len(eth1h)} eth1d={len(eth1d)} btc5={len(btc5)}", flush=True)

    print("[load] fetching OI / positioning hist", flush=True)
    oi_hist = fetch_hist("openInterestHist", "ETHUSDT", "5m", fetch_start, end_ms)
    top_ls = fetch_hist("topLongShortPositionRatio", "ETHUSDT", "5m", fetch_start, end_ms)
    global_ls = fetch_hist("globalLongShortAccountRatio", "ETHUSDT", "5m", fetch_start, end_ms)
    taker = fetch_hist("takerlongshortRatio", "ETHUSDT", "5m", fetch_start, end_ms)
    # Funding rate history (8h); as-of T uses last fundingTime <= T
    funding_hist = []
    cursor = fetch_start
    while cursor < end_ms:
        batch = http_get(f"{FAPI}/fapi/v1/fundingRate", {
            "symbol": "ETHUSDT",
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        })
        if not batch:
            break
        funding_hist.extend(batch)
        last_t = int(batch[-1]["fundingTime"])
        nxt = last_t + 1
        if nxt <= cursor:
            break
        cursor = nxt
        if len(batch) < 1000:
            break
        time.sleep(0.05)
    seen_f = {}
    for r in funding_hist:
        seen_f[int(r["fundingTime"])] = r
    funding_hist = [seen_f[k] for k in sorted(seen_f)]
    print(f"[load] oi={len(oi_hist)} top={len(top_ls)} global={len(global_ls)} taker={len(taker)} funding={len(funding_hist)}", flush=True)

    predictor = ETHPredictor()

    # Candidate as-of times: closed bar close_times within [start_ms, end_ms - horizon]
    def closed_times(klines, interval, horizon):
        times = []
        for k in klines:
            ct = int(k[6])
            if ct < start_ms or ct > end_ms - horizon:
                continue
            times.append(ct)
        return times

    tf_times = {
        "5m": closed_times(eth5, "5m", HORIZON_MS["5m"]),
        "1h": closed_times(eth1h, "1h", HORIZON_MS["1h"]),
        "1d": closed_times(eth1d, "1d", HORIZON_MS["1d"]),
    }
    # Subsample 5m to every bar (full); if too many, keep all — ~5000 ok
    print({k: len(v) for k, v in tf_times.items()}, flush=True)

    windows_by_tf = {tf: assign_windows(ts, n_windows=4 if tf != "1d" else 3) for tf, ts in tf_times.items() if ts}

    # Raw predictions before window labeling (one row per tf timestamp)
    raw_by_tf: Dict[str, List[PredRow]] = {"5m": [], "1h": [], "1d": []}

    need = sorted(set(tf_times["5m"]) | set(tf_times["1h"]) | set(tf_times["1d"]))
    print(f"[run] predict calls: {len(need)}", flush=True)

    for i, t_ms in enumerate(need):
        if i % 200 == 0:
            print(f"[run] {i}/{len(need)} t={t_ms}", flush=True)
        micro = build_micro_at(t_ms, oi_hist, top_ls, global_ls, taker, eth5, funding_hist)
        lead = btc_lead_at(t_ms, eth5, btc5)
        snap = make_snapshot(t_ms, eth5, eth1h, eth1d, btc5, micro, lead)
        if not snap["price"]:
            continue
        try:
            preds = predictor.predict(snap)
        except Exception as e:
            print(f"[warn] predict fail t={t_ms}: {e}", flush=True)
            continue

        for tf in ("5m", "1h", "1d"):
            if t_ms not in tf_times[tf]:
                continue
            rec = preds.get(tf) or {}
            if not rec:
                continue
            base = to_float(rec.get("base_price") or snap["price"])
            pred_dir = map_pred_dir(rec.get("direction") or "NEUTRAL")
            tp1 = to_float(rec.get("tp1"), 0.0)
            fut = close_at_or_after_horizon(
                {"5m": eth5, "1h": eth1h, "1d": eth1d}[tf], t_ms, HORIZON_MS[tf]
            )
            if not fut:
                continue
            actual_close, _ = fut
            move = actual_close - base
            act = actual_label(move, MIN_SPACE[tf])
            err = None
            if pred_dir != "FLAT" and tp1 > 0 and base > 0:
                err = abs(tp1 - actual_close) / base * 100.0
            raw_by_tf[tf].append(PredRow(tf, t_ms, base, pred_dir, tp1, act, actual_close, err, "", ""))

    report = {
        "EXPERIMENT_ID": EXPERIMENT_ID,
        "CODE_VERSION": CODE_VERSION,
        "BASELINE_VERSION": BASELINE_VERSION,
        "DATA_AUDIT": "PASS (AUDIT_FIX_ASOF_001 @ 2660ac2)",
        "DATA_RANGE": {
            "start_ms": start_ms,
            "end_ms": end_ms,
            "start_iso": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
            "end_iso": datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat(),
            "notes": [
                "Binance Futures public hist (klines + OI/LS/taker)",
                "Liquidation map DISABLED (no PIT snapshots)",
                "Macro DISABLED for historical as_of",
                "FLAT threshold = min_directional_space (5m:8 / 1h:30 / 1d:60 USDT)",
                "NEUTRAL predictions mapped to FLAT for metrics",
                "ALIGNED: btc_lead_lag uses spillover_score/lead_signal/divergence_pct (online parity)",
                "ALIGNED: vol_ratio from last closed 5m / MA20 closed vols",
                "ALIGNED: funding_rate from fundingRate hist as-of T",
                "ALIGNED: topLongShortPositionRatio (not AccountRatio)",
                "ALIGNED: Walk-Forward IS per-window expanding; OOS folds non-overlapping; global gap uses 60/40 disjoint cut",
            ],
        },
        "WALK_FORWARD_WINDOWS": {},
        "RESULTS": {},
        "IS_vs_OOS": {},
        "OVERFITTING_ASSESSMENT": None,
    }

    for tf in ("5m", "1h", "1d"):
        raw = raw_by_tf[tf]
        wins = {}
        win_defs = windows_by_tf.get(tf, [])
        report["WALK_FORWARD_WINDOWS"][tf] = {
            "windows": [
                {
                    "id": wid,
                    "oos": [oos_s, oos_e],
                    "is_rule": "expanding: all samples with as_of_ms < oos_start (per-window, independent)",
                }
                for wid, oos_s, oos_e in win_defs
            ],
            "note": "OOS folds are contiguous non-overlapping. IS is evaluated per window independently so W2+ IS is not consumed by earlier OOS labels.",
        }
        for wid, oos_s, oos_e in win_defs:
            is_rows = [r for r in raw if r.as_of_ms < oos_s]
            oos_rows = [r for r in raw if oos_s <= r.as_of_ms <= oos_e]
            wins[wid] = {
                "IS": metrics(is_rows),
                "OOS": metrics(oos_rows),
                "oos_range": [oos_s, oos_e],
                "n_is": len(is_rows),
                "n_oos": len(oos_rows),
            }
        # Global IS vs OOS for gap: chronological 60/40 split, no overlap (not the polluted single-label scheme)
        if raw:
            times_sorted = sorted(r.as_of_ms for r in raw)
            cut = times_sorted[int(len(times_sorted) * 0.6)]
            is_all = [r for r in raw if r.as_of_ms < cut]
            oos_all = [r for r in raw if r.as_of_ms >= cut]
        else:
            is_all, oos_all = [], []
        m_is = metrics(is_all)
        m_oos = metrics(oos_all)
        gap = None
        if m_is["direction_accuracy"] is not None and m_oos["direction_accuracy"] is not None:
            gap = m_is["direction_accuracy"] - m_oos["direction_accuracy"]
        report["RESULTS"][tf] = {
            "IS": m_is,
            "OOS": m_oos,
            "IS_OOS_split": "chronological 60/40 cut (disjoint) for gap only",
            "per_window": wins,
            "per_window_OOS": {wid: wins[wid]["OOS"] for wid in wins},
            "generalization_gap_dir_acc": gap,
        }
        report["IS_vs_OOS"][tf] = {
            "IS_direction_accuracy_pct": pct(m_is["direction_accuracy"]),
            "OOS_direction_accuracy_pct": pct(m_oos["direction_accuracy"]),
            "gap_pp": None if gap is None else round(gap * 100, 2),
            "IS_target_mean_err_pct": m_is["target_price_mean_err_pct"],
            "OOS_target_mean_err_pct": m_oos["target_price_mean_err_pct"],
        }

    # Overfitting assessment: if IS dir acc exceeds OOS by >8pp on any TF with enough samples → MEDIUM/HIGH
    gaps = []
    for tf in ("5m", "1h", "1d"):
        g = report["RESULTS"][tf]["generalization_gap_dir_acc"]
        n_oos = report["RESULTS"][tf]["OOS"]["n"]
        if g is not None and n_oos >= 20:
            gaps.append(g)
    if not gaps:
        assess = "LOW (insufficient OOS sample for strong claim; see per-TF n)"
    else:
        max_gap = max(gaps)
        if max_gap >= 0.12:
            assess = "HIGH"
        elif max_gap >= 0.08:
            assess = "MEDIUM"
        else:
            assess = "LOW"
    report["OVERFITTING_ASSESSMENT"] = assess

    out_path = os.path.join(out_dir, "report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # human summary
    summary_path = os.path.join(out_dir, "SUMMARY.txt")
    lines = []
    lines.append(f"EXPERIMENT_ID: {EXPERIMENT_ID}")
    lines.append(f"CODE_VERSION: {CODE_VERSION}")
    lines.append(f"BASELINE_VERSION: {BASELINE_VERSION}")
    lines.append(f"DATA_AUDIT: PASS")
    lines.append(f"DATA_RANGE: {report['DATA_RANGE']['start_iso']} -> {report['DATA_RANGE']['end_iso']}")
    lines.append("")
    for tf in ("5m", "1h", "1d"):
        r = report["RESULTS"][tf]
        lines.append(f"=== {tf} ===")
        lines.append(f"IS Direction Accuracy: {pct(r['IS']['direction_accuracy'])}% (n={r['IS']['n']})")
        lines.append(f"OOS Direction Accuracy: {pct(r['OOS']['direction_accuracy'])}% (n={r['OOS']['n']})")
        lines.append(f"UP/DOWN/FLAT Acc (OOS, recall by actual): {pct(r['OOS']['up_acc'])}% / {pct(r['OOS']['down_acc'])}% / {pct(r['OOS']['flat_acc'])}%")
        lines.append(f"Target Price Mean/Median Error % (OOS): {r['OOS']['target_price_mean_err_pct']} / {r['OOS']['target_price_median_err_pct']}")
        lines.append(f"Generalization Gap (IS-OOS dir acc): {None if r['generalization_gap_dir_acc'] is None else round(r['generalization_gap_dir_acc']*100,2)} pp")
        lines.append("Per-window OOS Direction Accuracy:")
        for wid, m in r["per_window_OOS"].items():
            lines.append(f"  {wid}: {pct(m['direction_accuracy'])}% (n={m['n']})")
        lines.append("")
    lines.append(f"OVERFITTING ASSESSMENT: {assess}")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines), flush=True)
    print(f"[done] wrote {out_path}", flush=True)


if __name__ == "__main__":
    main()
