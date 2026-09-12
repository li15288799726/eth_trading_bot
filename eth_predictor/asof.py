# -*- coding: utf-8 -*-
"""
AUDIT_FIX_ASOF_001 — Point-in-time / as-of-T helpers
===================================================
Closed-bar and timestamp filters for prediction/replay features.
Does NOT change strategy weights, thresholds, or UP/DOWN/NEUTRAL definitions.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Union

TZ_BJT = timezone(timedelta(hours=8))

# Binance interval -> duration ms
INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def now_ms(as_of_ms: Optional[int] = None) -> int:
    if as_of_ms is not None:
        return int(as_of_ms)
    return int(time.time() * 1000)


def bar_open_time_ms(k: Sequence) -> int:
    try:
        return int(float(k[0]))
    except Exception:
        return 0


def bar_close_time_ms(k: Sequence, interval: str = "5m") -> int:
    """
    Prefer explicit close_time at index 6 (Binance REST / WS candle layout).
    Fallback: open_time + interval - 1ms.
    """
    try:
        if len(k) > 6 and k[6] is not None:
            ct = int(float(k[6]))
            if ct > 0:
                return ct
    except Exception:
        pass
    dur = INTERVAL_MS.get(interval, 300_000)
    return bar_open_time_ms(k) + dur - 1


def is_bar_closed(k: Sequence, as_of_ms: Optional[int] = None, interval: str = "5m",
                  closed_flag: Optional[bool] = None) -> bool:
    """
    Closed iff Binance k.x is True, or close_time <= as_of_ms.
    Unclosed bars must NOT contribute final H/L/C/V to features.
    """
    if closed_flag is True:
        return True
    if closed_flag is False:
        # Still allow if close_time already elapsed (WS may lag the flag briefly)
        t = now_ms(as_of_ms)
        return bar_close_time_ms(k, interval) <= t
    t = now_ms(as_of_ms)
    return bar_close_time_ms(k, interval) <= t


def filter_closed_klines(klines: Optional[Sequence], as_of_ms: Optional[int] = None,
                         interval: str = "5m") -> List:
    """Return only bars with close_time <= as_of (closed bars)."""
    if not klines:
        return []
    t = now_ms(as_of_ms)
    out = []
    for k in klines:
        try:
            if bar_close_time_ms(k, interval) <= t:
                out.append(k)
        except Exception:
            continue
    return out


def as_of_filter(records: Optional[Iterable], ts_key: str = "timestamp",
                 as_of_ms: Optional[int] = None, ts_unit: str = "auto") -> List:
    """
    Keep records whose timestamp <= as_of_ms.
    ts_unit: 'ms' | 's' | 'auto' (auto: values < 1e12 treated as seconds).
    """
    if not records:
        return []
    t = now_ms(as_of_ms)
    out = []
    for r in records:
        try:
            if isinstance(r, dict):
                raw = r.get(ts_key)
                if raw is None:
                    for alt in ("timestamp", "time", "T", "fetched_ts", "ts"):
                        if alt in r and r[alt] is not None:
                            raw = r[alt]
                            break
                if raw is None:
                    continue
                ts = float(raw)
            else:
                continue
            if ts_unit == "auto":
                ts_ms = int(ts * 1000) if ts < 1e12 else int(ts)
            elif ts_unit == "s":
                ts_ms = int(ts * 1000)
            else:
                ts_ms = int(ts)
            if ts_ms <= t:
                out.append(r)
        except Exception:
            continue
    return out


def filter_completed_hist(records: Optional[Sequence], period: str = "5m",
                          as_of_ms: Optional[int] = None,
                          ts_key: str = "timestamp") -> List:
    """
    Point-in-time hist for OI / taker / long-short ratios.
    Keep buckets whose period has fully closed by as_of:
      timestamp + period_ms - 1 <= as_of_ms
    (Binance futures data hist timestamp is period start).
    """
    if not records:
        return []
    t = now_ms(as_of_ms)
    dur = INTERVAL_MS.get(period, 300_000)
    out = []
    for r in records:
        try:
            raw = r.get(ts_key) if isinstance(r, dict) else None
            if raw is None:
                continue
            ts = float(raw)
            ts_ms = int(ts * 1000) if ts < 1e12 else int(ts)
            if ts_ms + dur - 1 <= t:
                out.append(r)
        except Exception:
            continue
    return out


def day_start_ms_bjt(as_of_ms: Optional[int] = None) -> int:
    """Beijing 00:00 of the calendar day containing as_of."""
    t = now_ms(as_of_ms)
    dt = datetime.fromtimestamp(t / 1000.0, tz=TZ_BJT)
    start = datetime(dt.year, dt.month, dt.day, tzinfo=TZ_BJT)
    return int(start.timestamp() * 1000)


def extract_fetched_ts_ms(liq_data: Optional[dict]) -> Optional[int]:
    """Read fetched_ts / capture metadata from a liq snapshot dict."""
    if not isinstance(liq_data, dict):
        return None
    for key in ("fetched_ts", "fetched_ts_ms", "capture_time_ms", "captured_at_ms"):
        v = liq_data.get(key)
        if v is not None:
            try:
                ts = float(v)
                return int(ts * 1000) if ts < 1e12 else int(ts)
            except Exception:
                pass
    for key in ("fetched_at", "capture_time", "captured_at", "updated_at"):
        v = liq_data.get(key)
        if not v:
            continue
        if isinstance(v, (int, float)):
            ts = float(v)
            return int(ts * 1000) if ts < 1e12 else int(ts)
        if isinstance(v, str):
            try:
                # ISO or "YYYY-MM-DD HH:MM:SS"
                s = v.replace("Z", "+00:00")
                try:
                    dt = datetime.fromisoformat(s)
                except Exception:
                    dt = datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_BJT)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=TZ_BJT)
                return int(dt.timestamp() * 1000)
            except Exception:
                pass
    return None


def resolve_liq_snapshot(liq_data: Optional[dict], as_of_ms: Optional[int] = None,
                         history: Optional[Sequence[dict]] = None,
                         allow_unstamped_live: bool = True) -> dict:
    """
    Liquidation map as-of-T:
      - Prefer history entries with fetched_ts <= T (latest such).
      - Else if single snapshot has fetched_ts <= T, use it.
      - Else if live (as_of near wall-clock) and unstamped latest exists, allow for live.
      - Else DISABLE liquidation features (empty / _liq_disabled).
    """
    t = now_ms(as_of_ms)
    live_slack_ms = 5_000
    is_live = abs(t - int(time.time() * 1000)) <= live_slack_ms

    candidates: List[dict] = []
    if history:
        for item in history:
            if not isinstance(item, dict):
                continue
            # Full exLiqMap shape has list under "data"
            if not isinstance(item.get("data"), list):
                continue
            fts = extract_fetched_ts_ms(item)
            if fts is None:
                continue
            if fts <= t:
                candidates.append({"fetched_ts": fts, "snap": item})

    if candidates:
        best = max(candidates, key=lambda x: x["fetched_ts"])
        out = dict(best["snap"])
        out["fetched_ts"] = best["fetched_ts"]
        out["_liq_asof_ok"] = True
        return out

    if isinstance(liq_data, dict) and liq_data and not liq_data.get("_liq_disabled"):
        fts = extract_fetched_ts_ms(liq_data)
        if fts is not None and fts <= t:
            out = dict(liq_data)
            out["fetched_ts"] = fts
            out["_liq_asof_ok"] = True
            return out
        if fts is not None and fts > t:
            return {"_liq_disabled": True, "_liq_reason": "fetched_ts > as_of", "fetched_ts": fts}
        if is_live and allow_unstamped_live and isinstance(liq_data.get("data"), list):
            out = dict(liq_data)
            out["_liq_asof_ok"] = True
            out["_asof_unstamped"] = True
            return out
        return {"_liq_disabled": True, "_liq_reason": "no fetched_ts for replay/as-of"}

    return {"_liq_disabled": True, "_liq_reason": "no liquidation snapshot"}


def load_liq_history_from_data_dir(data_dir: Union[str, Path], limit: int = 200) -> List[dict]:
    """
    Best-effort historical liq snapshots from data/decrypted_YYYYMMDD_HHMMSS_*.json
    using filename timestamp as fetched_ts proxy when file body lacks stamp.
    """
    import json
    root = Path(data_dir)
    if not root.exists():
        return []
    files = sorted(root.glob("decrypted_*.json"), key=lambda p: p.stat().st_mtime)
    hist_dir = root / "auto" / "liq_history"
    if hist_dir.exists():
        files = files + sorted(hist_dir.glob("liq_*.json"), key=lambda p: p.stat().st_mtime)
    out: List[dict] = []
    for f in files[-limit:]:
        name = f.name
        if name.startswith("decrypted_analysis_"):
            continue
        fts = None
        try:
            stem = name
            if stem.startswith("decrypted_"):
                parts = stem.replace("decrypted_", "").split("_")
            elif stem.startswith("liq_"):
                parts = stem.replace("liq_", "").replace(".json", "").split("_")
            else:
                parts = []
            if len(parts) >= 2 and len(parts[0]) == 8 and len(parts[1]) >= 6:
                dt = datetime.strptime(parts[0] + parts[1][:6], "%Y%m%d%H%M%S").replace(tzinfo=TZ_BJT)
                fts = int(dt.timestamp() * 1000)
        except Exception:
            fts = None
        if fts is None:
            try:
                fts = int(f.stat().st_mtime * 1000)
            except Exception:
                continue
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not (isinstance(d, dict) and "rangeHigh" in d and isinstance(d.get("data"), list)):
            continue
        full = dict(d)
        full["fetched_ts"] = int(full.get("fetched_ts") or fts)
        out.append(full)
    return out
