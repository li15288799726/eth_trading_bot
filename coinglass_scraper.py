#!/usr/bin/env python3
"""
CoinGlass 币种(全交易所聚合)清算地图 JSON 拦截器

通过 Playwright 拦截 CoinGlass 前端的网络请求，直接抓取官方计算好的
聚合清算地图结构化 JSON 数据 (exLiqMap)，并提取多空各 Top 3 爆仓区。

数据源: https://www.coinglass.com/pro/futures/LiquidationMap
    页面第二张图「XX Exchange Liquidation Map」聚合 Binance/OKX/Bybit 数据，
    通过其币种选择器切换目标币种 (URL 参数无效，已实测)
对应 API: https://capi.coinglass.com/api/index/2/exLiqMap?merge=true&symbol=ETH
    (返回加密 data，前端解密后经 JSON.parse hook 拦截)

exLiqMap 解密后结构:
{
    "rangeHigh": 2653.6, "rangeLow": 2129.4,
    "instrument": {"baseAsset": "ETH", ...},
    "lastPrice": 2403.6,
    "data": [  # 各交易所，合并渲染
        {"instrument": {"exName": "Binance", ...}, "liqMapV2": {"2151": [[2151, 2278117.3, ...]]}},
        {"instrument": {"exName": "OKX", ...}, "liqMapV2": {...}},
        {"instrument": {"exName": "Bybit", ...}, "liqMapV2": {...}},
    ]
}

多空语义:
- 清算价 < 当前价 → 多头爆仓区 (价格下跌触发多头清算)
- 清算价 > 当前价 → 空头爆仓区 (价格上涨触发空头清算)
"""
import argparse
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from playwright.async_api import async_playwright, Response, TimeoutError as PlaywrightTimeoutError

# ============== 默认配置 ==============
DEFAULT_CONFIG = {
    "url": "https://www.coinglass.com/pro/futures/LiquidationMap",
    "symbol": "ETH",
    "headless": True,
    "wait_seconds": 15,
    "output_dir": "data",
    "cookies_file": "coinglass_manual.json",  # 登录态 (非 BTC 币种需要)
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# 拦截匹配模式：覆盖 liqHeatMap / liquidation/heatmap / liquidation/map 等核心接口
INTERCEPT_PATTERNS = [
    r"liqheatmap",
    r"liquidation/heatmap",
    r"liquidation/map",
    r"heatmap.*liquidation",
    r"liquidation.*heatmap",
    r"exliqmap",
]

# ============== 日志 ==============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("coinglass")


def url_matches(url: str) -> bool:
    """判断 URL 是否为清算热力图相关 API"""
    lower = url.lower()
    return any(re.search(p, lower) for p in INTERCEPT_PATTERNS)


def _detect_full_chromium_path() -> str | None:
    """
    自动探测完整 chromium 的可执行文件路径 (兼容 Windows 与 Linux)
    覆盖 PLAYWRIGHT_BROWSERS_PATH 指向的目录以及默认安装位置
    """
    candidates = []
    browsers_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if browsers_path:
        candidates.append(Path(browsers_path))
    candidates.extend([
        Path.home() / ".cache" / "ms-playwright",
        Path.home() / "AppData" / "Local" / "ms-playwright",
        Path(os.environ.get("PROGRAMFILES", r"C:\Program Files")) / "ms-playwright",
    ])
    is_win = sys.platform == "win32"
    for base in candidates:
        if not base.exists():
            continue
        for chromium_dir in sorted(base.glob("chromium-*"), reverse=True):
            if is_win:
                for sub in ("chrome-win", "chrome-win64"):
                    exe = chromium_dir / sub / "chrome.exe"
                    if exe.exists():
                        return str(exe)
            else:
                for sub in ("chrome-linux", "chrome-linux64"):
                    exe = chromium_dir / sub / "chrome"
                    if exe.exists():
                        return str(exe)
    return None


# ============== 数据保存与分析 ==============
def save_and_analyze_data(json_data, response_url: str, output_dir: Path):
    """
    保存原始 JSON + 提取关键字段生成分析摘要

    CoinGlass liqHeatMap 常见返回结构:
    {
        "code": "0",
        "msg": "success",
        "data": {
            "datas": [[price, longLiq, shortLiq], ...],
            "time": 1234567890,
            "symbol": "BTC",
            ...
        }
    }

    实际字段会因 model 版本不同而变化，这里做兼容处理。
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]

    # 1. 保存原始 JSON
    raw_file = output_dir / f"raw_{ts}.json"
    try:
        raw_file.write_text(
            json.dumps(json_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info(f"[保存] 原始数据 -> {raw_file} ({raw_file.stat().st_size} bytes)")
    except Exception as e:
        log.error(f"保存原始 JSON 失败: {e}")
        return

    # 2. 提取核心数据结构
    analysis = extract_analysis(json_data, response_url)
    analysis_file = output_dir / f"analysis_{ts}.json"
    analysis_file.write_text(
        json.dumps(analysis, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"[分析] 摘要 -> {analysis_file}")

    # 3. 控制台打印关键信息
    print_summary(analysis)


def extract_analysis(payload, url: str) -> dict:
    """从 CoinGlass 返回的 JSON 中提取关键字段"""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    result = {
        "capture_time": datetime.now().isoformat(),
        "api_url": url,
        "api_path": parsed.path,
        "query_params": {k: v[0] if len(v) == 1 else v for k, v in qs.items()},
        "raw_size_bytes": len(json.dumps(payload, ensure_ascii=False)),
        "top_level_keys": list(payload.keys()) if isinstance(payload, dict) else [],
    }

    # 兼容多种形态: hook 拦截的内层解密对象 / API 响应 {code, msg, data}
    # 注意: 聚合块顶层自带 "data" 键 (数组)，仅当 data 为 dict 时才 unwrap
    data = payload
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        data = payload["data"]

    # ===== 币种(全交易所聚合)清算地图: {rangeHigh, rangeLow, lastPrice, data: [...]} =====
    if isinstance(data, dict) and "rangeHigh" in data and isinstance(data.get("data"), list):
        result["data_keys"] = list(data.keys())
        result["format"] = "coinglass_agg_liqmap"
        result["instrument"] = data.get("instrument", {})
        result["range_low"] = data.get("rangeLow")
        result["range_high"] = data.get("rangeHigh")
        result["exchange_count"] = len(data.get("data", []))
        agg_result = extract_agg_liqmap_hotspots(data)
        if agg_result:
            result["exchanges"] = agg_result["exchanges"]
            result["current_price"] = agg_result["last_price"]
            result["long_top3"] = agg_result["long_top3"]
            result["short_top3"] = agg_result["short_top3"]
            result["long_total"] = agg_result["long_total"]
            result["short_total"] = agg_result["short_total"]
            result["long_zone_count"] = agg_result["long_zone_count"]
            result["short_zone_count"] = agg_result["short_zone_count"]
        return result

    # ===== 交易所交易对清算地图格式: liqMapV2 + lastPrice =====
    if isinstance(data, dict) and "liqMapV2" in data:
        result["data_keys"] = list(data.keys())
        result["format"] = "coinglass_liqmap"
        result["instrument"] = data.get("instrument", {})
        result["liqmap_levels"] = len(data.get("liqMapV2", {}))
        liqmap_result = extract_liqmap_hotspots(payload)
        if liqmap_result:
            result["current_price"] = liqmap_result["last_price"]
            result["long_top3"] = liqmap_result["long_top3"]
            result["short_top3"] = liqmap_result["short_top3"]
            result["long_total"] = liqmap_result["long_total"]
            result["short_total"] = liqmap_result["short_total"]
            result["long_zone_count"] = liqmap_result["long_zone_count"]
            result["short_zone_count"] = liqmap_result["short_zone_count"]
        return result

    # CoinGlass 解密格式: { instrument, liq, prices, y, rangeHigh, rangeLow, ... }
    if isinstance(data, dict) and "liq" in data and "y" in data:
        result["data_keys"] = list(data.keys())
        result["format"] = "coinglass_decrypted"
        result["instrument"] = data.get("instrument", {})
        result["range_high"] = data.get("rangeHigh")
        result["range_low"] = data.get("rangeLow")
        result["update_time"] = data.get("updateTime")
        result["precision"] = data.get("precision")
        result["liq_count"] = len(data.get("liq", []))
        result["price_levels"] = len(data.get("y", []))
        result["candlestick_count"] = len(data.get("prices", []))
        if data.get("y"):
            result["price_range"] = f"{data['y'][0]:.2f} - {data['y'][-1]:.2f}"
    elif isinstance(data, dict):
        result["data_keys"] = list(data.keys())
        for key, val in data.items():
            if isinstance(val, list) and val:
                sample = val[0]
                sample_str = json.dumps(sample, ensure_ascii=False)
                result[f"data_{key}_info"] = {
                    "length": len(val),
                    "sample": sample_str[:500] + ("..." if len(sample_str) > 500 else ""),
                }
            elif isinstance(val, dict):
                result[f"data_{key}_keys"] = list(val.keys())[:20]
            else:
                result[f"data_{key}"] = val

    # 尝试提取清算密集区 (多空各存 Top 10，避免单边行情时缺失另一侧)
    hotspots = extract_liquidation_hotspots(payload)
    if hotspots:
        cur_price = _get_current_price(data) if isinstance(data, dict) else None
        top_long = [h for h in hotspots if h.get("side") == "long"][:10]
        top_short = [h for h in hotspots if h.get("side") == "short"][:10]
        result["liquidation_hotspots"] = sorted(
            top_long + top_short, key=lambda x: x["total_liq"], reverse=True
        )
        if cur_price is not None:
            result["current_price"] = cur_price
        result["long_top3"] = top_long[:3]
        result["short_top3"] = top_short[:3]

    return result


def _get_current_price(data: dict) -> float | None:
    """
    从 CoinGlass 解密数据的 prices 蜡烛数组中取最新收盘价作为当前价格

    prices 结构: [[time, open, high, low, close, vol], ...]
    注意: OHLC 可能是字符串格式，需转换为 float
    """
    if "liqMapV2" in data and "lastPrice" in data:
        try:
            return float(data["lastPrice"])
        except (ValueError, TypeError):
            pass
    prices = data.get("prices")
    if not isinstance(prices, list) or not prices:
        return None
    last = prices[-1]
    if isinstance(last, (list, tuple)) and len(last) >= 5:
        try:
            return float(last[4])
        except (ValueError, TypeError):
            return None
    return None


def extract_agg_liqmap_hotspots(payload) -> dict:
    """
    从 CoinGlass 币种(全交易所聚合)清算地图提取多空各 Top N 爆仓区

    exLiqMap 聚合格式 (前端解密后):
    {
        "rangeHigh": 2653.6, "rangeLow": 2129.4,
        "instrument": {"baseAsset": "ETH", ...},
        "lastPrice": 2403.6,
        "data": [  # 各交易所清算地图，合并渲染
            {"instrument": {"exName": "Binance", "baseAsset": "ETH", ...},
             "liqMapV2": {"2151": [[2151, 2278117.3, null, null]], ...}},
            {"instrument": {"exName": "OKX", ...}, "liqMapV2": {...}},
            {"instrument": {"exName": "Bybit", ...}, "liqMapV2": {...}},
        ]
    }
    注意: 聚合图无杠杆/档位维度 (均为 null)，仅 [价格, 清算额USD]

    多空语义:
    - 清算价 < lastPrice → 多头爆仓区 (价格下跌触发多头清算)
    - 清算价 > lastPrice → 空头爆仓区 (价格上涨触发空头清算)
    """
    # 注意: 聚合块顶层自带 "data" 键 (数组)，不能盲目 unwrap
    data = payload
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        data = payload["data"]  # API 包装形态 {code, msg, data: {...}}
    if not (isinstance(data, dict) and "rangeHigh" in data
            and isinstance(data.get("data"), list) and "lastPrice" in data):
        return {}

    try:
        last_price = float(data["lastPrice"])
    except (ValueError, TypeError):
        return {}

    # 合并所有交易所的 liqMapV2，按价格档累加清算额
    merged = {}
    for entry in data["data"]:
        if not isinstance(entry, dict):
            continue
        liqmap = entry.get("liqMapV2")
        if not isinstance(liqmap, dict):
            continue
        for key, items in liqmap.items():
            try:
                price = float(key)
            except (ValueError, TypeError):
                continue
            if isinstance(items, (list, tuple)):
                for it in items:
                    if isinstance(it, (list, tuple)) and len(it) >= 2:
                        try:
                            merged[price] = merged.get(price, 0.0) + abs(float(it[1]))
                        except (ValueError, TypeError):
                            continue

    long_zones, short_zones = [], []
    for price, total in merged.items():
        if total <= 0:
            continue
        zone = {"price": price, "total_liq": total}
        (short_zones if price > last_price else long_zones).append(zone)

    long_zones.sort(key=lambda z: z["total_liq"], reverse=True)
    short_zones.sort(key=lambda z: z["total_liq"], reverse=True)

    return {
        "instrument": data.get("instrument", {}),
        "last_price": last_price,
        "range_low": data.get("rangeLow"),
        "range_high": data.get("rangeHigh"),
        "exchanges": [
            e.get("instrument", {}).get("exName", "?")
            for e in data["data"] if isinstance(e, dict)
        ],
        "long_top3": long_zones[:3],
        "short_top3": short_zones[:3],
        "long_total": sum(z["total_liq"] for z in long_zones),
        "short_total": sum(z["total_liq"] for z in short_zones),
        "long_zone_count": len(long_zones),
        "short_zone_count": len(short_zones),
    }


def extract_liqmap_hotspots(payload) -> dict:
    """
    从 CoinGlass 交易对清算地图 (liqMapV2) 提取多空各 Top N 爆仓区

    数据结构 (open-api-v4 /api/futures/liquidation/map 前端解密后):
    {
        "instrument": {"baseAsset": "ETH", "exName": "Binance", "instrumentId": "ETHUSDT"},
        "lastPrice": 2400,
        "liqMapV2": {
            "2361.2": [[2361.2, 22253000, 100, "h3"], ...],
            # key: 价格档位; value: [清算价, 清算额USD, 杠杆倍数, 档位] 列表
        }
    }

    多空语义 (清算地图标准约定):
    - 清算价 < 当前价 → 多头爆仓区 (价格下跌触发多头清算)
    - 清算价 > 当前价 → 空头爆仓区 (价格上涨触发空头清算)
    """
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not (isinstance(data, dict) and "liqMapV2" in data and "lastPrice" in data):
        return {}

    try:
        last_price = float(data["lastPrice"])
    except (ValueError, TypeError):
        return {}

    liqmap = data["liqMapV2"]
    if not isinstance(liqmap, dict):
        return {}

    long_zones, short_zones = [], []
    for key, items in liqmap.items():
        try:
            price = float(key)
        except (ValueError, TypeError):
            continue
        # 聚合该价格档位下所有杠杆档的清算额
        total = 0.0
        if isinstance(items, (list, tuple)):
            for it in items:
                if isinstance(it, (list, tuple)) and len(it) >= 2:
                    try:
                        total += abs(float(it[1]))
                    except (ValueError, TypeError):
                        continue
        if total <= 0:
            continue
        zone = {"price": price, "total_liq": total}
        (short_zones if price > last_price else long_zones).append(zone)

    long_zones.sort(key=lambda z: z["total_liq"], reverse=True)
    short_zones.sort(key=lambda z: z["total_liq"], reverse=True)

    return {
        "instrument": data.get("instrument", {}),
        "last_price": last_price,
        "long_top3": long_zones[:3],
        "short_top3": short_zones[:3],
        "long_total": sum(z["total_liq"] for z in long_zones),
        "short_total": sum(z["total_liq"] for z in short_zones),
        "long_zone_count": len(long_zones),
        "short_zone_count": len(short_zones),
    }


def extract_liquidation_hotspots(payload) -> list:
    """
    从 CoinGlass 解密数据中提取【当前】清算密集区

    关键修正 (2026-08-04):
    - liq 数组结构: [[time_idx, price_idx, amount], ...]
    - time_idx 代表历史时间切片 (通常 200+ 个)
    - 热力图展示的是【当前状态】各价格的清算积压，而非历史爆仓总和
    - 正确算法: 只取最新的 time_idx (即 max(time_idx)) 的切片数据
      过滤掉所有历史陈旧切片，避免盲目累加导致总量失真 (旧算法 10B+ 是错的)
    - 期望量级: 单档位几千万 (M) 到几亿 (M)，与网页柱状图纵坐标匹配

    支持的结构:
    - CoinGlass 解密格式: {"liq": [[time_idx, price_idx, amount], ...], "y": [price_levels]}
    - [[price, longLiq, shortLiq], ...]  (旧格式兜底)
    - [{"price": x, "liq": y}, ...]
    - {"datas": [[...]], "prices": [...]}
    """
    data = payload.get("data", payload) if isinstance(payload, dict) else payload

    # ===== CoinGlass 解密格式: liq + y 数组 (仅取最新时间切片) =====
    if isinstance(data, dict) and "liq" in data and "y" in data:
        liq_arr = data["liq"]
        y_prices = data["y"]
        if (isinstance(liq_arr, list) and liq_arr
                and isinstance(y_prices, list) and y_prices
                and isinstance(liq_arr[0], (list, tuple))
                and len(liq_arr[0]) >= 3):
            try:
                # 1. 找出最新的 time_idx (即当前状态)
                all_time_indices = [row[0] for row in liq_arr if len(row) >= 3]
                if not all_time_indices:
                    return []
                latest_time_idx = max(all_time_indices)
                total_time_slices = len(set(all_time_indices))

                # 2. 只保留最新 time_idx 的行，按 price_idx 聚合
                price_totals = {}
                skipped_historical = 0
                for row in liq_arr:
                    if len(row) >= 3:
                        time_idx, price_idx, amount = row[0], row[1], row[2]
                        if time_idx != latest_time_idx:
                            skipped_historical += 1
                            continue  # 跳过历史陈旧切片
                        price_totals[price_idx] = price_totals.get(price_idx, 0) + abs(amount)

                # 3. 映射到实际价格 + 多空方向分类
                # 多空语义 (清算热力图标准约定):
                #   价格 > 当前价 → 空头清算区 (Short, 价格上涨爆空)
                #   价格 < 当前价 → 多头清算区 (Long, 价格下跌爆多)
                cur_price = _get_current_price(data)
                scored = []
                for price_idx, total in price_totals.items():
                    if 0 <= price_idx < len(y_prices):
                        price = y_prices[price_idx]
                        side = "short" if (cur_price is not None and price > cur_price) else "long"
                        scored.append((price, total, price_idx, side))

                scored.sort(key=lambda x: x[1], reverse=True)

                # 调试日志: 帮助确认算法正确性
                side_summary = ""
                if cur_price is not None:
                    n_short = sum(1 for s in scored if s[3] == "short")
                    n_long = sum(1 for s in scored if s[3] == "long")
                    side_summary = f" | 当前价: {cur_price:.2f} | 空头区档位: {n_short} | 多头区档位: {n_long}"
                log.info(
                    f"[清算密集区] 时间切片: {total_time_slices} 个 | "
                    f"使用最新切片: time_idx={latest_time_idx} | "
                    f"跳过历史行: {skipped_historical} | "
                    f"最新切片聚合档位数: {len(scored)}"
                    f"{side_summary}"
                )

                # 返回全部聚合档位 (含 side 多空标记)，调用方按需取 Top N
                return [
                    {"price": p, "total_liq": t, "price_index": idx, "side": s}
                    for p, t, idx, s in scored
                ]
            except Exception as e:
                log.warning(f"CoinGlass 解密格式提取失败: {e}")
                pass

    candidates = []
    if isinstance(data, dict):
        for k in ("datas", "data", "list", "heatmap", "liquidation"):
            if k in data and isinstance(data[k], list):
                candidates.append(data[k])
    if isinstance(data, list):
        candidates.append(data)

    for arr in candidates:
        if not arr or not isinstance(arr[0], (list, tuple)):
            continue
        try:
            scored = []
            for row in arr:
                if len(row) >= 3 and all(isinstance(x, (int, float)) for x in row[:3]):
                    price, long_liq, short_liq = row[0], row[1], row[2]
                    total = abs(long_liq) + abs(short_liq)
                    scored.append((price, long_liq, short_liq, total))
            if not scored:
                continue
            scored.sort(key=lambda x: x[3], reverse=True)
            return [
                {"price": p, "long_liq": ll, "short_liq": sl, "total": t}
                for p, ll, sl, t in scored[:5]
            ]
        except Exception:
            continue
    return []


def print_summary(analysis: dict):
    """打印关键摘要到控制台"""
    print("\n" + "=" * 70)
    print(f"  API: {analysis.get('api_path', '?')}")
    print(f"  参数: {analysis.get('query_params', {})}")
    print(f"  原始大小: {analysis.get('raw_size_bytes', 0)} bytes")
    print(f"  顶层字段: {analysis.get('top_level_keys', [])}")
    if "data_keys" in analysis:
        print(f"  data 字段: {analysis['data_keys']}")

    # CoinGlass 币种聚合清算地图详情
    if analysis.get("format") == "coinglass_agg_liqmap":
        inst = analysis.get("instrument", {})
        print(f"  币种: {inst.get('baseAsset', '?')} | 聚合交易所: {', '.join(analysis.get('exchanges', []))}")
        print(f"  价格范围: {analysis.get('range_low', '?')} - {analysis.get('range_high', '?')}")
        cur = analysis.get("current_price")
        if cur is not None:
            print(f"  当前价格: {cur:,.1f}")
        print(f"  多头区总额: {analysis.get('long_total', 0) / 1e6:,.1f} M | 空头区总额: {analysis.get('short_total', 0) / 1e6:,.1f} M")

    # CoinGlass 交易所交易对清算地图详情
    if analysis.get("format") == "coinglass_liqmap":
        inst = analysis.get("instrument", {})
        print(f"  交易对: {inst.get('baseAsset', '?')}/{inst.get('quoteAsset', '?')} @ {inst.get('exName', '?')}")
        print(f"  价格档位: {analysis.get('liqmap_levels', 0)}")
        cur = analysis.get("current_price")
        if cur is not None:
            print(f"  当前价格: {cur:,.1f}")
        print(f"  多头区总额: {analysis.get('long_total', 0) / 1e6:,.1f} M | 空头区总额: {analysis.get('short_total', 0) / 1e6:,.1f} M")

    # CoinGlass 解密格式详情
    if analysis.get("format") == "coinglass_decrypted":
        inst = analysis.get("instrument", {})
        print(f"  交易对: {inst.get('baseAsset', '?')}/{inst.get('quoteAsset', '?')} @ {inst.get('exName', '?')}")
        print(f"  价格范围: {analysis.get('range_low', '?')} - {analysis.get('range_high', '?')}")
        print(f"  价格档位: {analysis.get('price_levels', 0)} | 蜡烛数: {analysis.get('candlestick_count', 0)}")
        print(f"  清算数据点: {analysis.get('liq_count', 0)}")
        print(f"  更新时间: {analysis.get('update_time', '?')}")

    if analysis.get("long_top3") or analysis.get("short_top3"):
        cur = analysis.get("current_price")
        if cur is not None:
            print(f"\n  当前价格: {cur:,.1f}")
        # 多空各 Top 3 爆仓区
        print(f"\n  >> 多头爆仓区 Top 3 (当前价下方, 下跌爆多):")
        for h in analysis.get("long_top3", []):
            print(
                f"     价格 {h['price']:>12,.1f} | "
                f"爆仓额 {h['total_liq'] / 1e6:>10,.2f} M"
            )
        print(f"\n  >> 空头爆仓区 Top 3 (当前价上方, 上涨爆空):")
        for h in analysis.get("short_top3", []):
            print(
                f"     价格 {h['price']:>12,.1f} | "
                f"爆仓额 {h['total_liq'] / 1e6:>10,.2f} M"
            )
    print("=" * 70 + "\n")


# ============== 主拦截流程 ==============
async def intercept_coinglass_heatmap(config: dict):
    """主流程：启动浏览器、访问页面、拦截 JSON"""
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    captured_count = 0

    # 优先使用环境变量指定的完整 chromium 路径
    # (规避 chrome-headless-shell 在某些环境下 ICU 数据加载失败的问题)
    executable_path = (
        os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH")
        or _detect_full_chromium_path()
    )
    if executable_path:
        log.info(f"使用完整 Chromium: {executable_path}")

    async with async_playwright() as p:
        launch_kwargs = {
            "headless": config["headless"],
            "args": [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        }
        if executable_path:
            launch_kwargs["executable_path"] = executable_path
        browser = await p.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=config["user_agent"],
            viewport={"width": 1600, "height": 900},
            locale="en-US",
        )

        # 反自动化检测
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        # 注入 JSON.parse hook：拦截前端解密后的清算地图数据
        # CoinGlass API 返回加密 data 字段，前端解密后必然经过 JSON.parse
        await context.add_init_script(JSON_PARSE_HOOK_JS)

        # 加载登录 cookies (优先顺序: 指定文件 > coinglass_manual.json > coinglass_auth.json)
        cookies_candidates = []
        if config.get("cookies_file"):
            cookies_candidates.append(Path(config["cookies_file"]))
        cookies_candidates.extend([
            Path("coinglass_manual.json"),
            Path.home() / "coinglass_manual.json",
            Path("coinglass_auth.json"),
            Path(__file__).parent / "coinglass_manual.json",
            Path(__file__).parent / "coinglass_auth.json",
        ])

        loaded_cookies = False
        for cfile in cookies_candidates:
            if cfile.exists():
                try:
                    auth = json.loads(cfile.read_text(encoding="utf-8"))
                    cookies = auth if isinstance(auth, list) else auth.get("cookies", [])
                    if cookies:
                        await context.add_cookies(cookies)
                        log.info(f"已加载登录态: {cfile} ({len(cookies)} cookies)")
                        loaded_cookies = True
                        break
                except Exception as e:
                    log.warning(f"加载 cookies 失败 ({cfile}): {e}")

        if not loaded_cookies:
            log.warning("未找到有效 cookies 文件 (非 BTC 币种可能需要登录态)")

        page = await context.new_page()
        code_40000_detected = False

        async def on_response(response: Response):
            nonlocal captured_count, code_40000_detected
            url = response.url
            if "exLiqMap" in url or "liqHeatMap" in url or "topPosition" in url:
                try:
                    text = await response.text()
                    if '"code":"40000"' in text or '"code": "40000"' in text:
                        code_40000_detected = True
                        log.warning(f"[登录态阻断] 接口返回 code=40000: CoinGlass 需要登录才能获取非 BTC 币种数据！({url[:90]})")
                except Exception:
                    pass
            if not url_matches(url):
                return
            if response.status != 200:
                log.debug(f"非 200 响应: {url} (status={response.status})")
                return
            try:
                data = await response.json()
            except Exception as e:
                log.warning(f"解析 JSON 失败 {url}: {e}")
                return

            log.info(f">> 命中接口: {url}")
            captured_count += 1
            try:
                save_and_analyze_data(data, url, output_dir)
            except Exception as e:
                log.error(f"保存/分析失败: {e}", exc_info=True)

        page.on("response", on_response)

        url = config["url"]
        log.info(f"正在加载页面: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        except PlaywrightTimeoutError:
            log.warning("页面加载超时，继续...")

        # 立即清除 Cookie 与遮罩弹窗 (不再漫长轮询)
        await dismiss_consent_dialog(page, wait_sec=6)
        await asyncio.sleep(2)

        # 检测 Cloudflare 验证页
        await detect_cloudflare(page)

        # 立即切换币种，无需等待默认 BTC 的漫长加载
        target_symbol = config["symbol"].upper()
        if target_symbol != "BTC":
            await switch_agg_symbol(page, target_symbol)

        # 等待数据返回
        log.info("等待数据返回...")
        await asyncio.sleep(8)

        # 检索 JSON.parse hook 拦截的数据 (按目标币种过滤)
        hook_count = await retrieve_captured_data(page, output_dir, symbol=target_symbol)

        # 兜底: 如果没抓到且未检测到 40000，重试一次切换
        if hook_count == 0 and not code_40000_detected:
            log.warning("未抓到目标币种数据，重试切换...")
            await switch_agg_symbol(page, target_symbol)
            await asyncio.sleep(10)
            hook_count = await retrieve_captured_data(page, output_dir, symbol=target_symbol)

        if code_40000_detected and hook_count == 0:
            log.error(f"CoinGlass 提示需要登录 (code 40000)，币种 {target_symbol} 无法获取数据，请更新 coinglass_manual.json 的 cookies！")

        await context.close()
        await browser.close()

    log.info(f"完成！API 响应: {captured_count}，JSON.parse hook: {hook_count}")
    return captured_count + hook_count



# ============== JSON.parse + Response.json Hook (拦截解密后的数据) ==============
JSON_PARSE_HOOK_JS = """
(() => {
    if (window.__json_parse_hooked__) return;
    window.__json_parse_hooked__ = true;
    window.__captured_heatmap_data__ = [];
    window.__captured_all_json__ = [];  // 诊断：记录所有 JSON.parse/Response.json 的结果摘要

    const MAX_CAPTURES = 30;
    const origParse = JSON.parse;

    // 宽松判断：是否包含值得记录的数组结构
    const hasInterestingArray = (obj) => {
        if (!obj || typeof obj !== 'object') return false;
        const str = JSON.stringify(obj);
        if (!str || str.length < 200) return false;

        // 直接是二维数组
        if (Array.isArray(obj) && obj.length > 5 && Array.isArray(obj[0])) return true;

        // 任意层级有长度>5的数组
        const check = (o, depth) => {
            if (depth > 3 || !o || typeof o !== 'object') return false;
            if (Array.isArray(o)) {
                if (o.length > 5) {
                    if (Array.isArray(o[0]) || typeof o[0] === 'number') return true;
                }
                return o.some(v => check(v, depth + 1));
            }
            for (const k in o) {
                if (check(o[k], depth + 1)) return true;
            }
            return false;
        };
        return check(obj, 0);
    };

    // 记录摘要 (用于诊断)
    const logSummary = (source, result) => {
        if (window.__captured_all_json__.length >= 50) return;
        try {
            const str = JSON.stringify(result);
            if (!str || str.length < 100) return;
            const summary = {
                source: source,
                length: str.length,
                type: Array.isArray(result) ? 'array' : typeof result,
                keys: Array.isArray(result) ? `array[${result.length}]` : Object.keys(result).slice(0, 10).join(','),
                preview: str.slice(0, 300),
            };
            window.__captured_all_json__.push(summary);
        } catch(e) {}
    };

    // 判断是否为热力图数据结构 (宽松版)
    const looksLikeHeatmapData = (obj) => {
        if (!obj || typeof obj !== 'object') return false;
        const str = JSON.stringify(obj);
        if (!str || str.length < 500) return false;

        // 模式1: { datas: [[price, longLiq, shortLiq], ...] }
        if (obj.datas && Array.isArray(obj.datas) && obj.datas.length > 5) return true;

        // 模式2: { data: { datas: [...] } } (嵌套)
        if (obj.data && typeof obj.data === 'object' && obj.data.datas && Array.isArray(obj.data.datas) && obj.data.datas.length > 5) return true;

        // 模式3: 直接是 [[price, longLiq, shortLiq], ...] 数组
        if (Array.isArray(obj) && obj.length > 10 && Array.isArray(obj[0]) && obj[0].length >= 2) return true;

        // 模式4: 包含 liquidation/heatmap 关键字段的大对象
        const keys = Object.keys(obj).join('').toLowerCase();
        if ((keys.includes('liquidation') || keys.includes('heatmap') || keys.includes('liqmap'))
            && str.length > 1000) return true;

        // 模式5: 聚合清算地图 exLiqMap: {rangeHigh, rangeLow, data: [{instrument, liqMapV2}, ...]}
        if (('rangeHigh' in obj || 'rangeLow' in obj) && Array.isArray(obj.data)) return true;

        // 模式6: 任意对象包含大数组的嵌套结构
        if (hasInterestingArray(obj) && str.length > 1000) return true;

        return false;
    };

    const captureIfMatch = (source, result) => {
        try {
            logSummary(source, result);
            if (window.__captured_heatmap_data__.length >= MAX_CAPTURES) return;
            if (looksLikeHeatmapData(result)) {
                window.__captured_heatmap_data__.push({
                    timestamp: Date.now(),
                    source: source,
                    data: result,
                    preview: JSON.stringify(result).slice(0, 500),
                });
            }
        } catch(e) {}
    };

    // Hook JSON.parse
    JSON.parse = function(text, reviver) {
        const result = origParse.apply(this, arguments);
        captureIfMatch('JSON.parse', result);
        return result;
    };

    // Hook Response.prototype.json
    if (window.Response && Response.prototype.json) {
        const origJson = Response.prototype.json;
        Response.prototype.json = function() {
            return origJson.apply(this, arguments).then(result => {
                captureIfMatch('Response.json', result);
                return result;
            });
        };
    }

    // Hook Response.prototype.text (解密可能先走 text 再 parse)
    if (window.Response && Response.prototype.text) {
        const origText = Response.prototype.text;
        Response.prototype.text = function() {
            return origText.apply(this, arguments).then(result => {
                try {
                    // 尝试解析 text 结果看是否是 JSON
                    if (result && (result.startsWith('{') || result.startsWith('['))) {
                        const parsed = origParse(result);
                        captureIfMatch('Response.text->parse', parsed);
                    }
                } catch(e) {}
                return result;
            });
        };
    }
})();
"""


async def retrieve_captured_data(page, output_dir: Path, symbol: str | None = None) -> int:
    """
    从 JSON.parse/Response.json hook 中检索拦截到的解密数据

    symbol: 目标币种过滤。liqMapV2 数据按 instrument.baseAsset 匹配，
            过滤掉页面初始加载的 BTC 等非目标数据。
    """
    log.info("检索 JSON.parse/Response.json hook 拦截的数据...")
    try:
        result = await page.evaluate("""() => ({
            heatmap: window.__captured_heatmap_data__ || [],
            all_json: window.__captured_all_json__ || [],
        })""")
        captured = result.get("heatmap", [])
        all_json = result.get("all_json", [])

        # 即使没捕获到热力图数据，也保存诊断摘要
        if all_json:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            diag_file = output_dir / f"json_hook_diag_{ts}.json"
            diag_file.write_text(
                json.dumps(all_json, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            log.info(f"[诊断] JSON hook 摘要 -> {diag_file} ({len(all_json)} 条记录)")

        if not captured:
            log.warning(f"JSON.parse hook 未捕获到热力图数据 (诊断记录: {len(all_json)} 条)")
            return 0

        log.info(f"JSON.parse hook 拦截到 {len(captured)} 个数据块")
        count = 0
        saved_pairs = set()  # 去重: 同一币种的重复数据块只分析一次
        for i, item in enumerate(captured):
            data = item.get("data")
            if not data:
                continue
            # 币种过滤 (严格): 必须有 instrument.baseAsset 且与目标一致
            # 过滤掉初始加载的 BTC 数据和 UI 主题等噪声数据块
            inst = data.get("instrument") if isinstance(data, dict) else None
            base_asset = (inst or {}).get("baseAsset", "").upper() if inst else ""
            if symbol:
                if base_asset != symbol.upper():
                    log.debug(f"跳过数据块: baseAsset={base_asset or '(无)'} (目标: {symbol})")
                    continue
            # 去重: baseAsset + 价格档位数相同视为同一份重复数据
            levels = len(data.get("liqMapV2", {})) if isinstance(data, dict) else 0
            dedup_key = (base_asset, levels, json.dumps(data, default=str)[:200])
            if base_asset and dedup_key in saved_pairs:
                continue
            if base_asset:
                saved_pairs.add(dedup_key)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            raw_file = output_dir / f"decrypted_{ts}_{i}.json"
            raw_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            log.info(f"[解密数据] -> {raw_file} ({raw_file.stat().st_size} bytes)")

            # 生成分析摘要
            analysis = extract_analysis(data, "json_parse_hook")
            analysis_file = output_dir / f"decrypted_analysis_{ts}_{i}.json"
            analysis_file.write_text(
                json.dumps(analysis, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            print_summary(analysis)
            count += 1

        return count
    except Exception as e:
        log.error(f"检索 JSON.parse hook 数据失败: {e}", exc_info=True)
        return 0


async def detect_cloudflare(page):
    """检测 Cloudflare 验证页面并等待通过"""
    try:
        content = await page.content()
        if "Just a moment" in content or "cf-browser-verification" in content:
            log.warning("检测到 Cloudflare 验证，等待 15s 让其通过...")
            await asyncio.sleep(15)
    except Exception:
        pass


async def dismiss_consent_dialog(page, wait_sec: float = 6):
    """关闭 Cookie 同意弹窗与遮罩层 (支持 Termly / FundingChoices / MuiModal)

    全屏遮罩 (termly, fc-dialog-overlay, MuiModal-backdrop) 会拦截点击, 导致币种切换失败。
    轮询检测并彻底移除遮罩 DOM。
    """
    deadline = asyncio.get_event_loop().time() + wait_sec
    handled = False
    while asyncio.get_event_loop().time() < deadline:
        try:
            result = await page.evaluate("""() => {
                let action = null;
                // 1. 移除 Termly 隐私合规弹窗
                const termly = document.querySelectorAll('#termly-code-snippet-support, [data-termly-part], [class*="termly"]');
                if (termly.length > 0) {
                    termly.forEach(el => el.remove());
                    action = 'termly_removed';
                }

                // 2. 移除 Google FundingChoices 弹窗
                const fc = document.querySelectorAll('.fc-consent-root, .fc-dialog-overlay');
                if (fc.length > 0) {
                    fc.forEach(el => el.remove());
                    action = action || 'fc_removed';
                }

                // 3. 移除 Mui 全屏 Backdrop 遮罩 (避免拦截 pointer-events)
                const backdrops = document.querySelectorAll('.MuiModal-backdrop, .MuiBackdrop-root, [class*="Backdrop"]');
                if (backdrops.length > 0) {
                    backdrops.forEach(b => b.remove());
                    action = action || 'backdrop_removed';
                }

                document.body.style.overflow = 'auto';
                return action;
            }""")
            if result:
                log.info(f"遮罩弹窗已清除: {result}")
                handled = True
                break
        except Exception:
            pass
        await asyncio.sleep(0.5)

    try:
        await page.evaluate("() => { document.body.style.overflow = 'auto'; }")
    except Exception:
        pass
    return handled


async def switch_agg_symbol(page, symbol: str):
    """
    切换「交易所清算地图」(币种聚合图, exLiqMap) 的币种选择器

    LiquidationMap 页面结构:
    - combobox[0]: 交易对选择器 (如 'Binance BTC/USDT Perpetual')
    - combobox[1]: 聚合图币种选择器 (如 'BTC')
    - combobox[2]: Hyperliquid 图币种选择器 (如 'BTC')
    """
    target = symbol.upper()
    log.info(f"开始切换聚合图币种到 {target}")

    # 清除遮罩
    await dismiss_consent_dialog(page, wait_sec=3)
    await asyncio.sleep(1)

    boxes = page.locator("input.MuiAutocomplete-input[role='combobox']")
    n = await boxes.count()
    coin_boxes = []
    for i in range(n):
        try:
            val = await boxes.nth(i).input_value()
            if val and "/" not in val and "Perpetual" not in val and 1 < len(val.strip()) <= 12:
                coin_boxes.append(i)
        except Exception:
            continue
    log.info(f"币种选择器候选 (索引): {coin_boxes}")
    if not coin_boxes:
        log.warning("未找到聚合图币种选择器")
        return False

    box = boxes.nth(coin_boxes[0])
    try:
        await page.evaluate("""() => {
            document.querySelectorAll('#termly-code-snippet-support, [data-termly-part], [class*="termly"], .MuiModal-backdrop, .MuiBackdrop-root').forEach(e => e.remove());
            document.body.style.overflow = 'auto';
        }""")

        await box.click(force=True, timeout=5000)
        await asyncio.sleep(0.8)
        await page.keyboard.press("Control+a")
        await page.keyboard.press("Delete")
        await asyncio.sleep(0.5)
        await box.fill(target)
        await asyncio.sleep(1.5)

        clicked = await page.evaluate(f"""() => {{
            const opts = document.querySelectorAll('.MuiAutocomplete-option, [role="option"]');
            const target = '{target}';
            for (const opt of opts) {{
                const text = (opt.textContent || '').trim();
                if (text === target) {{ opt.click(); return text; }}
            }}
            for (const opt of opts) {{
                const text = (opt.textContent || '').trim();
                if (text.startsWith(target + ' ') || text.startsWith(target + '(')) {{
                    opt.click(); return text;
                }}
            }}
            return null;
        }}""")

        if clicked:
            log.info(f"点击了下拉匹配选项: {clicked}")
        else:
            await page.keyboard.press("Enter")
            log.info("按 Enter 确认币种输入")

        await asyncio.sleep(5)
        log.info(f"聚合图币种切换完成: {target}")
        return True
    except Exception as e:
        log.warning(f"切换聚合图币种失败: {e}")
        return False


async def try_switch_symbol(page, symbol: str):
    """
    在 CoinGlass 页面上切换币种以触发新数据请求

    CoinGlass Pro LiquidationHeatMap 页面结构 (经 DOM 诊断确认):
    - 币种选择器是 MuiAutocomplete 输入框 (role='combobox')
    - 位于页面中部 (约 y=330 位置)
    - 旁边有 "Pair" 标签按钮

    切换策略:
    1. 定位 MuiAutocomplete 输入框
    2. 清空当前值 (BTC)
    3. 输入目标币种 (ETH)
    4. 等待自动补全下拉
    5. 按 Enter 或点击匹配项
    """
    target = symbol.upper()
    log.info(f"开始切换币种到 {target}")

    # 步骤 1: 定位 MuiAutocomplete 输入框 (多种选择器兜底)
    autocomplete_selectors = [
        "input.MuiAutocomplete-input",
        "input[role='combobox']",
        "input[class*='Autocomplete']",
        "input[class*='autocomplete']",
        # Mui Input 兜底
        ".MuiInput-root input",
        ".MuiInputBase-input",
    ]

    input_el = None
    for sel in autocomplete_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                # 检查是否可见且可交互
                if await loc.is_visible():
                    input_el = loc
                    log.info(f"找到币种输入框: {sel}")
                    break
        except Exception:
            continue

    if input_el is None:
        log.warning("未找到 MuiAutocomplete 输入框")
        return

    # 步骤 2: 点击输入框激活
    try:
        await input_el.click(timeout=5000)
        await asyncio.sleep(0.5)
    except Exception as e:
        log.warning(f"点击输入框失败: {e}")
        return

    # 步骤 3: 清空当前值 (全选后删除)
    try:
        # Ctrl+A 全选
        await page.keyboard.press("Control+a")
        await asyncio.sleep(0.2)
        # Delete 删除
        await page.keyboard.press("Delete")
        await asyncio.sleep(0.3)
    except Exception:
        pass

    # 步骤 4: 输入目标币种
    try:
        await input_el.fill(target, timeout=3000)
    except Exception:
        await page.keyboard.type(target)
    log.info(f"已输入 '{target}' 到币种输入框")
    await asyncio.sleep(1.5)  # 等待自动补全下拉

    # 步骤 5: 尝试点击下拉匹配项
    # 关键: 必须匹配 "ETHUSDT" 或 "ETH/USDT" 等完整交易对名称
    # 不能只匹配 "ETH" 否则会误点 Google 登录等含 ETH 文本的元素
    pair_patterns = [
        f"{target}USDT",
        f"{target}/USDT",
        f"{target}-USDT",
        f"{target}USDT-PERP",
    ]
    option_selectors = []
    for pp in pair_patterns:
        option_selectors.extend([
            f".MuiAutocomplete-option:has-text('{pp}')",
            f"[role='option']:has-text('{pp}')",
            f"li[role='option']:has-text('{pp}')",
        ])

    clicked = False
    for pat in option_selectors:
        try:
            opt = page.locator(pat).first
            if await opt.count() > 0:
                await opt.click(timeout=3000)
                log.info(f"点击了匹配项: {pat}")
                clicked = True
                break
        except Exception:
            continue

    # 如果精确匹配没找到，尝试用 JS 列出所有选项并找最佳匹配
    if not clicked:
        try:
            best = await page.evaluate(f"""() => {{
                const opts = document.querySelectorAll('.MuiAutocomplete-option, [role="option"]');
                const target = '{target}';
                for (const opt of opts) {{
                    const text = opt.textContent.trim();
                    // 必须包含 USDT 且以 target 开头 (如 "ETHUSDT")
                    if (text.includes('USDT') && text.startsWith(target)) {{
                        opt.click();
                        return text;
                    }}
                }}
                return null;
            }}""")
            if best:
                log.info(f"通过 JS 点击了选项: {best}")
                clicked = True
        except Exception:
            pass

    # 如果还是没点到，按 Enter 确认
    if not clicked:
        try:
            await page.keyboard.press("Enter")
            log.info("按 Enter 确认币种选择")
        except Exception:
            pass

    # 等待新数据加载
    await asyncio.sleep(8)
    log.info(f"币种切换完成: {target}")


async def _try_url_symbol_switch(page, symbol: str):
    """兜底: 尝试通过 URL 参数切换币种 (已弃用，保留以防万一)"""
    base_url = "https://www.coinglass.com/pro/futures/LiquidationHeatMap"
    candidates = [
        f"{base_url}?symbol={symbol}",
        f"{base_url}?coin={symbol}",
    ]
    for url in candidates:
        try:
            log.info(f"尝试 URL 切换: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(5)
            return
        except Exception as e:
            log.debug(f"URL {url} 失败: {e}")
            continue


# ============== ECharts 数据提取 (绕过 API 加密) ==============
ECHARTS_EXTRACT_JS = """
() => {
    const results = [];
    const seen = new Set();
    const diagnostics = {};

    // 找 React fiber key (不同 React 版本前缀不同)
    const reactFiberKey = (el) => Object.keys(el).find(k =>
        k.startsWith('__reactFiber$') || k.startsWith('__reactInternalInstance$')
    );

    // 深度搜索对象，找有 getOption 方法的对象 (echarts 实例)
    const findEchartsInstance = (root, maxDepth = 4) => {
        const visited = new WeakSet();
        const queue = [{obj: root, depth: 0, path: ''}];
        while (queue.length > 0) {
            const {obj, depth, path} = queue.shift();
            if (!obj || typeof obj !== 'object' || visited.has(obj)) continue;
            visited.add(obj);
            if (typeof obj.getOption === 'function' && typeof obj.setOption === 'function') {
                return {instance: obj, path};
            }
            if (depth >= maxDepth) continue;
            try {
                for (const key of Object.keys(obj)) {
                    try {
                        const val = obj[key];
                        if (val && typeof val === 'object') {
                            queue.push({obj: val, depth: depth + 1, path: path + '.' + key});
                        }
                    } catch(e) {}
                }
            } catch(e) {}
        }
        return null;
    };

    // 找所有可能的 ECharts 容器
    const allContainers = [];
    document.querySelectorAll('.echarts-for-react, [_echarts_instance_], [__echarts_instance__]').forEach(c => {
        allContainers.push(c);
    });
    // 也通过 canvas 找父容器
    document.querySelectorAll('canvas[data-zr-dom-id]').forEach(canvas => {
        let p = canvas.parentElement;
        while (p && !allContainers.includes(p)) {
            if (p.classList && p.classList.contains('echarts-for-react')) {
                allContainers.push(p);
                break;
            }
            if (p.hasAttribute && (p.hasAttribute('_echarts_instance_') || p.hasAttribute('__echarts_instance__'))) {
                allContainers.push(p);
                break;
            }
            p = p.parentElement;
            if (!p) break;
        }
    });
    diagnostics.container_count = allContainers.length;

    // 1. 尝试通过 React fiber 找 echarts 实例
    for (const container of allContainers) {
        const fiberKey = reactFiberKey(container);
        if (!fiberKey) continue;

        let fiber = container[fiberKey];
        let attempts = 0;
        let found = null;

        // 向上遍历 fiber 树
        while (fiber && attempts < 30 && !found) {
            attempts++;
            // 检查 stateNode (类组件实例)
            if (fiber.stateNode) {
                found = findEchartsInstance(fiber.stateNode, 3);
                if (found) found.source = 'fiber.stateNode';
            }
            // 检查 memoizedProps (可能有 echarts 实例传入)
            if (!found && fiber.memoizedProps) {
                // 直接检查 props.ref
                if (fiber.memoizedProps.ref && fiber.memoizedProps.ref.current) {
                    found = findEchartsInstance(fiber.memoizedProps.ref.current, 3);
                    if (found) found.source = 'fiber.memoizedProps.ref';
                }
                // 检查所有 props
                if (!found) {
                    found = findEchartsInstance(fiber.memoizedProps, 3);
                    if (found) found.source = 'fiber.memoizedProps';
                }
            }
            // 检查 memoizedState
            if (!found && fiber.memoizedState) {
                let state = fiber.memoizedState;
                let sa = 0;
                while (state && sa < 10 && !found) {
                    sa++;
                    if (state.memoizedState) {
                        found = findEchartsInstance(state.memoizedState, 2);
                        if (found) found.source = 'fiber.memoizedState';
                    }
                    state = state.next;
                }
            }
            fiber = fiber.return;
        }

        if (found && found.instance) {
            try {
                const inst = found.instance;
                const opt = inst.getOption();
                const id = container.getAttribute('_echarts_instance_')
                        || container.getAttribute('__echarts_instance__')
                        || `idx_${results.length}`;
                if (seen.has(id)) continue;
                seen.add(id);

                const info = {
                    container_id: id,
                    container_class: container.className,
                    echarts_source: found.source,
                    echarts_path: found.path,
                    title: opt.title && opt.title[0] ? opt.title[0].text : null,
                    series_count: opt.series ? opt.series.length : 0,
                    series: [],
                    xAxis_info: null,
                    yAxis_info: null,
                };

                if (opt.series) {
                    opt.series.forEach(s => {
                        info.series.push({
                            type: s.type,
                            name: s.name,
                            data_length: s.data ? s.data.length : 0,
                            full_data: s.data || [],
                        });
                    });
                }
                if (opt.xAxis && opt.xAxis[0]) {
                    info.xAxis_info = {
                        type: opt.xAxis[0].type,
                        name: opt.xAxis[0].name,
                        data_length: opt.xAxis[0].data ? opt.xAxis[0].data.length : 0,
                        data: opt.xAxis[0].data || [],
                    };
                }
                if (opt.yAxis && opt.yAxis[0]) {
                    info.yAxis_info = {
                        type: opt.yAxis[0].type,
                        name: opt.yAxis[0].name,
                        data_length: opt.yAxis[0].data ? opt.yAxis[0].data.length : 0,
                        data: opt.yAxis[0].data || [],
                    };
                }
                results.push(info);
            } catch(e) {
                results.push({error: e.message, source: found.source, container: container.className});
            }
        }
    }

    diagnostics.found_count = results.length;
    return {
        instance_count: results.length,
        echarts_available: results.length > 0,
        diagnostics,
        instances: results,
    };
}
"""


async def extract_echarts_data(page, output_dir: Path) -> bool:
    """
    从页面 ECharts 实例直接提取已解密渲染数据
    (CoinGlass 的 liqHeatMap API 返回加密 data 字段，但 ECharts 渲染时已解密)
    """
    log.info("尝试从 ECharts 实例提取已解密数据...")
    try:
        # 等待 ECharts 渲染
        await page.wait_for_selector(
            ".echarts-for-react, [_echarts_instance_], canvas[data-zr-dom-id]",
            timeout=20000,
        )
        await asyncio.sleep(3)  # 给渲染留时间

        result = await page.evaluate(ECHARTS_EXTRACT_JS)
        if not result or not result.get("instances"):
            diag = result.get("diagnostics", {}) if result else {}
            log.warning(
                f"ECharts 提取无结果 | echarts_available={result.get('echarts_available') if result else '?'} "
                f"| 诊断: {json.dumps(diag, ensure_ascii=False)}"
            )
            # 即使没拿到实例，也保存诊断信息
            if result:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
                diag_file = output_dir / f"echarts_diag_{ts}.json"
                diag_file.write_text(
                    json.dumps(result, ensure_ascii=False, indent=2, default=str),
                    encoding="utf-8",
                )
                log.info(f"[诊断] ECharts 诊断信息 -> {diag_file}")
            return False

        log.info(f"找到 {result['instance_count']} 个 ECharts 实例")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        echarts_file = output_dir / f"echarts_{ts}.json"
        echarts_file.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        log.info(f"[ECharts] 数据 -> {echarts_file} ({echarts_file.stat().st_size} bytes)")

        # 打印摘要
        for inst in result["instances"]:
            print("\n" + "-" * 60)
            print(f"  实例: {inst.get('container_class', '?')[:80]}")
            print(f"  标题: {inst.get('title')}")
            print(f"  series 数量: {inst.get('series_count', 0)}")
            for i, s in enumerate(inst.get("series", [])):
                print(f"  series[{i}] type={s.get('type')} name={s.get('name')} 数据点={s.get('data_length')}")
            if inst.get("xAxis_info"):
                xa = inst["xAxis_info"]
                print(f"  X轴: type={xa.get('type')} name={xa.get('name')} 档位={xa.get('data_length')}")
            if inst.get("yAxis_info"):
                ya = inst["yAxis_info"]
                print(f"  Y轴: type={ya.get('type')} name={ya.get('name')} 档位={ya.get('data_length')}")
        print("-" * 60 + "\n")
        return True
    except Exception as e:
        log.error(f"ECharts 提取失败: {e}", exc_info=True)
        return False


# ============== 入口 ==============
def parse_args():
    p = argparse.ArgumentParser(description="CoinGlass 交易所清算地图 JSON 拦截器")
    p.add_argument("--symbol", default=DEFAULT_CONFIG["symbol"],
                   help="币种 (BTC/ETH/SOL...)，默认 ETH")
    p.add_argument("--headed", action="store_true",
                   help="非 headless 模式 (调试用)")
    p.add_argument("--wait", type=int, default=DEFAULT_CONFIG["wait_seconds"],
                   help="等待秒数")
    p.add_argument("--out", default=DEFAULT_CONFIG["output_dir"],
                   help="输出目录")
    p.add_argument("--url", default=DEFAULT_CONFIG["url"],
                   help="目标页面 URL")
    p.add_argument("--cookies", default=DEFAULT_CONFIG["cookies_file"],
                   help="登录 cookies 文件 (login_helper.py / 手动导出，非 BTC 币种必需)")
    return p.parse_args()


def main():
    args = parse_args()
    config = dict(DEFAULT_CONFIG)
    config["symbol"] = args.symbol
    config["headless"] = not args.headed
    config["wait_seconds"] = args.wait
    config["output_dir"] = args.out
    config["url"] = args.url
    config["cookies_file"] = args.cookies

    count = asyncio.run(intercept_coinglass_heatmap(config))
    sys.exit(0 if count > 0 else 1)


if __name__ == "__main__":
    main()
