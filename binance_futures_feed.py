# -*- coding: utf-8 -*-
"""
币安合约数据源模块 (Binance Futures Feed)
=========================================
提供高精度合约行情与微观流动性指标：
1. 实时价格与 5m K线 (OHLCV + 主动买入量 Taker Volume)
2. 实时持仓量 (Open Interest - OI)
3. 5分钟持仓量变化趋势与增减量 (5m OI Delta & Change %)
4. 5分钟主动买卖量与买卖比率 (Taker Buy/Sell Volume & Buy/Sell Ratio)
5. 大户持仓量多空比 (Top Trader Long/Short Position Ratio)
6. 自动故障转移 (Failover) 与 本地代理适配 (支持直连 & 127.0.0.1:7890)
7. 毫秒级日内 Daily VWAP 及 ±1σ/±2σ/±3σ 轨道实时自算 (UTC 00:00 起算)
"""
import asyncio
import json
import math
import threading
import time
import requests
from datetime import datetime, timezone, timedelta

TZ_BJT = timezone(timedelta(hours=8))

# AUDIT_FIX_ASOF_001 helpers (lazy-safe import for standalone feed use)
try:
    from eth_predictor.asof import (
        filter_closed_klines, filter_completed_hist, as_of_filter,
        day_start_ms_bjt, now_ms, INTERVAL_MS,
    )
except Exception:  # pragma: no cover
    filter_closed_klines = None
    filter_completed_hist = None
    as_of_filter = None
    day_start_ms_bjt = None
    now_ms = None
    INTERVAL_MS = {"5m": 300000, "1h": 3600000, "1d": 86400000}


HOSTS = [
    "fapi.binance.com",
    "fapi.binance.vision",
]

DEFAULT_PROXIES = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}


class BinanceFuturesFeed:
    def __init__(self, symbol="ETHUSDT", interval="5m", timeout=6):
        self.symbol = symbol.upper()
        self.interval = interval
        self.timeout = timeout
        self.host = HOSTS[0]
        self.use_proxy = False
        self.proxies = None

        # 实时数据缓存
        self.price = None
        self.klines = []           # 5m K线 [[open_ms, o, h, l, c, vol, ...], ...]
        self.klines_1h = []        # 1h K线
        self.klines_1d = []        # 1d K线
        self.last_price_time = 0.0
        self.last_htf_update = 0.0

        # WebSocket 实时流状态
        self.ws_connected = False
        self.last_ws_message_time = 0.0
        self.ws_thread = None
        self._stop_ws = threading.Event()
        self.data_lock = threading.RLock()

        # 实时爆仓事件流缓存 (@forceOrder)
        self.liquidations_history = []
        self.last_force_order_time = 0.0

        # BTC 领先滞后先导资产监控 (BTC Lead-Lag Alpha)
        self.btc_price = None
        self.btc_klines_5m = []
        self.last_btc_kline_time = 0.0

        # 量仓高级数据缓存
        self.oi_current = None        # 实时总持仓量 (ETH)
        self.oi_delta_5m = 0.0        # 过去 5 分钟持仓增减量 (ETH)
        self.oi_delta_1h = 0.0        # 过去 1 小时真实持仓增减量 (ETH)
        self.oi_delta_1d = 0.0        # 过去 1 日 (24h) 真实持仓增减量 (ETH)
        self.oi_change_pct = 0.0      # 过去 5 分钟持仓变化百分比 %
        self.taker_buy_vol_5m = 0.0   # 过去 5 分钟主动买量 (ETH)
        self.taker_sell_vol_5m = 0.0  # 过去 5 分钟主动卖量 (ETH)
        self.buy_sell_ratio_5m = 1.0  # 主动买卖比 (>1 买盘主动, <1 卖盘主动)
        self.top_long_pct = 50.0      # 大户多仓占比 %
        self.top_short_pct = 50.0     # 大户空仓占比 %
        self.top_ls_ratio = 1.0       # 大户多空比
        self.global_ls_ratio = 1.0    # 全网散户/大户账户多空比 (动态拉取)
        self.funding_rate = 0.0001    # 实时资金费率 (动态拉取)
        self.vol_ma20 = 0.0           # 20根 5m K线均量
        self.vol_ratio = 1.0          # 当前量比 (最近已收盘K线成交量 / 20周期均量)

        # AUDIT_FIX_ASOF_001: point-in-time hist caches + WS closed flags
        self.oi_hist_5m = []
        self.oi_hist_1h = []
        self.oi_hist_1d = []
        self.taker_hist_5m = []
        self.top_ls_hist_5m = []
        self.global_ls_hist_5m = []
        self._last_kline_closed = None   # last ETH 5m k.x from WS
        self._last_btc_kline_closed = None

        self.last_heavy_update = 0.0  # 上次更新量仓衍生指标的时间
        self.last_ok_time = 0.0
        self.error = None

        self.session = requests.Session()
        self._auto_detect_connection()

    def _auto_detect_connection(self):
        """自动检测网络：直连优先，若直连不通则尝试代理 127.0.0.1:7890"""
        for h in HOSTS:
            try:
                r = self.session.get(f"https://{h}/fapi/v1/ping", timeout=3)
                if r.status_code == 200:
                    self.host = h
                    self.use_proxy = False
                    self.proxies = None
                    return
            except Exception:
                continue

        try:
            r = self.session.get(f"https://{HOSTS[0]}/fapi/v1/ping", proxies=DEFAULT_PROXIES, timeout=4)
            if r.status_code == 200:
                self.host = HOSTS[0]
                self.use_proxy = True
                self.proxies = DEFAULT_PROXIES
                return
        except Exception:
            pass

        self.host = HOSTS[0]
        self.proxies = DEFAULT_PROXIES
        self.use_proxy = True

    def _request(self, path, params=None, timeout=None):
        """统一请求封装，带域名重试与代理适配 (基于 Session 长连接复用)"""
        last_exc = None
        for attempt in range(2):
            for h in HOSTS:
                url = f"https://{h}{path}"
                try:
                    r = self.session.get(
                        url,
                        params=params,
                        proxies=self.proxies if self.use_proxy else None,
                        timeout=timeout or self.timeout
                    )
                    r.raise_for_status()
                    self.last_ok_time = time.time()
                    return r.json()
                except Exception as e:
                    last_exc = e
            if attempt == 0 and not self.use_proxy:
                self.use_proxy = True
                self.proxies = DEFAULT_PROXIES
        raise last_exc

    def start_websocket(self):
        """启动币安官方 WebSocket 实时数据流 (毫秒级 aggTrade + 5m K线实时推送)"""
        if self.ws_thread and self.ws_thread.is_alive():
            return
        self._stop_ws.clear()
        self.ws_thread = threading.Thread(target=self._run_websocket_thread, daemon=True, name="BinanceWSFeed")
        self.ws_thread.start()

    def stop_websocket(self):
        """安全停止 WebSocket"""
        self._stop_ws.set()

    def _run_websocket_thread(self):
        """后台独立事件循环运行 WebSocket 订阅"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._ws_stream_loop())
        finally:
            loop.close()

    async def _ws_stream_loop(self):
        import websockets
        sym = self.symbol.lower()
        ws_url = f"wss://fstream.binance.com/market/stream?streams={sym}@aggTrade/{sym}@kline_{self.interval}/{sym}@forceOrder/btcusdt@aggTrade/btcusdt@kline_5m"
        print(f"[*] 启动币安合约官方 WebSocket 实时流 (含 forceOrder 爆仓与 BTC 先导): {ws_url}", flush=True)

        while not self._stop_ws.is_set():
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**22
                ) as ws:
                    self.ws_connected = True
                    print(f"[WebSocket] ✅ 币安合约实时流已成功建立！实时成交、5m K线、毫秒级爆仓与 BTC 先导流已启动", flush=True)
                    while not self._stop_ws.is_set():
                        msg = await asyncio.wait_for(ws.recv(), timeout=10.0)
                        self._handle_ws_message(msg)
            except Exception as e:
                self.ws_connected = False
                if not self._stop_ws.is_set():
                    await asyncio.sleep(2.0)

    def _handle_ws_message(self, raw_msg):
        """毫秒级处理 WebSocket 推送：实时刷新价格、当前 K 线极值、爆仓订单流及 BTC 先导"""
        try:
            data = json.loads(raw_msg)
            stream = data.get("stream", "")
            payload = data.get("data", {})
            now = time.time()
            sym = self.symbol.lower()

            with self.data_lock:
                if f"{sym}@aggTrade" in stream or (stream == "" and payload.get("s", "").lower() == sym and payload.get("e") == "aggTrade"):
                    px = float(payload.get("p", 0.0))
                    if px > 0:
                        self.price = px
                        self.last_price_time = now
                        self.last_ws_message_time = now
                        self.last_ok_time = now
                        # 实时同步刷新最新 5m K线的极值与收盘价
                        if self.klines:
                            cur_k = self.klines[-1]
                            cur_k[4] = px
                            cur_k[2] = max(float(cur_k[2]), px)
                            cur_k[3] = min(float(cur_k[3]), px)

                elif "btcusdt@aggTrade" in stream:
                    b_px = float(payload.get("p", 0.0))
                    if b_px > 0:
                        self.btc_price = b_px
                        if self.btc_klines_5m:
                            cur_b = self.btc_klines_5m[-1]
                            cur_b[4] = b_px
                            cur_b[2] = max(float(cur_b[2]), b_px)
                            cur_b[3] = min(float(cur_b[3]), b_px)

                elif f"{sym}@kline" in stream:
                    k = payload.get("k", {})
                    if k:
                        open_time = int(k.get("t", 0))
                        is_closed = bool(k.get("x", False))  # Binance k.x — bar closed
                        self._last_kline_closed = is_closed
                        candle = [
                            open_time,
                            float(k.get("o", 0.0)),
                            float(k.get("h", 0.0)),
                            float(k.get("l", 0.0)),
                            float(k.get("c", 0.0)),
                            float(k.get("v", 0.0)),
                            int(k.get("T", 0)),
                            float(k.get("q", 0.0)),
                            int(k.get("n", 0)),
                            float(k.get("V", 0.0)),
                            float(k.get("Q", 0.0)),
                            k.get("B", "0"),
                        ]
                        # Raw buffer keeps forming bar for live price / TP tracking.
                        # Feature accessors (get_closed_klines) exclude unclosed bars.
                        if not self.klines:
                            self.klines = [candle]
                        elif open_time == self.klines[-1][0]:
                            self.klines[-1] = candle
                        elif open_time > self.klines[-1][0]:
                            self.klines.append(candle)
                            self.klines = self.klines[-288:]

                        self.price = float(k.get("c", self.price or 0.0))
                        self.last_price_time = now
                        self.last_ws_message_time = now
                        self.last_ok_time = now

                        self._refresh_vol_ratio_closed()

                elif "btcusdt@kline" in stream:
                    k = payload.get("k", {})
                    if k:
                        open_time = int(k.get("t", 0))
                        self._last_btc_kline_closed = bool(k.get("x", False))
                        candle = [
                            open_time,
                            float(k.get("o", 0.0)),
                            float(k.get("h", 0.0)),
                            float(k.get("l", 0.0)),
                            float(k.get("c", 0.0)),
                            float(k.get("v", 0.0)),
                            int(k.get("T", 0)),
                            float(k.get("q", 0.0)),
                            int(k.get("n", 0)),
                            float(k.get("V", 0.0)),
                            float(k.get("Q", 0.0)),
                            k.get("B", "0"),
                        ]
                        if not self.btc_klines_5m:
                            self.btc_klines_5m = [candle]
                        elif open_time == self.btc_klines_5m[-1][0]:
                            self.btc_klines_5m[-1] = candle
                        elif open_time > self.btc_klines_5m[-1][0]:
                            self.btc_klines_5m.append(candle)
                            self.btc_klines_5m = self.btc_klines_5m[-60:]
                        self.btc_price = float(k.get("c", self.btc_price or 0.0))

                elif "forceOrder" in stream or payload.get("e") == "forceOrder":
                    o = payload.get("o", {})
                    if o:
                        side = o.get("S", "SELL")  # SELL=多头爆仓, BUY=空头爆仓
                        qty = float(o.get("q", 0.0))
                        liq_px = float(o.get("p", 0.0))
                        liq_time = float(o.get("T", now * 1000)) / 1000.0
                        liq_type = "LONG_LIQ" if side == "SELL" else "SHORT_LIQ"
                        self.liquidations_history.append({
                            "time": liq_time,
                            "type": liq_type,
                            "side": side,
                            "qty": qty,
                            "price": liq_px,
                            "usd_vol": round(qty * liq_px, 2)
                        })
                        self.last_force_order_time = now
                        cutoff = now - 3600
                        if len(self.liquidations_history) > 300:
                            self.liquidations_history = [x for x in self.liquidations_history if x["time"] >= cutoff]
        except Exception:
            pass

    def get_realtime_liquidation_stats(self, lookback_seconds=300):
        """获取最近 lookback 秒内的实时真实爆仓量与多空倾斜统计"""
        now = time.time()
        cutoff = now - lookback_seconds
        with self.data_lock:
            recent = [x for x in self.liquidations_history if x["time"] >= cutoff]

        long_vol = sum(x["qty"] for x in recent if x["type"] == "LONG_LIQ")
        short_vol = sum(x["qty"] for x in recent if x["type"] == "SHORT_LIQ")
        total_vol = long_vol + short_vol
        net_bias = round((short_vol - long_vol) / total_vol, 3) if total_vol > 0 else 0.0

        # 连环爆仓踩踏预警 (30秒内爆仓量 > 150 ETH 或 单笔爆仓 > 80 ETH)
        recent_30s = [x for x in recent if x["time"] >= (now - 30)]
        vol_30s = sum(x["qty"] for x in recent_30s)
        has_cascade = (vol_30s >= 150.0) or any(x["qty"] >= 80.0 for x in recent_30s)
        last_ev = recent[-1] if recent else None

        return {
            "lookback_seconds": lookback_seconds,
            "long_liq_vol_eth": round(long_vol, 2),
            "short_liq_vol_eth": round(short_vol, 2),
            "total_liq_vol_eth": round(total_vol, 2),
            "net_bias": net_bias,
            "cascade_alert": has_cascade,
            "count": len(recent),
            "last_event": last_ev
        }

    def _refresh_vol_ratio_closed(self):
        """Volume ratio from last CLOSED 5m bar only (AUDIT_FIX_ASOF_001)."""
        closed = self.get_closed_klines("5m")
        if len(closed) >= 20:
            vols = [float(item[5]) for item in closed[-20:]]
            self.vol_ma20 = sum(vols) / len(vols) if vols else 1.0
            cur_vol = float(closed[-1][5])
            self.vol_ratio = round(cur_vol / self.vol_ma20, 2) if self.vol_ma20 > 0 else 1.0
        elif len(closed) >= 2:
            vols = [float(item[5]) for item in closed[:-1]]
            self.vol_ma20 = (sum(vols) / len(vols)) if vols else 1.0
            cur_vol = float(closed[-1][5])
            self.vol_ratio = round(cur_vol / self.vol_ma20, 2) if self.vol_ma20 > 0 else 1.0

    def get_closed_klines(self, tf="5m", as_of_ms=None):
        """
        Return only bars with close_time <= as_of for the given TF.
        Incomplete 1h/1d bars must not pollute features.
        """
        with self.data_lock:
            if tf == "1h":
                raw = list(self.klines_1h or [])
                interval = "1h"
            elif tf == "1d":
                raw = list(self.klines_1d or [])
                interval = "1d"
            elif tf == "btc_5m":
                raw = list(self.btc_klines_5m or [])
                interval = "5m"
            else:
                raw = list(self.klines or [])
                interval = "5m"
        if filter_closed_klines is not None:
            return filter_closed_klines(raw, as_of_ms=as_of_ms, interval=interval)
        # Fallback without asof module: drop last bar if its close_time is in the future
        t = int(as_of_ms) if as_of_ms is not None else int(time.time() * 1000)
        out = []
        dur = INTERVAL_MS.get(interval, 300000)
        for k in raw:
            try:
                ct = int(float(k[6])) if len(k) > 6 and k[6] is not None else int(float(k[0])) + dur - 1
                if ct <= t:
                    out.append(k)
            except Exception:
                continue
        return out

    def get_microstructure_asof(self, as_of_ms=None):
        """
        Point-in-time OI / volume / long-short ratios (timestamp <= T, completed buckets).
        Live path uses wall-clock when as_of_ms is None.
        """
        t = int(as_of_ms) if as_of_ms is not None else int(time.time() * 1000)

        def _completed(hist, period):
            if filter_completed_hist is not None:
                return filter_completed_hist(hist, period=period, as_of_ms=t)
            return list(hist or [])

        oi5 = _completed(self.oi_hist_5m, "5m")
        oi1h = _completed(self.oi_hist_1h, "1h")
        oi1d = _completed(self.oi_hist_1d, "1d")
        taker = _completed(self.taker_hist_5m, "5m")
        top = _completed(self.top_ls_hist_5m, "5m")
        glob = _completed(self.global_ls_hist_5m, "5m")

        # Live realtime OI is valid point-in-time at wall clock; for historical as_of use last completed hist.
        is_live = abs(t - int(time.time() * 1000)) <= 5000
        oi_current = self.oi_current
        if not is_live:
            series = oi5 or oi1h or oi1d
            if series:
                oi_current = float(series[-1].get("sumOpenInterest", oi_current or 0.0))

        oi_delta_5m = 0.0
        oi_change_pct = 0.0
        if oi5 and len(oi5) >= 2:
            prev_oi = float(oi5[-2]["sumOpenInterest"])
            cur_oi = float(oi_current) if (is_live and oi_current) else float(oi5[-1]["sumOpenInterest"])
            oi_delta_5m = round(cur_oi - prev_oi, 2)
            oi_change_pct = round((oi_delta_5m / prev_oi) * 100.0, 3) if prev_oi > 0 else 0.0
        elif is_live:
            oi_delta_5m = self.oi_delta_5m
            oi_change_pct = self.oi_change_pct

        oi_delta_1h = 0.0
        if oi1h and len(oi1h) >= 2:
            prev = float(oi1h[-2]["sumOpenInterest"])
            cur = float(oi_current) if (is_live and oi_current) else float(oi1h[-1]["sumOpenInterest"])
            oi_delta_1h = round(cur - prev, 2)
        elif is_live:
            oi_delta_1h = self.oi_delta_1h

        oi_delta_1d = 0.0
        if oi1d and len(oi1d) >= 2:
            prev = float(oi1d[-2]["sumOpenInterest"])
            cur = float(oi_current) if (is_live and oi_current) else float(oi1d[-1]["sumOpenInterest"])
            oi_delta_1d = round(cur - prev, 2)
        elif is_live:
            oi_delta_1d = self.oi_delta_1d

        buy_sell_ratio_5m = self.buy_sell_ratio_5m
        taker_buy = self.taker_buy_vol_5m
        taker_sell = self.taker_sell_vol_5m
        if taker:
            latest = taker[-1]
            taker_buy = round(float(latest.get("buyVol", taker_buy)), 2)
            taker_sell = round(float(latest.get("sellVol", taker_sell)), 2)
            buy_sell_ratio_5m = round(float(latest.get("buySellRatio", buy_sell_ratio_5m)), 4)

        top_ls_ratio = self.top_ls_ratio
        top_long_pct = self.top_long_pct
        top_short_pct = self.top_short_pct
        if top:
            latest = top[-1]
            top_long_pct = round(float(latest.get("longAccount", top_long_pct / 100.0)) * 100.0, 2)
            top_short_pct = round(float(latest.get("shortAccount", top_short_pct / 100.0)) * 100.0, 2)
            top_ls_ratio = round(float(latest.get("longShortRatio", top_ls_ratio)), 4)

        global_ls_ratio = self.global_ls_ratio
        if glob:
            global_ls_ratio = round(float(glob[-1].get("longShortRatio", global_ls_ratio)), 4)

        return {
            "oi_current": oi_current,
            "oi_delta_5m": oi_delta_5m,
            "oi_delta_1h": oi_delta_1h,
            "oi_delta_1d": oi_delta_1d,
            "oi_change_pct": oi_change_pct,
            "taker_buy_vol_5m": taker_buy,
            "taker_sell_vol_5m": taker_sell,
            "buy_sell_ratio_5m": buy_sell_ratio_5m,
            "top_long_pct": top_long_pct,
            "top_short_pct": top_short_pct,
            "top_ls_ratio": top_ls_ratio,
            "global_ls_ratio": global_ls_ratio,
            "funding_rate": self.funding_rate,
            "vol_ratio": self.vol_ratio,
            "as_of_ms": t,
        }

    def poll_price_and_klines(self):

        """
        价格与 5m K线获取策略：
        1. 优先使用 WebSocket 实时数据流 (毫秒级，无慢速 REST 延迟)；
        2. 若 WebSocket 断开或首次启动尚未就绪，无缝由 REST API 补全与兜底。
        """
        now = time.time()
        # 若 WebSocket 正常连通且最近 5 秒有数据，直接使用内存实时流数据，免除慢速 HTTP 轮询
        if self.ws_connected and (now - self.last_ws_message_time < 5.0) and self.price and len(self.klines) >= 20:
            return

        # 兜底或首次加载：通过 REST API 补齐完整历史
        try:
            px_data = self._request("/fapi/v1/ticker/price", {"symbol": self.symbol}, timeout=4)
            with self.data_lock:
                self.price = float(px_data["price"])
                self.last_price_time = now

            if not self.klines or now - getattr(self, "_last_full_kline", 0) >= 60:
                ks = self._request("/fapi/v1/klines", {
                    "symbol": self.symbol, "interval": self.interval, "limit": 288
                }, timeout=5)
                with self.data_lock:
                    self.klines = ks
                self._last_full_kline = now
            else:
                ks = self._request("/fapi/v1/klines", {
                    "symbol": self.symbol, "interval": self.interval, "limit": 2
                }, timeout=4)
                with self.data_lock:
                    for k in ks:
                        if self.klines and k[0] == self.klines[-1][0]:
                            self.klines[-1] = k
                        elif not self.klines or k[0] > self.klines[-1][0]:
                            self.klines.append(k)
                            self.klines = self.klines[-288:]

            with self.data_lock:
                self._refresh_vol_ratio_closed()

            # 补充初始化 BTCUSDT 5m 行情 (每 120 秒刷新一次或首次启动补齐)
            if not self.btc_price or not self.btc_klines_5m or now - getattr(self, "_last_btc_kline_poll", 0) >= 120:
                try:
                    btc_px_data = self._request("/fapi/v1/ticker/price", {"symbol": "BTCUSDT"}, timeout=3)
                    btc_ks = self._request("/fapi/v1/klines", {"symbol": "BTCUSDT", "interval": "5m", "limit": 48}, timeout=4)
                    with self.data_lock:
                        self.btc_price = float(btc_px_data["price"])
                        self.btc_klines_5m = btc_ks
                    self._last_btc_kline_poll = now
                except Exception:
                    pass

            self.error = None
        except Exception as e:
            if not self.price:
                self.error = f"行情拉取失败: {e}"
                raise

    def get_btc_lead_lag_stats(self, as_of_ms=None):
        """计算 BTC 领先滞后先导溢出指标 (BTC Lead-Lag Alpha)

        AUDIT_FIX_ASOF_001:
          - Filter closed 5m klines as-of T
          - Historical replay: use T-time last closed closes (forbid wall-clock spot)
          - Live: may use live spot vs last closed bar
        """
        t_ms = int(as_of_ms) if as_of_ms is not None else int(time.time() * 1000)
        wall_ms = int(time.time() * 1000)
        is_historical = as_of_ms is not None and abs(t_ms - wall_ms) > 5000

        with self.data_lock:
            live_btc = self.btc_price
            live_eth = self.price
            btc_kl_raw = list(self.btc_klines_5m)
            eth_kl_raw = list(self.klines)

        if filter_closed_klines is not None:
            btc_kl = filter_closed_klines(btc_kl_raw, as_of_ms=t_ms, interval="5m")
            eth_kl = filter_closed_klines(eth_kl_raw, as_of_ms=t_ms, interval="5m")
        else:
            # Fallback: keep bars whose close_time (idx 6) or open+5m-1 <= t_ms
            def _closed(raw):
                out = []
                for k in raw:
                    try:
                        ct = int(float(k[6])) if len(k) > 6 and k[6] is not None else int(float(k[0])) + 299999
                        if ct <= t_ms:
                            out.append(k)
                    except Exception:
                        continue
                return out
            btc_kl = _closed(btc_kl_raw)
            eth_kl = _closed(eth_kl_raw)

        if is_historical:
            btc_px = float(btc_kl[-1][4]) if btc_kl else 0.0
            eth_px = float(eth_kl[-1][4]) if eth_kl else 0.0
        else:
            btc_px = float(live_btc) if live_btc else (float(btc_kl[-1][4]) if btc_kl else 0.0)
            eth_px = float(live_eth) if live_eth else (float(eth_kl[-1][4]) if eth_kl else 0.0)

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

        # 计算 BTC 5m 涨跌幅
        btc_change_5m = 0.0
        if is_historical:
            if len(btc_kl) >= 2:
                prev_btc = float(btc_kl[-2][4])
                if prev_btc > 0:
                    btc_change_5m = round(((btc_px - prev_btc) / prev_btc) * 100.0, 3)
            elif btc_kl:
                open_btc = float(btc_kl[-1][1])
                if open_btc > 0:
                    btc_change_5m = round(((btc_px - open_btc) / open_btc) * 100.0, 3)
        else:
            # Live: spot vs last closed bar
            if btc_kl:
                prev_btc = float(btc_kl[-1][4])
                if prev_btc > 0:
                    btc_change_5m = round(((btc_px - prev_btc) / prev_btc) * 100.0, 3)

        # 计算 ETH 5m 涨跌幅
        eth_change_5m = 0.0
        if is_historical:
            if len(eth_kl) >= 2:
                prev_eth = float(eth_kl[-2][4])
                if prev_eth > 0:
                    eth_change_5m = round(((eth_px - prev_eth) / prev_eth) * 100.0, 3)
            elif eth_kl:
                open_eth = float(eth_kl[-1][1])
                if open_eth > 0:
                    eth_change_5m = round(((eth_px - open_eth) / open_eth) * 100.0, 3)
        else:
            if eth_kl:
                prev_eth = float(eth_kl[-1][4])
                if prev_eth > 0:
                    eth_change_5m = round(((eth_px - prev_eth) / prev_eth) * 100.0, 3)

        # 领先滞后剪刀差 (BTC 涨幅 - ETH 涨幅)
        div = round(btc_change_5m - eth_change_5m, 3)

        # 溢出分 (-1.0 ~ +1.0)
        spillover_score = max(-1.0, min(1.0, round(div / 0.45, 3)))

        if div >= 0.18 and btc_change_5m > 0.12:
            signal = "BULLISH_CATCHUP"
            text = f"BTC 领涨溢出 (+{btc_change_5m:.2f}%)，ETH 滞后补涨差 +{div:.2f}%"
        elif div <= -0.18 and btc_change_5m < -0.12:
            signal = "BEARISH_DRAG"
            text = f"BTC 领跌拖拽 ({btc_change_5m:.2f}%)，ETH 面临拖拽补跌差 {div:.2f}%"
        elif btc_change_5m > 0.30 and eth_change_5m > 0.25:
            signal = "BULLISH_RESONANCE"
            text = f"BTC 与 ETH 强共振放量拉升 (BTC +{btc_change_5m:.2f}%)"
        elif btc_change_5m < -0.30 and eth_change_5m < -0.25:
            signal = "BEARISH_RESONANCE"
            text = f"BTC 与 ETH 强共振破位跳水 (BTC {btc_change_5m:.2f}%)"
        else:
            signal = "SYNCHRONIZED"
            text = f"BTC 与 ETH 走势同步 (BTC {btc_change_5m:+.2f}% / ETH {eth_change_5m:+.2f}%)"

        return {
            "btc_price": round(btc_px, 2),
            "eth_price": round(eth_px, 2),
            "btc_change_5m_pct": btc_change_5m,
            "eth_change_5m_pct": eth_change_5m,
            "divergence_pct": div,
            "lead_signal": signal,
            "spillover_score": spillover_score,
            "status_text": text,
            "as_of_ms": t_ms,
        }

    def poll_volume_and_oi(self, force=False):
        """低频轮询：持仓量、OI Delta、主动买卖比、大户持仓多空比 (每 10-15 秒一次)
        AUDIT_FIX_ASOF_001: cache hist series; derive deltas from completed buckets (timestamp<=now).
        """
        now = time.time()
        if not force and now - self.last_heavy_update < 12.0:
            return

        try:
            # A. 实时持仓量 (Open Interest) — point-in-time at wall clock
            oi_data = self._request("/fapi/v1/openInterest", {"symbol": self.symbol}, timeout=4)
            self.oi_current = float(oi_data.get("openInterest", 0.0))

            # B. 5m 持仓量历史 (计算 5m OI Delta) — keep series for as-of replay
            oi_hist = self._request("/futures/data/openInterestHist", {
                "symbol": self.symbol, "period": "5m", "limit": 30
            }, timeout=4)
            if oi_hist:
                self.oi_hist_5m = oi_hist

            # B2. 1h 真实持仓量历史
            try:
                oi_hist_1h = self._request("/futures/data/openInterestHist", {
                    "symbol": self.symbol, "period": "1h", "limit": 30
                }, timeout=4)
                if oi_hist_1h:
                    self.oi_hist_1h = oi_hist_1h
            except Exception:
                pass

            # B3. 1d / 24h 真实持仓量历史
            try:
                oi_hist_1d = self._request("/futures/data/openInterestHist", {
                    "symbol": self.symbol, "period": "1d", "limit": 14
                }, timeout=4)
                if oi_hist_1d:
                    self.oi_hist_1d = oi_hist_1d
            except Exception:
                pass

            # C. 5m 主动买卖量
            taker_hist = self._request("/futures/data/takerlongshortRatio", {
                "symbol": self.symbol, "period": "5m", "limit": 30
            }, timeout=4)
            if taker_hist:
                self.taker_hist_5m = taker_hist

            # D. 大户持仓量多空比
            top_hist = self._request("/futures/data/topLongShortPositionRatio", {
                "symbol": self.symbol, "period": "5m", "limit": 30
            }, timeout=4)
            if top_hist:
                self.top_ls_hist_5m = top_hist

            # E. 全网账户多空比
            try:
                global_hist = self._request("/futures/data/globalLongShortAccountRatio", {
                    "symbol": self.symbol, "period": "5m", "limit": 30
                }, timeout=4)
                if global_hist:
                    self.global_ls_hist_5m = global_hist
            except Exception:
                pass

            # F. 实时资金费率
            try:
                funding_info = self._request("/fapi/v1/premiumIndex", {
                    "symbol": self.symbol
                }, timeout=4)
                if funding_info and isinstance(funding_info, dict) and "lastFundingRate" in funding_info:
                    self.funding_rate = round(float(funding_info["lastFundingRate"]), 6)
            except Exception:
                pass

            # Derive live fields from completed buckets + realtime OI (as-of now)
            micro = self.get_microstructure_asof(as_of_ms=int(now * 1000))
            self.oi_delta_5m = micro["oi_delta_5m"]
            self.oi_change_pct = micro["oi_change_pct"]
            self.oi_delta_1h = micro["oi_delta_1h"]
            self.oi_delta_1d = micro["oi_delta_1d"]
            self.taker_buy_vol_5m = micro["taker_buy_vol_5m"]
            self.taker_sell_vol_5m = micro["taker_sell_vol_5m"]
            self.buy_sell_ratio_5m = micro["buy_sell_ratio_5m"]
            self.top_long_pct = micro["top_long_pct"]
            self.top_short_pct = micro["top_short_pct"]
            self.top_ls_ratio = micro["top_ls_ratio"]
            self.global_ls_ratio = micro["global_ls_ratio"]

            self.last_heavy_update = now
        except Exception as e:
            self.error = f"量仓指标更新失败: {e}"

    def poll_higher_timeframe_klines(self, force=False):
        """定期拉取 1h 与 1d 真实 K 线 (每 60 秒轮询一次，保障均线彩带与技术指标所需样本容量)"""
        now = time.time()
        if not force and now - getattr(self, "last_htf_update", 0.0) < 60.0:
            return
        try:
            # 1H K线拉取 100 根 (充足覆盖 EMA55 彩带与 RSI/ATR)
            ks_1h = self._request("/fapi/v1/klines", {
                "symbol": self.symbol, "interval": "1h", "limit": 100
            }, timeout=5)
            if ks_1h and isinstance(ks_1h, list) and len(ks_1h) >= 55:
                self.klines_1h = ks_1h

            # 1D K线拉取 30 根 (充足覆盖 14 天指标计算)
            ks_1d = self._request("/fapi/v1/klines", {
                "symbol": self.symbol, "interval": "1d", "limit": 30
            }, timeout=5)
            if ks_1d and isinstance(ks_1d, list) and len(ks_1d) >= 14:
                self.klines_1d = ks_1d

            self.last_htf_update = now
        except Exception:
            pass

    def calc_daily_vwap(self, as_of_ms=None):
        """
        计算今日北京时间 00:00 (UTC+8) 至 as_of T 的日内真实 Daily VWAP 及 ±1σ/±2σ/±3σ 轨道
        AUDIT_FIX_ASOF_001: only closed 5m bars; accumulate day-start -> T (no end-of-window leak).
        """
        closed = self.get_closed_klines("5m", as_of_ms=as_of_ms)
        if not closed:
            return None
        t_ms = int(as_of_ms) if as_of_ms is not None else int(time.time() * 1000)
        if day_start_ms_bjt is not None:
            today_start_ms = day_start_ms_bjt(t_ms)
        else:
            now_bjt = datetime.fromtimestamp(t_ms / 1000.0, tz=TZ_BJT)
            today_start_ms = int(datetime(now_bjt.year, now_bjt.month, now_bjt.day, tzinfo=TZ_BJT).timestamp() * 1000)

        today_ks = [k for k in closed if k[0] >= today_start_ms]
        if len(today_ks) < 12:
            today_ks = closed[-48:]

        cum_vol = 0.0
        cum_tp_vol = 0.0
        kl_data = []
        vwap_series = []
        for k in today_ks:
            h, l, c, v = float(k[2]), float(k[3]), float(k[4]), float(k[5])
            tp = (h + l + c) / 3.0
            cum_vol += v
            cum_tp_vol += tp * v
            kl_data.append((tp, v))
            vwap_series.append((cum_tp_vol / cum_vol) if cum_vol > 0 else c)

        if cum_vol <= 0:
            return None

        vwap = round(cum_tp_vol / cum_vol, 2)
        sum_sq = sum(v * ((tp - vwap) ** 2) for tp, v in kl_data)
        sigma = math.sqrt(sum_sq / cum_vol)
        sigma_round = round(sigma, 2)
        # AUDIT_FIX_ASOF_001: historical as_of must use as-of / last closed close — never wall-clock self.price
        last_close = float(today_ks[-1][4])
        wall_ms = int(time.time() * 1000)
        is_historical = as_of_ms is not None and abs(t_ms - wall_ms) > 5000
        if is_historical:
            cur_px = last_close
        else:
            cur_px = float(self.price) if self.price else last_close
        z_score = round((cur_px - vwap) / sigma, 3) if sigma > 0 else 0.0
        slope = round(vwap_series[-1] - vwap_series[-6], 2) if len(vwap_series) >= 6 else 0.0

        u1 = round(vwap + sigma, 2)
        l1 = round(vwap - sigma, 2)
        u2 = round(vwap + 2 * sigma, 2)
        l2 = round(vwap - 2 * sigma, 2)
        u3 = round(vwap + 3 * sigma, 2)
        l3 = round(vwap - 3 * sigma, 2)
        mid_upper = round((u1 + u2) / 2.0, 2)
        mid_lower = round((l1 + l2) / 2.0, 2)

        return {
            "vwap": vwap,
            "sigma": sigma_round,
            "z_score": z_score,
            "slope": slope,
            "price": cur_px,
            "u1": u1, "l1": l1,
            "u2": u2, "l2": l2,
            "u3": u3, "l3": l3,
            "mid_upper": mid_upper,
            "mid_lower": mid_lower,
            "n_candles": len(today_ks),
            "source": "Binance Daily VWAP (北京 00:00, closed bars as-of-T)",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "age": 0,
            "stale": False,
            "as_of_ms": t_ms,
        }

    def get_market_sentiment(self):
        """
        量仓多空定性分析（严格区分多仓与空仓）：
        返回结构体包含：
          - regime: 'BULL_ATTACK' (多头主动进攻) | 'BEAR_ATTACK' (空头主动进攻)
                    | 'SHORT_SQUEEZE' (空头逼空踩踏/平仓) | 'LONG_FLUSH' (多头割肉出清)
                    | 'NEUTRAL' (中性震荡)
          - text: 人类可读分析
          - can_short: 是否适合开空 (布尔)
          - can_long: 是否适合开多 (布尔)
        """
        ratio = self.buy_sell_ratio_5m
        delta = self.oi_delta_5m
        delta_pct = self.oi_change_pct

        if ratio >= 1.25 and delta > 0:
            return {
                "regime": "BULL_ATTACK",
                "label": "多头主动进攻",
                "color": "#26a69a",
                "text": f"主动买单占优(买卖比 {ratio:.2f}) + 增仓(+{delta:.0f} ETH)，主力真金白银建多仓",
                "can_short": False,  # 强势多头，严禁摸顶开空
                "can_long": True,
            }
        elif ratio <= 0.80 and delta > 0:
            return {
                "regime": "BEAR_ATTACK",
                "label": "空头主动进攻",
                "color": "#ef5350",
                "text": f"主动卖单砸盘(买卖比 {ratio:.2f}) + 增仓(+{delta:.0f} ETH)，主力真金白银建空仓",
                "can_short": True,
                "can_long": False,  # 强势空头，严禁抄底接飞刀
            }
        elif delta < -200 and ratio <= 0.95:
            return {
                "regime": "LONG_FLUSH",
                "label": "多头出清/踩踏释放",
                "color": "#d4a94e",
                "text": f"持仓剧降({delta:.0f} ETH) + 卖压释放，多头杠杆盘被清洗，下行减速",
                "can_short": False,  # 踩踏已至末期，不可追空
                "can_long": True,   # 反弹/均值回归机会
            }
        elif delta < -200 and ratio >= 1.05:
            return {
                "regime": "SHORT_SQUEEZE",
                "label": "空头平仓/逼空冲高",
                "color": "#a78bfa",
                "text": f"持仓减少({delta:.0f} ETH) + 买盘为平仓买回，非真多头推进，谨防冲高回落",
                "can_short": True,   # 冲高回落开空机会
                "can_long": False,  # 不追多
            }
        else:
            return {
                "regime": "NEUTRAL",
                "label": "量仓均衡震荡",
                "color": "#8b93a7",
                "text": f"主动买卖比 {ratio:.2f}，持仓变化 {delta:+.0f} ETH ({delta_pct:+.2f}%)，多空处于博弈期",
                "can_short": True,
                "can_long": True,
            }

    def snapshot(self):
        sentiment = self.get_market_sentiment()
        return {
            "symbol": self.symbol,
            "price": self.price,
            "oi_current": round(self.oi_current, 2) if self.oi_current else None,
            "oi_delta_5m": self.oi_delta_5m,
            "oi_delta_1h": self.oi_delta_1h,
            "oi_delta_1d": self.oi_delta_1d,
            "oi_change_pct": self.oi_change_pct,
            "taker_buy_vol_5m": self.taker_buy_vol_5m,
            "taker_sell_vol_5m": self.taker_sell_vol_5m,
            "buy_sell_ratio_5m": self.buy_sell_ratio_5m,
            "top_long_pct": self.top_long_pct,
            "top_short_pct": self.top_short_pct,
            "top_ls_ratio": self.top_ls_ratio,
            "global_ls_ratio": self.global_ls_ratio,
            "funding_rate": self.funding_rate,
            "vol_ratio": self.vol_ratio,
            "realtime_liquidations": self.get_realtime_liquidation_stats(300),
            "btc_lead_lag": self.get_btc_lead_lag_stats(),
            "ws_connected": bool(self.ws_connected and (time.time() - self.last_ws_message_time < 5.0)),
            "sentiment": sentiment,
            "updated_at": datetime.now().strftime("%H:%M:%S"),
        }
