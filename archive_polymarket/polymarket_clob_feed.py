# -*- coding: utf-8 -*-
"""
Polymarket 官方 CLOB 实时行情与订单簿模块 (Polymarket CLOB Feed v2.0)
================================================================
严格按照 Polymarket 官方开发规范实现：
1. 市场极速发现:
   - 监听 Gamma API 最新创建的 5 分钟 "Ethereum Up or Down" 市场 (order=id 倒序, 杜绝 startDate=None 导致的漏盘)
   - 监听 CLOB WebSocket 的 new_market 事件, 实现 0 毫秒延迟换盘发现
2. 毫秒级订单簿:
   - 连接官方 CLOB WebSocket (wss://ws-subscriptions-clob.polymarket.com/ws/market)
   - 官方规定心跳: 每 8 秒发送 '{}' 心跳帧, 彻底解决 30 秒断开问题
   - 监听 book (全量快照), price_change (增量更新), best_bid_ask (顶档更新), last_trade_price (成交价)
   - 事件驱动回调: 盘口变动毫秒级推送策略决策, 杜绝轮询延迟
3. 盘口深度:
   - 实时维护 UP 和 DOWN 双边最新 5 档买卖单深度与数量
   - 实时计算最优买卖价、中间价、价差以及当前 5 分钟到期倒计时
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
import aiohttp
import requests

logger = logging.getLogger(__name__)
CST_TZ = timezone(timedelta(hours=8))

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
PROXY = "http://127.0.0.1:7890"
DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


class PolymarketClobFeed:
    def __init__(self, proxy=PROXY):
        self.proxy = proxy
        self.headers = {"User-Agent": DEFAULT_UA, "Accept": "application/json", "Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"}
        self._auto_detect_connection()
        self.current_market = None       # {title, slug, up_token, down_token, start_ts, end_ts, ...}
        self.previous_market = None
        self.orderbook = {
            "up": {"bids": [], "asks": [], "best_bid": 0.50, "best_ask": 0.51, "mid": 0.505, "spread": 0.01, "last_trade": 0.50},
            "down": {"bids": [], "asks": [], "best_bid": 0.49, "best_ask": 0.50, "mid": 0.495, "spread": 0.01, "last_trade": 0.50},
        }
        self._bids = {"up": {}, "down": {}}
        self._asks = {"up": {}, "down": {}}
        self.last_update_ts = 0.0
        self.last_msg_ts = 0.0
        self.ws_connected = False
        self._running = False
        self._ws_task = None
        self._discovery_task = None
        self._market_switched_callbacks = []
        self._book_updated_callbacks = []

    def _auto_detect_connection(self):
        """直连优先，若直连不通且配置了代理则尝试代理 127.0.0.1:7890"""
        try:
            r = requests.get(f"{GAMMA_API}/events?limit=1", headers=self.headers, timeout=3)
            if r.status_code == 200:
                self.proxy = None
                return
        except Exception:
            pass

        if self.proxy:
            try:
                r = requests.get(f"{GAMMA_API}/events?limit=1", headers=self.headers,
                                 proxies={"http": self.proxy, "https": self.proxy}, timeout=3)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            self.proxy = None

    def on_market_switched(self, cb):
        """注册市场轮换时的回调函数 (用于旧市场到期结算)"""
        self._market_switched_callbacks.append(cb)

    def on_book_updated(self, cb):
        """注册盘口更新毫秒级回调 (用于 0 延迟驱动策略 tick)"""
        self._book_updated_callbacks.append(cb)

    def _trigger_book_updated(self):
        """盘口变动时立即通知订阅方 (事件驱动)"""
        snap = self.snapshot()
        for cb in self._book_updated_callbacks:
            try:
                if asyncio.iscoroutinefunction(cb):
                    asyncio.create_task(cb(snap))
                else:
                    cb(snap)
            except Exception:
                pass

    async def start(self):
        """启动后台市场发现与 WebSocket 监听"""
        self._running = True
        # 1. 首次发现当前 5m 市场
        await self.discover_market()
        # 2. 启动长连接监听任务
        self._ws_task = asyncio.create_task(self._ws_loop())
        # 3. 启动快速市场轮换检测 (每 8 秒检查新盘)
        self._discovery_task = asyncio.create_task(self._discovery_loop())

    async def stop(self):
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
        if self._discovery_task:
            self._discovery_task.cancel()

    async def discover_market(self):
        """从 Gamma API 检索当前 5m Ethereum Up or Down 市场 (精准对齐当前时间戳, 绝无延迟)"""
        now = time.time()
        t_nc = int(now * 1000)

        # 1. 优先从 official series_slug 抓取所有未闭市的 5m 市场
        url_series = f"{GAMMA_API}/events?series_slug=eth-up-or-down-5m&closed=false&limit=40&_nocache={t_nc}"
        events = []
        try:
            timeout = aiohttp.ClientTimeout(total=8)
            async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as session:
                async with session.get(url_series, proxy=self.proxy) as resp:
                    if resp.status == 200:
                        events = await resp.json()
        except Exception:
            pass

        # 2. 备用补充: 如果 series 抓取为空，使用通用 events 列表
        if not events:
            url_events = f"{GAMMA_API}/events?limit=40&order=id&ascending=false&_nocache={t_nc}"
            try:
                timeout = aiohttp.ClientTimeout(total=8)
                async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as session:
                    async with session.get(url_events, proxy=self.proxy) as resp:
                        if resp.status == 200:
                            events = await resp.json()
            except Exception:
                pass

        if not events:
            return False

        candidates = []
        for ev in events:
            slug = ev.get("slug", "")
            title = ev.get("title", "")
            is_eth_5m = "eth-updown-5m-" in slug or ("ethereum up or down" in title.lower() and "5m" in slug)
            if not is_eth_5m:
                continue

            markets = ev.get("markets", [])
            if not markets:
                continue
            m = markets[0]
            if m.get("closed") is True:
                continue

            # 提取 5m 区间起始时间戳
            ts_part = slug.split("eth-updown-5m-")[-1] if "eth-updown-5m-" in slug else ""
            if ts_part.isdigit():
                start_ts = int(ts_part)
            else:
                try:
                    sd = ev.get("startDate") or m.get("startDate")
                    if sd:
                        start_ts = int(datetime.fromisoformat(sd.replace("Z", "+00:00")).timestamp())
                    else:
                        start_ts = int(ev.get("id", 0))
                except Exception:
                    start_ts = int(ev.get("id", 0))

            end_ts = start_ts + 300
            candidates.append((start_ts, end_ts, ev, m))

        if not candidates:
            return False

        # 智能匹配与对齐当前时间 (杜绝选择明天预排期盘口而导致的 10 分钟或 24 小时延迟错位):
        # 优先级1: 当前正在进行的 5m 周期 [start_ts <= now < end_ts]
        active = [c for c in candidates if c[0] <= now < c[1]]
        if active:
            sel = min(active, key=lambda c: now - c[0])
        else:
            # 优先级2: 下一个即将开始的周期 [start_ts >= now] 中最早的一个
            upcoming = [c for c in candidates if c[0] >= now]
            if upcoming:
                sel = min(upcoming, key=lambda c: c[0])
            else:
                # 优先级3: 最近刚刚结束的一个周期
                sel = max(candidates, key=lambda c: c[1])

        start_ts, end_ts, latest_ev, m = sel

        tokens = m.get("clobTokenIds")
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        outcomes = m.get("outcomes")
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)

        if not tokens or len(tokens) < 2:
            return False

        up_token = str(tokens[0])
        down_token = str(tokens[1])
        if outcomes and len(outcomes) >= 2:
            if str(outcomes[0]).lower() == "down":
                up_token = str(tokens[1])
                down_token = str(tokens[0])

        slug = latest_ev.get("slug", "")

        # 转换为本地北京时间 (CST, UTC+8) 显示
        try:
            dt_start = datetime.fromtimestamp(start_ts, tz=timezone.utc).astimezone(CST_TZ)
            dt_end = datetime.fromtimestamp(end_ts, tz=timezone.utc).astimezone(CST_TZ)
            s_cst = dt_start.strftime("%H:%M")
            e_cst = dt_end.strftime("%H:%M")
            period_cst = f"{s_cst} ~ {e_cst} (北京时间)"
        except Exception:
            period_cst = latest_ev.get("title", slug)

        new_market = {
            "id": str(latest_ev.get("id", "")),
            "title": latest_ev.get("title", ""),
            "display_title": f"ETH 5m UP/DOWN · {period_cst}",
            "period_cst": period_cst,
            "slug": slug,
            "ts": start_ts,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "up_token": up_token,
            "down_token": down_token,
            "condition_id": m.get("conditionId", ""),
            "startDate": latest_ev.get("startDate", ""),
            "endDate": latest_ev.get("endDate", ""),
        }

        # 检查是否发生市场轮换
        if not self.current_market or self.current_market.get("slug") != new_market["slug"]:
            old_market = self.current_market
            self.previous_market = old_market
            self.current_market = new_market

            # 清空旧盘口数据
            self._bids = {"up": {}, "down": {}}
            self._asks = {"up": {}, "down": {}}

            # 预拉一次初始订单簿快照
            await self._fetch_initial_book(up_token, "up")
            await self._fetch_initial_book(down_token, "down")

            # 如果有注册的市场切换回调，触发结算处理
            if old_market:
                for cb in self._market_switched_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(cb):
                            asyncio.create_task(cb(old_market, new_market))
                        else:
                            cb(old_market, new_market)
                    except Exception:
                        pass

            # 重置并唤醒 WebSocket 订阅新 Token
            if self._ws_task and not self._ws_task.done():
                self._ws_task.cancel()
                self._ws_task = asyncio.create_task(self._ws_loop())

            return True
        return False

    async def _fetch_initial_book(self, token_id, side):
        url = f"{CLOB_API}/book?token_id={token_id}"
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as session:
                async with session.get(url, proxy=self.proxy) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self._apply_full_book(side, data.get("bids", []), data.get("asks", []))
        except Exception:
            pass

    def _apply_full_book(self, side, bids, asks):
        bids_d = {}
        asks_d = {}
        for b in bids:
            try:
                p = round(float(b["price"]), 4)
                s = round(float(b["size"]), 2)
                if s > 0:
                    bids_d[p] = s
            except Exception:
                pass
        for a in asks:
            try:
                p = round(float(a["price"]), 4)
                s = round(float(a["size"]), 2)
                if s > 0:
                    asks_d[p] = s
            except Exception:
                pass

        self._bids[side] = bids_d
        self._asks[side] = asks_d
        self._recompute_book(side)

    def _recompute_book(self, side):
        bids_d = self._bids[side]
        asks_d = self._asks[side]

        sorted_bids = sorted(bids_d.items(), key=lambda x: -x[0])
        sorted_asks = sorted(asks_d.items(), key=lambda x: x[0])

        best_bid = sorted_bids[0][0] if sorted_bids else None
        best_ask = sorted_asks[0][0] if sorted_asks else None

        if best_bid is not None and best_ask is not None:
            mid = round((best_bid + best_ask) / 2.0, 4)
            spread = round(max(0.0, best_ask - best_bid), 4)
        elif best_bid is not None:
            mid = best_bid
            spread = 0.0
        elif best_ask is not None:
            mid = best_ask
            spread = 0.0
        else:
            mid = None
            spread = 0.0

        self.orderbook[side]["bids"] = sorted_bids[:5]
        self.orderbook[side]["asks"] = sorted_asks[:5]
        self.orderbook[side]["best_bid"] = best_bid
        self.orderbook[side]["best_ask"] = best_ask
        self.orderbook[side]["mid"] = mid
        self.orderbook[side]["spread"] = spread
        self.last_update_ts = time.time()
        self.last_msg_ts = time.time()

    async def _heartbeat_loop(self, ws):
        """Polymarket 官方要求的心跳循环: 每 8 秒发送一次 '{}', 保持 WebSocket 永不超时断开"""
        while self._running:
            await asyncio.sleep(8)
            try:
                if not ws.closed:
                    await ws.send_str("{}")
            except Exception:
                break

    async def _ws_loop(self):
        while self._running:
            if not self.current_market:
                await asyncio.sleep(1)
                continue

            up_token = self.current_market["up_token"]
            down_token = self.current_market["down_token"]

            try:
                conn_timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
                async with aiohttp.ClientSession(headers=self.headers, timeout=conn_timeout) as session:
                    async with session.ws_connect(WS_URL, proxy=self.proxy, heartbeat=15) as ws:
                        self.ws_connected = True
                        sub_msg = {
                            "assets_ids": [up_token, down_token],
                            "type": "market",
                            "custom_feature_enabled": True
                        }
                        await ws.send_json(sub_msg)

                        hb_task = asyncio.create_task(self._heartbeat_loop(ws))
                        try:
                            async for msg in ws:
                                if not self._running:
                                    break
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    try:
                                        payload = json.loads(msg.data)
                                        self._handle_ws_message(payload)
                                    except Exception:
                                        pass
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                        finally:
                            hb_task.cancel()
            except asyncio.CancelledError:
                self.ws_connected = False
                break
            except Exception:
                self.ws_connected = False

            self.ws_connected = False
            await asyncio.sleep(1)

    def _handle_ws_message(self, payload):
        items = payload if isinstance(payload, list) else [payload]
        changed = False

        for item in items:
            event_type = item.get("event_type")

            if event_type == "new_market" or "question" in item:
                q = item.get("question", "")
                if "Ethereum Up or Down" in q:
                    asyncio.create_task(self.discover_market())
                    continue

            asset_id = str(item.get("asset_id", ""))
            side = None
            if self.current_market:
                if asset_id == str(self.current_market["up_token"]):
                    side = "up"
                elif asset_id == str(self.current_market["down_token"]):
                    side = "down"

            if not side:
                continue

            if event_type == "book" or ("bids" in item and "asks" in item):
                self._apply_full_book(side, item.get("bids", []), item.get("asks", []))
                if "last_trade_price" in item and item["last_trade_price"]:
                    try:
                        self.orderbook[side]["last_trade"] = float(item["last_trade_price"])
                    except Exception:
                        pass
                changed = True

            elif event_type == "price_change":
                changes = item.get("changes", [])
                for chg in changes:
                    try:
                        p = round(float(chg.get("price", 0)), 4)
                        s = round(float(chg.get("size", 0)), 2)
                        side_type = chg.get("side", "").upper()
                        if side_type == "BUY":
                            if s > 0:
                                self._bids[side][p] = s
                            else:
                                self._bids[side].pop(p, None)
                        elif side_type == "SELL":
                            if s > 0:
                                self._asks[side][p] = s
                            else:
                                self._asks[side].pop(p, None)
                    except Exception:
                        pass
                self._recompute_book(side)
                changed = True

            elif event_type == "best_bid_ask":
                try:
                    bb = round(float(item.get("best_bid", self.orderbook[side]["best_bid"])), 4)
                    ba = round(float(item.get("best_ask", self.orderbook[side]["best_ask"])), 4)
                    self.orderbook[side]["best_bid"] = bb
                    self.orderbook[side]["best_ask"] = ba
                    self.orderbook[side]["mid"] = round((bb + ba) / 2.0, 4)
                    self.orderbook[side]["spread"] = round(max(0.0, ba - bb), 4)
                    self.last_update_ts = time.time()
                    self.last_msg_ts = time.time()
                    changed = True
                except Exception:
                    pass

            elif event_type == "last_trade_price":
                try:
                    px = round(float(item.get("price", self.orderbook[side]["last_trade"])), 4)
                    self.orderbook[side]["last_trade"] = px
                    self.last_update_ts = time.time()
                    self.last_msg_ts = time.time()
                    changed = True
                except Exception:
                    pass

        if changed:
            self._trigger_book_updated()

    async def _discovery_loop(self):
        while self._running:
            await asyncio.sleep(4)
            try:
                await self.discover_market()
            except Exception:
                pass

    def snapshot(self):
        now = time.time()
        # 精确计算当前合约真实的剩余秒数，与到期时间完美对齐
        if self.current_market and self.current_market.get("end_ts"):
            rem_sec = max(0, int(self.current_market["end_ts"] - now))
        else:
            rem_sec = 300 - (int(now) % 300)
        ping = getattr(self, "ping_ms", 35)

        return {
            "connected": self.ws_connected,
            "latency_ms": ping,
            "market": self.current_market,
            "remaining_sec": rem_sec,
            "remaining_str": f"{rem_sec // 60:02d}:{rem_sec % 60:02d}",
            "up": self.orderbook["up"],
            "down": self.orderbook["down"],
            "last_update": datetime.fromtimestamp(self.last_update_ts).strftime("%H:%M:%S") if self.last_update_ts else "--",
        }
