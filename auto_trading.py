# -*- coding: utf-8 -*-
"""
自动模拟交易程序 —— 币安 ETHUSDT 5 分钟实时价格 · 双策略纸面交易 · Web 实时监控 (v2.3 VWAP日基准阶梯版)
=================================================================================================
不接交易所下单, 纯本地模拟盘(纸面交易):

  行情价格与量仓: Binance Futures 官方合约公共 API (无需 Key, 支持直连与本地代理)
            - 价格 ticker + 5m K线 (OHLCV + Taker 主动买入量)
            - 实时总持仓量 (Open Interest - OI)
            - 5分钟持仓变化量 (5m OI Delta)
            - 5分钟主动买卖比率 (Taker Buy/Sell Ratio)
            - 大户持仓量多空比 (Top Trader Long/Short Position Ratio)
            - 实时日内 Daily VWAP (每天北京时间 00:00 归零自算，日基准，含 ±1σ/±2σ/±3σ 标准差轨道)

  CoinGlass 数据(每 --liq-interval 秒自动重抓, 默认 30 分钟):
    - ETH 三所聚合清算地图: data/auto/liq_latest.json
    - CoinGlass VWAP 备份/参考: data/legend/vwap_legend_values.json

  策略一 VWAP 日基准阶梯均值回归:
        1. 每日 00:00 至 04:00 (北京时间) 不开新仓 (已有持仓正常止损止盈监控)
        2. 价格触及 #1#2 中心值: 上方中心值开空 / 下方中心值开多
           - 止损: 逆势触及 #2 轨 (#2上轨止损平空 / #2下轨止损平多)
           - 止盈: 价格回归 VWAP 主线时达成均值止盈
        3. 价格到达 #3 轨: 上方 #3 轨开空 / 下方 #3 轨开多 (极值反转)
           - 平仓: 价格回踩/回弹至 #2 轨 (#2上轨平空 / #2下轨平多)
        4. 量仓门禁: 主力放量增仓单边强攻时暂缓逆势摸顶/接飞刀; 1.5% 兜底防极端单边

  策略二 ETH 清算地图 (量仓出清确认 + TP全平 + 1.5%硬止损/清算带击穿止损 + 6h超时保护):
        跌破多头清算区开多 (需量仓确认出清/止跌, 防主力砸盘破位)
        涨破空头清算区开空 (需量仓确认逼空衰竭, 防主力增仓逼空)
        价格到达 TP 清算中心全平; 浮盈 >= 0.6% 且反向异动时提前保盈;
        若逆势亏损达 1.5% 或击穿最外层清算区，立即执行硬止损平仓；超过 6 小时未回归执行超时平仓。

  Web 监控: http://127.0.0.1:8787
"""
import argparse
import asyncio
import json
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE = Path(__file__).resolve().parent
STATE_FILE = BASE / "data" / "auto" / "state.json"
DASHBOARD = BASE / "web" / "dashboard.html"
LIQ_DATA = BASE / "test_data" / "exliqmap_eth_raw.json"
LIQ_LATEST = BASE / "data" / "auto" / "liq_latest.json"
LEGEND_FILE = BASE / "data" / "legend" / "vwap_legend_values.json"
FETCHER = BASE / "coinglass_fetch.py"

from binance_futures_feed import BinanceFuturesFeed
from eth_predictor.service import PredictorService

MAX_EVENTS = 300
MAX_KLINES_CHART = 240  # 前端 K 线根数
EQUITY_SAMPLE_SEC = 15  # 权益曲线采样间隔
ORDER_QTY = 1.0         # 每次开仓/加仓固定数量(ETH)


# ----------------------------------------------------------------------------
# VWAP 图例辅助读取 (CoinGlass 备份)
# ----------------------------------------------------------------------------
def load_legend():
    """读取 CoinGlass VWAP 图例文件(备用)"""
    try:
        d = json.loads(LEGEND_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not d.get("ok") or "vwap" not in d:
        return None
    age = None
    try:
        t = datetime.fromisoformat(d["updated_at"]).timestamp()
        age = int(time.time() - t)
    except Exception:
        pass
    
    u1, l1 = d.get("band1_upper"), d.get("band1_lower")
    u2, l2 = d.get("band2_upper"), d.get("band2_lower")
    u3, l3 = d.get("band3_upper"), d.get("band3_lower")
    if u1 is None or u2 is None or l1 is None or l2 is None:
        return None

    mid_upper = round((u1 + u2) / 2.0, 2)
    mid_lower = round((l1 + l2) / 2.0, 2)

    return {
        "vwap": d["vwap"],
        "u3": u3, "l3": l3,
        "u2": u2, "l2": l2,
        "u1": u1, "l1": l1,
        "mid_upper": mid_upper,
        "mid_lower": mid_lower,
        "updated_at": d.get("updated_at"),
        "age": age,
        "source": "CoinGlass 图例",
        "stale": False,
    }


# ----------------------------------------------------------------------------
# 纸面账户 + 策略基类
# ----------------------------------------------------------------------------
class Strategy:
    def __init__(self, sid, title, desc, equity, fee, leverage, engine):
        self.id = sid
        self.title = title
        self.desc = desc
        self.equity = equity
        self.init_equity = equity
        self.fee = fee
        self.leverage = leverage
        self.engine = engine
        self.position = None   # {"side":±1, "qty":n, "notional":n, "entries":[p], "fees":f, "meta":{}, "entry_time":str, "entry_ts":float}
        self.trades = []
        self.fees_paid = 0.0

    def _fill(self, side, price, qty, reason):
        """按固定数量下单(qty 单位: ETH)"""
        notional = qty * price
        fee = notional * self.fee
        self.equity -= fee
        self.fees_paid += fee
        now_ts = time.time()
        now_iso = datetime.now().isoformat(timespec="seconds")
        if self.position is None:
            self.position = {
                "side": side, "qty": qty, "notional": notional,
                "entries": [price], "fees": fee, "meta": {},
                "entry_time": now_iso, "entry_ts": now_ts
            }
        else:
            self.position["qty"] += qty
            self.position["notional"] += notional
            self.position["entries"].append(price)
            self.position["fees"] += fee
        avg = self.position["notional"] / self.position["qty"]
        verb = ("开多" if side > 0 else "开空") if len(self.position["entries"]) == 1 else "加仓"
        self.engine.emit(self.id, verb,
                         "%s %.4f ETH @ %.2f | 名义 %.2f | 均价 %.2f | %s"
                         % (verb, qty, price, notional, avg, reason))
        return verb

    def _close(self, price, reason):
        pos = self.position
        if not pos:
            return
        gross = (price - pos["notional"] / pos["qty"]) * pos["qty"] * pos["side"]
        exit_fee = pos["qty"] * price * self.fee
        self.equity -= exit_fee
        self.fees_paid += exit_fee
        pnl = gross - pos["fees"] - exit_fee
        self.equity += gross
        side_txt = "多" if pos["side"] > 0 else "空"
        self.trades.append({
            "time": datetime.now().isoformat(timespec="seconds"),
            "ts": int(time.time()),
            "side": side_txt, "qty": round(pos["qty"], 6),
            "entry": round(pos["notional"] / pos["qty"], 2),
            "exit": round(price, 2),
            "pnl": round(pnl, 4), "fees": round(pos["fees"] + exit_fee, 4),
            "equity": round(self.equity, 2), "reason": reason,
        })
        self.engine.emit(self.id, "平仓",
                         "平%s %.4f ETH @ %.2f | 盈亏 %+.2f | %s | 权益 %.2f"
                         % (side_txt, pos["qty"], price, pnl, reason, self.equity))
        self.position = None

    def snapshot(self, price):
        pos = None
        if self.position:
            p = self.position
            avg = p["notional"] / p["qty"]
            pos = {
                "side": "多" if p["side"] > 0 else "空",
                "qty": round(p["qty"], 6),
                "avg_entry": round(avg, 2),
                "notional": round(p["notional"], 2),
                "upnl": round((price - avg) * p["qty"] * p["side"], 2) if price else 0.0,
                "entry_time": p.get("entry_time", "--"),
                "meta": p.get("meta", {}),
            }
        wins = sum(1 for t in self.trades if t["pnl"] > 0)
        return {
            "id": self.id, "title": self.title, "desc": self.desc,
            "equity": round(self.equity, 2),
            "init_equity": self.init_equity,
            "pnl": round(self.equity - self.init_equity, 2),
            "pnl_pct": round(100.0 * (self.equity / self.init_equity - 1.0), 3),
            "upnl": pos["upnl"] if pos else 0.0,
            "position": pos,
            "n_trades": len(self.trades), "wins": wins,
            "win_rate": round(100.0 * wins / len(self.trades), 1) if self.trades else 0.0,
            "fees_paid": round(self.fees_paid, 2),
            "trades": self.trades[-50:],
        }


# ----------------------------------------------------------------------------
# 策略一: VWAP #1#2 中心值均值回归 (带量仓过滤 + 1.2%硬止损 + 6h超时保护)
# ----------------------------------------------------------------------------
class VwapStrategy(Strategy):
    """
    策略一: VWAP 日基准阶梯均值回归策略 (v2.3 规范版)
    ========================================================================
    核心规则:
    1. 基准设定:
       - 采用币安合约 5m 官方数据实时自算的日内 Daily VWAP (每天 00:00 归零自算，日基准)。
    2. 时间风控:
       - 每日 00:00 至 04:00 (北京时间) 禁止开新仓；
       - 已有持仓在禁开期内正常执行止盈与止损监控。
    3. 交易机制 1 (#1#2 中心值交易):
       - 开仓:
         * 触及 + 侧中心值 (mid_upper = (u1+u2)/2) 且 price < u2: 开空 1 单位
         * 触及 - 侧中心值 (mid_lower = (l1+l2)/2) 且 price > l2: 开多 1 单位
       - 止损: 价格逆势触及 #2 轨 (做空达到 u2 / 做多达到 l2) 时立即止损平仓
       - 止盈: 价格回归 VWAP 主线且有浮盈时均值回归止盈平仓
    4. 交易机制 2 (#3 轨极值反转交易):
       - 开仓:
         * 触及 +3σ 轨 (price >= u3): 开空 1 单位
         * 触及 -3σ 轨 (price <= l3): 开多 1 单位
       - 平仓: 价格回踩/回弹至 #2 轨 (做空回踩 u2 / 做多回弹 l2) 时平仓止盈
    5. 量仓微观门禁与兜底风控:
       - 主力量仓进攻拦截: 多头单边增仓爆买 (BULL_ATTACK) 禁摸顶开空，空头单边增仓狂砸 (BEAR_ATTACK) 禁接飞刀开多；
       - 兜底硬止损: 逆势极端亏损达到 1.5% 兜底离场防极端黑天鹅；
       - 超时保护: 持仓超 6 小时保护平仓。
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.last_stop_time = 0.0
        self.last_stop_side = 0
        self._last_time_ban_warn = 0.0
        self._last_guard_warn_short = 0.0
        self._last_guard_warn_long = 0.0

    def on_tick(self, price, bands, vol_oi=None):
        if not bands or not price:
            return
        vwap = bands.get("vwap")
        u1 = bands.get("u1")
        l1 = bands.get("l1")
        u2 = bands.get("u2")
        l2 = bands.get("l2")
        u3 = bands.get("u3")
        l3 = bands.get("l3")
        mid_upper = bands.get("mid_upper")  # + 侧中心值 (u1+u2)/2
        mid_lower = bands.get("mid_lower")  # - 侧中心值 (l1+l2)/2

        if any(v is None for v in (vwap, u2, l2, u3, l3, mid_upper, mid_lower)):
            return

        sentiment = vol_oi.get("sentiment", {}) if vol_oi else {}
        can_short = sentiment.get("can_short", True)
        can_long = sentiment.get("can_long", True)

        now = time.time()
        now_dt = datetime.now()
        is_banned_hours = (0 <= now_dt.hour < 4)  # 每日北京时间 00:00 至 04:00 不开新仓

        # 当价格重新穿越回 VWAP 对侧时，自动解除上次同向止损冷却
        if (self.last_stop_side == 1 and price >= vwap) or (self.last_stop_side == -1 and price <= vwap):
            self.last_stop_side = 0

        if self.position is None:
            # 1. 每日 00:00 - 04:00 禁开期拦截
            if is_banned_hours:
                if (price >= mid_upper or price <= mid_lower or price >= u3 or price <= l3):
                    if now - getattr(self, "_last_time_ban_warn", 0) > 300:
                        self._last_time_ban_warn = now
                        self.engine.emit(self.id, "时间风控",
                                         f"当前时间 {now_dt.strftime('%H:%M:%S')} 处于每日0时至4时风控期，禁止开新仓")
                return

            # 2. 优先检查 #3 轨极值开仓 (到#3时开仓)
            # A. 触及 +3σ 轨，开空
            if price >= u3:
                if not can_short:
                    if now - getattr(self, "_last_guard_warn_short", 0) > 60:
                        self._last_guard_warn_short = now
                        self.engine.emit(self.id, "门禁拦截",
                                         f"价格达到+3σ轨 {u3:.2f}，但{sentiment.get('text', '')}，暂停开空防摸顶")
                else:
                    reason = f"触及+3σ轨 {u3:.2f} 极值反转开空 | 目标回踩#2轨 {u2:.2f} 平仓 | 量仓: {sentiment.get('label', '均衡')}"
                    self._fill(-1, price, ORDER_QTY, reason)
                    if self.position:
                        self.position["meta"]["entry_type"] = "band3"
                        self.position["meta"]["init_vwap"] = vwap
                        self.position["meta"]["target_rail"] = u2
                return

            # B. 触及 -3σ 轨，开多
            elif price <= l3:
                if not can_long:
                    if now - getattr(self, "_last_guard_warn_long", 0) > 60:
                        self._last_guard_warn_long = now
                        self.engine.emit(self.id, "门禁拦截",
                                         f"价格达到-3σ轨 {l3:.2f}，但{sentiment.get('text', '')}，暂停开多防接飞刀")
                else:
                    reason = f"触及-3σ轨 {l3:.2f} 极值反转开多 | 目标回弹#2轨 {l2:.2f} 平仓 | 量仓: {sentiment.get('label', '均衡')}"
                    self._fill(+1, price, ORDER_QTY, reason)
                    if self.position:
                        self.position["meta"]["entry_type"] = "band3"
                        self.position["meta"]["init_vwap"] = vwap
                        self.position["meta"]["target_rail"] = l2
                return

            # 3. 检查 #1#2 中心值开仓 (价格在#1#2中心开多或空)
            # A. 触及 + 侧中心值 (mid_upper <= price < u2)
            elif mid_upper <= price < u2:
                # 冷却期保护: 若刚在 #2 止损做空，5分钟内不重复在中心值追空，防震荡磨损
                if self.last_stop_side == -1 and (now - self.last_stop_time < 300):
                    return

                if not can_short:
                    if now - getattr(self, "_last_guard_warn_short", 0) > 60:
                        self._last_guard_warn_short = now
                        self.engine.emit(self.id, "门禁拦截",
                                         f"价格达到+侧中心值 {mid_upper:.2f}，但{sentiment.get('text', '')}，暂停开空防摸顶")
                else:
                    reason = f"触及+侧中心值 {mid_upper:.2f} 开空 | #2轨 {u2:.2f} 止损 | 目标回归 VWAP {vwap:.2f} | 量仓: {sentiment.get('label', '均衡')}"
                    self._fill(-1, price, ORDER_QTY, reason)
                    if self.position:
                        self.position["meta"]["entry_type"] = "mid"
                        self.position["meta"]["init_vwap"] = vwap
                        self.position["meta"]["stop_rail"] = u2

            # B. 触及 - 侧中心值 (l2 < price <= mid_lower)
            elif l2 < price <= mid_lower:
                # 冷却期保护: 若刚在 #2 止损做多，5分钟内不重复在中心值抄底，防震荡磨损
                if self.last_stop_side == 1 and (now - self.last_stop_time < 300):
                    return

                if not can_long:
                    if now - getattr(self, "_last_guard_warn_long", 0) > 60:
                        self._last_guard_warn_long = now
                        self.engine.emit(self.id, "门禁拦截",
                                         f"价格达到-侧中心值 {mid_lower:.2f}，但{sentiment.get('text', '')}，暂停开多防接飞刀")
                else:
                    reason = f"触及-侧中心值 {mid_lower:.2f} 开多 | #2轨 {l2:.2f} 止损 | 目标回归 VWAP {vwap:.2f} | 量仓: {sentiment.get('label', '均衡')}"
                    self._fill(+1, price, ORDER_QTY, reason)
                    if self.position:
                        self.position["meta"]["entry_type"] = "mid"
                        self.position["meta"]["init_vwap"] = vwap
                        self.position["meta"]["stop_rail"] = l2

        else:
            # 4. 持仓状态出场管理
            side = self.position["side"]
            avg_entry = self.position["notional"] / self.position["qty"]
            profit_pct = (price - avg_entry) / avg_entry * side
            hold_sec = time.time() - self.position.get("entry_ts", time.time())
            meta = self.position.setdefault("meta", {})
            entry_type = meta.get("entry_type", "mid")

            # --- 分支 A: #1#2 中心值开仓出场机制 ---
            if entry_type == "mid":
                # 止损: #2 时止损 (做空破 u2 / 做多跌破 l2)
                hit_rail_stop = (side < 0 and price >= u2) or (side > 0 and price <= l2)
                if hit_rail_stop:
                    self.last_stop_time = time.time()
                    self.last_stop_side = side
                    rail_str = f"触及+2σ轨({u2:.2f})" if side < 0 else f"触及-2σ轨({l2:.2f})"
                    self._close(price, f"{rail_str} 达成止损平仓 (亏损 {profit_pct*100:+.2f}%)")
                    return

                # 止盈: 价格回归 VWAP
                is_reach_vwap = (side < 0 and price <= vwap) or (side > 0 and price >= vwap)
                if is_reach_vwap and profit_pct > 0.0005:
                    self.last_stop_side = 0
                    self._close(price, f"回归 VWAP {vwap:.2f} 达成均值止盈 (浮盈 {profit_pct*100:+.2f}%)")
                    return

            # --- 分支 B: #3 轨极值开仓出场机制 ---
            elif entry_type == "band3":
                # 平仓: #2 时平仓 (做空回踩 u2 / 做多回弹 l2)
                is_reach_band2 = (side < 0 and price <= u2) or (side > 0 and price >= l2)
                if is_reach_band2:
                    self.last_stop_side = 0
                    rail_str = f"回踩+2σ轨({u2:.2f})" if side < 0 else f"回弹-2σ轨({l2:.2f})"
                    self._close(price, f"{rail_str} 达成止盈平仓 (浮盈 {profit_pct*100:+.2f}%)")
                    return

            # --- 通用兜底风控 1: 1.5% 极端单边硬止损 ---
            if profit_pct <= -0.015:
                self.last_stop_time = time.time()
                self.last_stop_side = side
                self._close(price, f"触发极端行情 1.5% 兜底硬止损 (亏损 {profit_pct*100:+.2f}%)")
                return

            # --- 通用兜底风控 2: 6 小时持仓超时保护 ---
            if hold_sec >= 21600:
                self._close(price, f"持仓超时保护平仓 (已持仓 {hold_sec/3600:.1f}小时, 浮盈 {profit_pct*100:+.2f}%)")
                return


# ----------------------------------------------------------------------------
# 策略二: ETH 清算地图 (带量仓出清确认 + 1.5%硬止损/破位止损 + 6h超时保护)
# ----------------------------------------------------------------------------
class LiqMapStrategy(Strategy):
    """
    策略二:
    - 突破清算区1/2/3开仓或加仓 (需量仓确认出清/止跌, 遇单边真开仓推单则规避)
    - 基础平仓: 到达多空清算中心 TP 全平
    - 辅助平仓: 累积一定浮盈且出现反向主力极端推单异动时提前保盈平仓
    - 硬止损: 逆势亏损达到 1.5% 或 彻底击穿清算区(+15点) 强制止损，杜绝无限死扛
    - 超时保护: 超过 6 小时未能回归平仓
    """

    def __init__(self, liq, *a, **kw):
        super().__init__(*a, **kw)
        self.long_zones = liq.get("long_zones", []) if liq else []
        self.short_zones = liq.get("short_zones", []) if liq else []
        self.tp = liq.get("tp") if liq else None
        self.last_stop_time = 0.0
        self.last_stop_side = 0

    def update_zones(self, liq):
        if not liq:
            return
        self.long_zones = liq.get("long_zones", [])
        self.short_zones = liq.get("short_zones", [])
        self.tp = liq.get("tp")
        if self.position:
            filled = self.position.setdefault("meta", {}).setdefault("filled", [False] * 3)
            self.position["meta"]["filled"] = (filled + [False] * 3)[:3]

    def on_tick(self, price, vol_oi=None):
        if not price:
            return
        sentiment = vol_oi.get("sentiment", {}) if vol_oi else {}
        regime = sentiment.get("regime", "NEUTRAL")
        can_short = sentiment.get("can_short", True)
        can_long = sentiment.get("can_long", True)

        now = time.time()
        # 若价格回归到 TP 或对侧，解除止损冷却
        if self.tp is not None:
            if (self.last_stop_side == 1 and price >= self.tp) or (self.last_stop_side == -1 and price <= self.tp):
                self.last_stop_side = 0

        if self.position is None:
            # 首次开仓判定
            for side, zones, label in ((+1, self.long_zones, "多头清算区"),
                                       (-1, self.short_zones, "空头清算区")):
                if not zones:
                    continue

                # 止损冷却期拦截 (15分钟防重开)
                if self.last_stop_side == side and (now - self.last_stop_time < 900):
                    continue

                # 破位拦截: 若价格已经击穿最外层清算区超出 15 点，严禁开仓接飞刀
                if side > 0 and price <= min(zones) - 15.0:
                    continue
                if side < 0 and price >= max(zones) + 15.0:
                    continue

                crossed = price <= zones[0] if side > 0 else price >= zones[0]
                if crossed:
                    # 量仓门禁检查
                    if side > 0 and not can_long:
                        if now - getattr(self, "_last_guard_warn", 0) > 60:
                            self._last_guard_warn = now
                            self.engine.emit(self.id, "门禁拦截",
                                             f"跌破{label}1 {zones[0]:.1f}，但{sentiment.get('text', '')}，等待出清企稳")
                        break
                    elif side < 0 and not can_short:
                        if now - getattr(self, "_last_guard_warn", 0) > 60:
                            self._last_guard_warn = now
                            self.engine.emit(self.id, "门禁拦截",
                                             f"涨破{label}1 {zones[0]:.1f}，但{sentiment.get('text', '')}，等待逼空衰竭")
                        break

                    for i, lv in enumerate(zones):
                        hit = price <= lv if side > 0 else price >= lv
                        if hit:
                            self._fill(side, price, ORDER_QTY,
                                       f"突破{label}{i+1} {lv:.1f} | 量仓: {sentiment.get('label', '均衡')}")
                            if self.position:
                                self.position.setdefault("meta", {}).setdefault("filled", [False] * 3)[i] = True
                    break
            return

        side = self.position["side"]
        zones = self.long_zones if side > 0 else self.short_zones
        label = "多头清算区" if side > 0 else "空头清算区"
        filled = self.position.setdefault("meta", {}).setdefault("filled", [False] * 3)

        # 加仓逻辑
        for i, lv in enumerate(zones):
            if not filled[i]:
                hit = price <= lv if side > 0 else price >= lv
                if hit:
                    if (side > 0 and not can_long) or (side < 0 and not can_short):
                        continue
                    filled[i] = True
                    self._fill(side, price, ORDER_QTY,
                               f"突破{label}{i+1} {lv:.1f} 加仓 | 量仓: {sentiment.get('label', '均衡')}")

        avg_entry = self.position["notional"] / self.position["qty"]
        profit_pct = (price - avg_entry) / avg_entry * side
        hold_sec = time.time() - self.position.get("entry_ts", time.time())

        # 击穿最外层清算区结构性止损
        deep_break = False
        if self.short_zones and side < 0 and price >= max(self.short_zones) + 15.0:
            deep_break = True
        elif self.long_zones and side > 0 and price <= min(self.long_zones) - 15.0:
            deep_break = True

        # 平仓 1: 到达 TP 全平 (正常止盈)
        if self.tp is not None:
            if (price >= self.tp) if side > 0 else (price <= self.tp):
                self._close(price, f"到达多空清算中心 TP {self.tp:.2f} (浮盈 {profit_pct*100:+.2f}%)")
                self.last_stop_side = 0
                return

        # 平仓 2: 量仓动能耗尽或反向突发强力进攻提前止盈保本
        if profit_pct >= 0.006:
            if (side > 0 and regime == "BEAR_ATTACK") or (side < 0 and regime == "BULL_ATTACK"):
                self._close(price, f"量仓异动提前止盈 (浮盈 {profit_pct*100:.2f}%，反向主力进攻)")
                self.last_stop_side = 0
                return

        # 平仓 3: 硬止损 (逆势亏损达到 1.5% 或 彻底击穿最外层清算区)
        if profit_pct <= -0.015 or deep_break:
            self.last_stop_time = time.time()
            self.last_stop_side = side
            reason = "击穿最外层清算区破位止损" if deep_break else f"触发硬止损 (亏损 {profit_pct*100:.2f}%)"
            self._close(price, reason)
            return

        # 平仓 4: 超时保护 (超过 6 小时未回归 TP)
        if hold_sec >= 21600:
            self._close(price, f"持仓超时平仓 (已持仓 {hold_sec/3600:.1f}小时, 浮盈 {profit_pct*100:+.2f}%)")
            return


def load_liq_zones(path):
    """提取清算区"""
    try:
        d = json.loads(Path(path).read_text(encoding="utf-8"))
        last_price = float(d.get("lastPrice", 0.0))
        agg = {}
        for ex in d.get("data", []):
            for p, rows in (ex.get("liqMapV2") or {}).items():
                agg[float(p)] = agg.get(float(p), 0.0) + sum(r[1] for r in rows)
        long_lv = {p: a for p, a in agg.items() if p < last_price}
        short_lv = {p: a for p, a in agg.items() if p > last_price}

        def zones(levels, from_high):
            if not levels:
                return []
            top3 = sorted(levels.items(), key=lambda kv: -kv[1])[:3]
            prices = sorted((p for p, _ in top3), reverse=from_high)
            return [round(p, 1) for p in prices]

        L = zones(long_lv, True)
        S = zones(short_lv, False)
        tp = (max(L) + min(S)) / 2.0 if (L and S) else None
        return {"last_price": last_price, "long_zones": L, "short_zones": S, "tp": round(tp, 2) if tp else None}
    except Exception as e:
        return {"last_price": 0.0, "long_zones": [], "short_zones": [], "tp": None}


# ----------------------------------------------------------------------------
# 交易引擎: 币安合约 Feed + CoinGlass + 自动复盘 + Web API
# ----------------------------------------------------------------------------
class Engine:
    def __init__(self, args):
        self.args = args
        self.futures_feed = BinanceFuturesFeed(symbol="ETHUSDT", interval="5m")
        self.feed = self.futures_feed

        self.liq_path = LIQ_LATEST if LIQ_LATEST.exists() else Path(args.liq_data)
        self.liq = load_liq_zones(self.liq_path)
        self.predictor_service = PredictorService(self.futures_feed, liq_path=self.liq_path)
        self.started_at = time.time()
        self.events = []
        self.equity_history = []
        
        self.vwap_sys = VwapStrategy(
            "vwap", "策略一 · VWAP 日基准阶梯均值回归",
            "日基准VWAP | #1#2中心开多/空(#2止损/回归VWAP止盈) | #3反转开仓(#2平仓) | 每日0-4时不开仓",
            args.equity, args.fee, args.leverage, self)
        
        self.liq_sys = LiqMapStrategy(
            self.liq, "liqmap", "策略二 · ETH 清算地图 + 量仓出清",
            "跌破多头清算区开多 / 涨破空头清算区开空 / 到 TP 全平 (量仓出清确认 + 1.5%硬止损)",
            args.equity, args.fee, args.leverage, self)

        # 仅保留策略一和策略二
        self.strategies = [self.vwap_sys, self.liq_sys]
        self.last_sample = 0.0
        self.last_save = 0.0
        self.error = None
        self._liq_path_str = str(self.liq_path)
        self._liq_mtime = None
        self._last_cycle = 0.0
        self._fetching = False
        self._stale_warned = False
        try:
            self._liq_mtime = self.liq_path.stat().st_mtime
        except OSError:
            pass

    def _reload_liq(self, force=False):
        if LIQ_LATEST.exists() and str(self.liq_path) != str(LIQ_LATEST):
            self.liq_path = LIQ_LATEST
            force = True
        try:
            m = self.liq_path.stat().st_mtime
        except OSError:
            return
        if not force and m == self._liq_mtime:
            return
        try:
            new_liq = load_liq_zones(self.liq_path)
            if not new_liq.get("long_zones") or not new_liq.get("short_zones"):
                return
            old = self.liq
            self.liq = new_liq
            self.liq_sys.update_zones(new_liq)
            self._liq_mtime = m
            self._liq_path_str = str(self.liq_path)
            if (old.get("long_zones") != new_liq.get("long_zones")
                    or old.get("short_zones") != new_liq.get("short_zones")
                    or old.get("tp") != new_liq.get("tp") or force):
                self.emit("sys", "清算地图重载",
                          "多%s 空%s TP %s (lastPrice %.2f)"
                          % (new_liq["long_zones"], new_liq["short_zones"],
                             f"{new_liq['tp']:.2f}" if new_liq['tp'] is not None else "--",
                             new_liq["last_price"]))
        except Exception as e:
            self.emit("sys", "警告", "清算地图重载失败: %s" % e)

    def _cycle_5min(self):
        self._reload_liq()

    async def _do_fetch(self):
        if self._fetching:
            return
        self._fetching = True
        self.emit("sys", "抓取", "开始抓取 CoinGlass (清算地图 + VWAP 图例)...")
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, str(FETCHER),
                cwd=str(BASE), stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT)
            try:
                out, _ = await asyncio.wait_for(proc.communicate(), timeout=600)
            except asyncio.TimeoutError:
                proc.kill()
                self.emit("sys", "抓取", "抓取超时(10 分钟), 已终止, 下轮重试")
                return
            lines = (out or b"").decode("utf-8", "replace").strip().splitlines()
            summary = None
            for ln in reversed(lines):
                if ln.startswith("FETCH_RESULT"):
                    try:
                        summary = json.loads(ln[len("FETCH_RESULT"):])
                    except Exception:
                        pass
                    break
            if summary is None:
                tail = " / ".join(lines[-3:]) if lines else "无输出"
                self.emit("sys", "抓取", "结果解析失败 (exit=%s): %s"
                          % (proc.returncode, tail))
                return
            liq = summary.get("liq") or {}
            self.emit("sys", "抓取",
                       "清算地图 %s (%s)"
                       % ("OK" if liq.get("ok") else "失败", liq.get("detail")))
            self._reload_liq()
        except Exception as e:
            self.emit("sys", "抓取", "抓取异常: %s" % e)
        finally:
            self._fetching = False

    async def fetch_loop(self):
        await asyncio.sleep(20)
        while True:
            await self._do_fetch()
            await asyncio.sleep(self.args.liq_interval)

    def emit(self, system, action, text):
        self.events.append({
            "time": datetime.now().isoformat(timespec="seconds"),
            "ts": int(time.time()), "system": system,
            "action": action, "text": text})
        if len(self.events) > MAX_EVENTS:
            self.events = self.events[-MAX_EVENTS:]
        print("[%s][%s] %s" % (datetime.now().strftime("%H:%M:%S"), system, text),
              flush=True)

    def get_vwap_bands(self):
        """优先使用币安周线实时自算 Weekly VWAP (北京周一 00:00 归零); 缺失时回退到日内或 CoinGlass"""
        weekly = getattr(self.futures_feed, "calc_weekly_vwap", lambda: None)()
        if weekly and weekly.get("vwap"):
            return weekly
        daily = self.futures_feed.calc_daily_vwap()
        if daily and daily.get("vwap"):
            return daily
        return load_legend()

    async def run(self):
        self.emit("sys", "启动", f"币安合约行情连接: {self.futures_feed.host} (代理={self.futures_feed.use_proxy})")
        if self.args.resume and STATE_FILE.exists():
            self._load_state()

        # 启动币安官方 WebSocket 实时数据流 (毫秒级价格与 5m K线)
        self.futures_feed.start_websocket()

        tasks = []
        if not self.args.no_fetch:
            tasks.append(asyncio.create_task(self.fetch_loop()))

        # 首次初始化量仓数据
        try:
            await asyncio.to_thread(self.futures_feed.poll_volume_and_oi, True)
        except Exception:
            pass

        while True:
            try:
                # 1. 轮询价格与 K 线 (5m / 1h / 1d)
                await asyncio.to_thread(self.futures_feed.poll_price_and_klines)
                await asyncio.to_thread(self.futures_feed.poll_higher_timeframe_klines)
                price = self.futures_feed.price
                if not price:
                    await asyncio.sleep(self.args.poll)
                    continue

                # 2. 轮询高阶量仓指标 (持仓量、OI Delta、主动买卖量、大户多空比)
                await asyncio.to_thread(self.futures_feed.poll_volume_and_oi)
                vol_oi = self.futures_feed.snapshot()

                # 3. 读取/计算实时 VWAP (日内 Daily VWAP 优先)
                vwap_bands = self.get_vwap_bands()
                if vwap_bands and vwap_bands.get("vwap"):
                    self.vwap_sys.on_tick(price, vwap_bands, vol_oi)
                    self._stale_warned = False
                else:
                    if not self._stale_warned:
                        self._stale_warned = True
                        self.emit("sys", "警告", "VWAP 数据缺失, 策略一暂停等待计算")

                # 4. 运行清算地图策略
                self.liq_sys.on_tick(price, vol_oi)
                self.error = None

                # 5. 更新 ETH 多周期预测引擎 (5m / 1h / 1d 预测与 12h 自主自优化复盘)
                try:
                    self.predictor_service.update_tick()
                except Exception as ep:
                    print(f"[!] 预测服务更新异常: {ep}", flush=True)

                now = time.time()
                # 采样权益曲线 (仅保留策略一与策略二两组数据)
                if now - self.last_sample >= EQUITY_SAMPLE_SEC:
                    self.last_sample = now
                    self.equity_history.append([
                        int(now),
                        round(self.vwap_sys.equity, 2),
                        round(self.liq_sys.equity, 2)
                    ])
                    self.equity_history = self.equity_history[-4000:]

                # 周期保存状态
                if now - self.last_save >= 60:
                    self.last_save = now
                    self._save_state()

                # 5分钟检测
                if now - self._last_cycle >= 300:
                    self._last_cycle = now
                    self._cycle_5min()

            except Exception as e:
                self.error = str(e)
                if (str(e) != getattr(self, "_last_err", None)
                        or time.time() - getattr(self, "_last_err_ts", 0) > 60):
                    self._last_err = str(e)
                    self._last_err_ts = time.time()
                    self.emit("sys", "错误", "行情/策略异常(将自动重试): %s" % e)

            await asyncio.sleep(self.args.poll)

        for t in tasks:
            t.cancel()

    def state(self):
        vwap_view = self.get_vwap_bands()
        weekly_vwap = getattr(self.futures_feed, "calc_weekly_vwap", lambda: None)()
        daily_vwap = getattr(self.futures_feed, "calc_daily_vwap", lambda: None)()
        price = self.futures_feed.price
        ks = []
        for k in self.futures_feed.klines[-MAX_KLINES_CHART:]:
            ks.append([int(k[0] // 1000), float(k[1]), float(k[2]),
                       float(k[3]), float(k[4]), float(k[5])])

        vol_oi_snap = self.futures_feed.snapshot()
        pred_snap = {}
        try:
            pred_snap = self.predictor_service.snapshot()
        except Exception as ep:
            pred_snap = {"error": str(ep)}

        return {
            "now": datetime.now().isoformat(timespec="seconds"),
            "symbol": self.futures_feed.symbol,
            "host": self.futures_feed.host,
            "price": price,
            "feed_ok": bool(self.futures_feed.last_ok_time and time.time() - self.futures_feed.last_ok_time < 30),
            "ws_connected": bool(self.futures_feed.ws_connected and (time.time() - self.futures_feed.last_ws_message_time < 5.0)),
            "error": self.error,
            "uptime_sec": int(time.time() - self.started_at),
            "config": {"equity": self.args.equity, "fee": self.args.fee,
                       "leverage": self.args.leverage, "poll": self.args.poll,
                       "liq_interval": self.args.liq_interval,
                       "no_fetch": self.args.no_fetch},
            "fetching": self._fetching,
            "vwap": vwap_view,
            "vwap_weekly": weekly_vwap,
            "vwap_daily": daily_vwap,
            "volume_oi": vol_oi_snap,
            "klines": ks,
            "liq": self.liq,
            "systems": {s.id: s.snapshot(price) for s in self.strategies},
            "equity_history": self.equity_history[-2000:],
            "events": self.events[-100:],
            "prediction": pred_snap,
        }

    def _save_state(self):
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            STATE_FILE.write_text(json.dumps({
                "saved_at": datetime.now().isoformat(timespec="seconds"),
                "systems": {s.id: {
                    "equity": s.equity, "init_equity": s.init_equity,
                    "position": s.position, "trades": s.trades,
                    "fees_paid": getattr(s, "fees_paid", 0.0)} for s in self.strategies},
                "equity_history": self.equity_history,
                "events": self.events,
            }, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            print("[!] 状态保存失败: %s" % e, flush=True)

    def _load_state(self):
        try:
            d = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            for s in self.strategies:
                sd = d.get("systems", {}).get(s.id)
                if sd:
                    s.equity = sd.get("equity", s.equity)
                    s.init_equity = sd.get("init_equity", s.init_equity)
                    s.position = sd.get("position")
                    s.trades = sd.get("trades", [])
                    if hasattr(s, "fees_paid"):
                        s.fees_paid = sd.get("fees_paid", 0.0)
            self.equity_history = d.get("equity_history", [])
            self.events = d.get("events", [])
            self.emit("sys", "恢复", "已从 %s 恢复状态 (双策略)" % STATE_FILE.name)
        except Exception as e:
            self.emit("sys", "警告", "状态恢复失败: %s" % e)


# ----------------------------------------------------------------------------
# Web 服务
# ----------------------------------------------------------------------------
def build_app(engine):
    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(engine.run())
        yield
        task.cancel()
        engine.futures_feed.stop_websocket()
        engine._save_state()

    app = FastAPI(lifespan=lifespan)

    @app.get("/")
    async def index():
        if DASHBOARD.exists():
            return FileResponse(DASHBOARD)
        return JSONResponse({"error": "dashboard.html 缺失"}, status_code=500)

    @app.get("/history")
    async def history_page():
        history_html = BASE / "web" / "history.html"
        if history_html.exists():
            return FileResponse(history_html)
        return JSONResponse({"error": "history.html 缺失"}, status_code=500)

    @app.get("/api/state")
    async def api_state():
        return engine.state()

    @app.get("/api/prediction")
    async def api_prediction():
        return engine.predictor_service.snapshot()

    @app.get("/api/prediction/history")
    async def api_prediction_history(limit: int = 500, tf: str = None, outcome: str = None):
        all_history = engine.predictor_service.storage.load_history(limit=limit)
        active_map = getattr(engine.predictor_service.lifecycle, "active_predictions", {}) or {}
        # 实时热合并当前内存中活跃生命周期状态，确保二级界面毫秒级同步
        for p in all_history:
            if p.get("status") == "ACTIVE":
                tf_key = p.get("timeframe")
                live_act = active_map.get(tf_key)
                if live_act and live_act.get("pred_id") == p.get("pred_id"):
                    for k in ["stage", "stage_step", "stage_label", "tp1_status", "tp2_status",
                              "tp1_hit_ts", "tp1_hit_price", "secondary_eval", "highest_seen",
                              "lowest_seen", "exit_info", "sl_breached", "timeout_ts", "timeout_iso", "timeout_candles",
                              "roll_count", "tp_tolerance", "extended_targets", "original_tp2"]:
                        if k in live_act:
                            p[k] = live_act[k]

        filtered = all_history
        if tf and tf != "ALL":
            filtered = [h for h in filtered if h.get("timeframe") == tf]
        if outcome and outcome != "ALL":
            filtered = [h for h in filtered if (h.get("verified_result") or {}).get("outcome") == outcome]
        return {
            "total": len(filtered),
            "history": list(reversed(filtered)),
            "metrics": engine.predictor_service.optimizer.compute_accuracy_metrics(all_history)
        }

    @app.post("/api/prediction/reset")
    async def api_prediction_reset():
        res = engine.predictor_service.reset_all_history()
        return res

    @app.post("/api/prediction/review")
    async def api_prediction_review():
        res = engine.predictor_service.trigger_review(reason="Web 用户手动触发 12H 实盘复盘分析")
        return {"ok": True, "report": res}

    @app.get("/api/macro_events")
    async def get_macro_events():
        if hasattr(engine.predictor_service, "macro_manager"):
            return engine.predictor_service.macro_manager.evaluate_composite_events()
        return {}

    return app


def main():
    ap = argparse.ArgumentParser(description="币安 5m 实时数据 · 双策略自动模拟交易 (v2.3 VWAP日基准阶梯版)")
    ap.add_argument("--port", type=int, default=8787, help="Web 监控端口(默认 8787)")
    ap.add_argument("--host", default="0.0.0.0", help="Web 监听地址(默认 0.0.0.0 允许远程访问)")
    ap.add_argument("--equity", type=float, default=1000.0, help="每个系统初始权益 USDT")
    ap.add_argument("--leverage", type=float, default=1.0, help="名义杠杆")
    ap.add_argument("--fee", type=float, default=0.0005, help="单边手续费率(默认 0.05%%)")
    ap.add_argument("--poll", type=float, default=0.5, help="行情轮询间隔秒(配合WebSocket实时推送)")
    ap.add_argument("--liq-data", default=str(LIQ_DATA), help="清算地图回退 JSON(无抓取文件时)")
    ap.add_argument("--liq-interval", type=int, default=1800,
                    help="CoinGlass 自动抓取间隔秒(默认 1800=30 分钟)")
    ap.add_argument("--no-fetch", action="store_true", help="不自动抓取 CoinGlass")
    ap.add_argument("--resume", action="store_true", help="恢复上次状态(权益/持仓/记录)")
    args = ap.parse_args()

    engine = Engine(args)
    app = build_app(engine)
    print("=" * 72)
    print("自动模拟交易 · 币安 ETHUSDT 5m · 双策略纸面交易 (v2.3 VWAP日基准阶梯版)")
    print("  策略一 VWAP: #1#2中心开多空(#2止损/回归VWAP平仓) | #3开仓(#2平仓) | 每日0-4时不开仓")
    print("  策略二 清算地图: 突破清算区开仓 -> 到达 TP 全平 (量仓出清确认 + 1.5%硬止损)")
    print("  量仓维度: 实时 OI + 5m ΔOI + 5m 主动买卖比 + 大户多空比 (币安合约官方数据)")
    print("  VWAP 引擎: Binance 官方 5m 毫秒级自算日内 Daily VWAP (日基准, 北京 00:00 归零)")
    print("  Web 监控端口: %d (已绑定 %s, 可通过 服务器IP:%d 访问)" % (args.port, args.host, args.port))
    print("=" * 72, flush=True)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
