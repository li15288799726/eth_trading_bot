# -*- coding: utf-8 -*-
"""
Polymarket ETH 5分钟 UP/DOWN 自动交易策略 (Polymarket Strategy v2.0)
=============================================================
纯本地模拟盘 (纸面交易):
- 初始资金: 1,000 USDT/USDC (严格遵照用户要求)
- 每笔下单: 10 USDT/USDC (严格遵照用户要求)
- 行情来源: Polymarket 官方 CLOB 实时订单簿 (WebSocket 毫秒级推送)
- 跨策略信号联动 (币安量仓微观结构 + CoinGlass VWAP轨道 + 清算地图):
  1. 量仓主力动量推单 (BULL_ATTACK / BEAR_ATTACK / 5m Taker失衡):
     - 当 5m 主动买入比 >= 1.15 且增仓推单时, 顺势买入 UP Token
     - 当 5m 主动卖出比 <= 0.87 且增仓推单时, 顺势买入 DOWN Token
  2. 均值回归与支撑阻力反弹 (VWAP #1#2 轨道 + 清算区极值):
     - 价格触及 VWAP 下中心值或多头清算区, 结合空头出清 (LONG_FLUSH) 反弹买入 UP
     - 价格触及 VWAP 上中心值或空头清算区, 结合空头逼空衰竭 (SHORT_SQUEEZE) 承压买入 DOWN
  3. 盘口赔率优选与止盈交割:
     - 盘口买价 <= 0.65 时入场 (高期望赔率), 每期 5m 市场限开 1 单 (风控纪律)
     - 盘中浮盈 >= 40% 或盘口达到 >= 0.80 时提前止盈
     - 周期结束由市场切换自动交割兑付 (收盘价 >= 开盘价 UP 胜兑现 $1.00, 反之 DOWN 胜兑现 $1.00)
"""
import time
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

ORDER_COST = 10.0      # 每笔固定 10 USDT/USDC
INIT_EQUITY = 1000.0   # 初始资金 1000 USDT/USDC

# Polymarket 官方 CLOB 手续费模型 (crypto_fees_v2):
# Maker (挂单): 0% 费率, 享受 20% Taker 手续费返佣 (Rebate)
# Taker (吃单): Fee = Shares * 0.07 * Price * (1 - Price)
POLY_FEE_RATE = 0.07     # 官方加密市场 Taker 系数 (0.07)
POLY_REBATE_RATE = 0.20  # Maker 挂单返佣比例 (20%)

def calc_taker_fee(shares, price):
    """Polymarket 官方动态 Taker 手续费公式 (crypto_fees_v2)"""
    if not price or price <= 0.0 or price >= 1.0:
        return 0.0
    return round(float(shares) * POLY_FEE_RATE * float(price) * (1.0 - float(price)), 4)


class PolymarketStrategy:
    def __init__(self, engine, equity=INIT_EQUITY, trade_size=ORDER_COST):
        self.id = "polymarket"
        self.title = "策略三 · Polymarket ETH 5m UP/DOWN"
        self.desc = "5分钟二元预测: 结合币安量仓进攻与 VWAP/清算区极值 (每笔 10 U 模拟下单)"
        self.engine = engine
        self.init_equity = float(equity)
        self.cash = float(equity)
        self.trade_size = float(trade_size)

        self.position = None   # 当前持仓: {side, slug, token_id, entry_price, shares, cost, entry_eth_price, entry_time, reason}
        self.trades = []       # 历史成交: [{time, side, cost, entry_price, exit_price, pnl, pnl_pct, reason, equity}]
        self.fees_paid = 0.0   # 累计支付官方 CLOB 手续费 (crypto_fees_v2)
        self.market_start_eth_price = {} # {slug: eth_price_at_start}
        self.last_trade_slug = None
        self._last_guard_warn = 0.0

    @property
    def equity(self):
        """动态总权益 = 现金 + 当前持仓以中间价/买一价计量的浮动市值"""
        pos_val = 0.0
        if self.position:
            pos_val = self.position["shares"] * self.position.get("current_price", self.position["entry_price"])
        return round(self.cash + pos_val, 2)

    @equity.setter
    def equity(self, val):
        """兼容状态恢复: 无持仓时同步 cash，有持仓时权益由 cash+持仓市值计算"""
        if not self.position and val is not None:
            self.cash = float(val)

    def on_market_switch(self, old_market, new_market, current_eth_price):
        """当 Polymarket 轮换到新 5 分钟市场时，触发旧市场未平仓订单结算"""
        if not old_market:
            return

        old_slug = old_market.get("slug")
        # 记录新市场初始 ETH 价格
        if new_market and current_eth_price:
            self.market_start_eth_price[new_market.get("slug")] = current_eth_price

        # 如果在旧市场持有未平仓订单，执行到期交割结算
        if self.position and self.position.get("slug") == old_slug:
            self._settle_market(old_market, current_eth_price)

    def _settle_market(self, market, end_eth_price):
        """5 分钟到期交割结算 (Polymarket 规则: 收盘价 >= 开盘价 则 UP 兑付 1.00，反之 DOWN 兑付 1.00)"""
        pos = self.position
        if not pos:
            return
        self.position = None  # 立即置空，杜绝异常导致死循环重复交割
        slug = market.get("slug")
        start_eth_price = self.market_start_eth_price.get(slug, pos.get("entry_eth_price", end_eth_price))

        is_up_win = end_eth_price >= start_eth_price
        winning_side = "UP" if is_up_win else "DOWN"
        won = (pos["side"] == winning_side)

        exit_price = 1.00 if won else 0.00
        gross_payout = pos["shares"] * exit_price
        total_fee = round(pos.get("entry_fee", 0.0), 4)
        pnl = round(gross_payout - pos["cost"], 4)
        self.cash += gross_payout

        res_text = "获胜全额兑付 $1.00 (免交割费)" if won else "未中归零 $0.00"
        eth_delta = end_eth_price - start_eth_price
        reason = f"5m 到期交割结算 | ETH {start_eth_price:.2f} -> {end_eth_price:.2f} ({eth_delta:+.2f}) -> {winning_side} 胜 | {res_text} (手续费 {total_fee:.3f}U)"

        self.trades.append({
            "time": datetime.now().isoformat(timespec="seconds"),
            "ts": int(time.time()),
            "market_slug": slug,
            "market_title": market.get("title", slug),
            "period_cst": market.get("period_cst", pos.get("period_cst", "")),
            "side": pos["side"],
            "side_cn": pos.get("side_cn", "看涨 UP" if pos["side"] == "UP" else "看跌 DOWN"),
            "cost": pos["cost"],
            "shares": round(pos["shares"], 4),
            "entry_price": pos["entry_price"],
            "exit_price": exit_price,
            "entry_eth_price": round(start_eth_price, 2),
            "exit_eth_price": round(end_eth_price, 2),
            "eth_delta": round(eth_delta, 2),
            "won": won,
            "fee": total_fee,
            "fees": total_fee,
            "payout": round(gross_payout, 2),
            "pnl": round(pnl, 4),
            "pnl_pct": round((pnl / pos["cost"]) * 100.0, 2),
            "equity": self.equity,
            "reason": reason,
        })

        verb = "盈利交割" if won else "亏损交割"
        try:
            self.engine.emit(self.id, verb,
                             f"{slug} 结算: 持有 {pos['side']} {pos['shares']:.2f} 份 | 兑现 {gross_payout:.2f} U | 盈亏 {pnl:+.2f} U | {reason}")
        except Exception:
            pass

    def _fill(self, side, token_id, ask_price, market, eth_price, reason):
        """按固定 10 USDT 下单买入 UP 或 DOWN Token (严格扣除官方 crypto_fees_v2 Taker 手续费)"""
        shares = round(self.trade_size / ask_price, 4)
        fee = calc_taker_fee(shares, ask_price)
        total_req = self.trade_size + fee

        if self.cash < total_req:
            self.engine.emit(self.id, "资金不足", f"当前可用现金 {self.cash:.2f} U 低于单笔下单所需 {total_req:.2f} U (含官方手续费 {fee:.4f} U)")
            return

        self.cash -= total_req
        self.fees_paid = round(self.fees_paid + fee, 4)
        slug = market.get("slug", "")
        self.last_trade_slug = slug

        self.position = {
            "side": side,
            "side_cn": "看涨 UP" if side == "UP" else "看跌 DOWN",
            "token_id": token_id,
            "slug": slug,
            "title": market.get("title", slug),
            "period_cst": market.get("period_cst", ""),
            "entry_price": ask_price,
            "current_price": ask_price,
            "shares": shares,
            "cost": self.trade_size,
            "entry_fee": fee,
            "entry_eth_price": eth_price,
            "entry_time": datetime.now().strftime("%H:%M:%S"),
            "entry_time_full": datetime.now().isoformat(timespec="seconds"),
            "reason": reason,
        }

        self.engine.emit(self.id, f"买入{side}",
                         f"Polymarket 模拟下单: 买入 {side} @ {ask_price:.4f} (本金 {self.trade_size:.2f} U + 官方手续费 {fee:.4f} U, 获 {shares:.2f} 份) | ETH @ {eth_price:.2f} | 原因: {reason}")

    def _early_take_profit(self, bid_price, eth_price, reason):
        """盘中提前止盈 (若持仓 Token 浮盈 >= 40% 或价格达到 >= 0.80, 提前保盈锁定利润)"""
        pos = self.position
        if not pos:
            return
        self.position = None  # 立即置空
        gross_payout = pos["shares"] * bid_price
        exit_fee = calc_taker_fee(pos["shares"], bid_price)
        net_payout = gross_payout - exit_fee
        total_fee = round(pos.get("entry_fee", 0.0) + exit_fee, 4)
        pnl = round(net_payout - pos["cost"], 4)
        self.cash += net_payout
        self.fees_paid = round(self.fees_paid + exit_fee, 4)

        self.trades.append({
            "time": datetime.now().isoformat(timespec="seconds"),
            "ts": int(time.time()),
            "market_slug": pos["slug"],
            "market_title": pos.get("title", pos["slug"]),
            "period_cst": pos.get("period_cst", ""),
            "side": pos["side"],
            "side_cn": pos.get("side_cn", "看涨 UP" if pos["side"] == "UP" else "看跌 DOWN"),
            "cost": pos["cost"],
            "shares": round(pos["shares"], 4),
            "entry_price": pos["entry_price"],
            "exit_price": bid_price,
            "entry_eth_price": round(pos.get("entry_eth_price", eth_price), 2),
            "exit_eth_price": round(eth_price, 2),
            "eth_delta": round(eth_price - pos.get("entry_eth_price", eth_price), 2),
            "won": True,
            "fee": total_fee,
            "fees": total_fee,
            "payout": round(gross_payout, 2),
            "pnl": round(pnl, 4),
            "pnl_pct": round((pnl / pos["cost"]) * 100.0, 2),
            "equity": self.equity,
            "reason": f"盘中提前止盈锁定利润 | 净利 {pnl:+.2f} U (含手续费 {total_fee:.3f}U) | {reason}",
        })

        try:
            self.engine.emit(self.id, "提前止盈",
                             f"{pos['slug']} 提前兑现: 卖出 {pos['side']} @ {bid_price:.4f} | 净回收 {net_payout:.2f} U (手续费 {total_fee:.3f}U) | 净利 {pnl:+.2f} U | {reason}")
        except Exception:
            pass

    def on_tick(self, eth_price, vol_oi, vwap_bands, liq_zones, poly_snapshot):
        """毫秒级 tick 驱动: 评估盘口与跨策略信号"""
        if not poly_snapshot or not poly_snapshot.get("connected"):
            return

        market = poly_snapshot.get("market")
        if not market or not eth_price:
            return

        slug = market.get("slug", "")
        if slug not in self.market_start_eth_price:
            self.market_start_eth_price[slug] = eth_price

        # 如果当前持仓属于过去的已到期市场(例如程序重启加载了历史持仓)，立即执行到期交割结算
        if self.position and self.position.get("slug") != slug:
            past_market = {
                "slug": self.position["slug"],
                "title": self.position.get("title", self.position["slug"]),
                "period_cst": self.position.get("period_cst", "")
            }
            self._settle_market(past_market, eth_price)

        up_book = poly_snapshot.get("up", {})
        down_book = poly_snapshot.get("down", {})

        up_ask = up_book.get("best_ask", 0.51)
        up_bid = up_book.get("best_bid", 0.49)
        down_ask = down_book.get("best_ask", 0.51)
        down_bid = down_book.get("best_bid", 0.49)

        # 更新持仓浮动市价
        if self.position and self.position.get("slug") == slug:
            if self.position["side"] == "UP":
                self.position["current_price"] = up_book.get("mid", up_bid)
            else:
                self.position["current_price"] = down_book.get("mid", down_bid)

            # 提前止盈检查: 浮盈 >= 40% 或盘口买一 >= 0.80
            entry_p = self.position["entry_price"]
            curr_bid = up_bid if self.position["side"] == "UP" else down_bid
            if curr_bid is not None and (curr_bid >= entry_p * 1.40 or curr_bid >= 0.80):
                self._early_take_profit(curr_bid, eth_price, f"收益达标提前止盈 (开仓 {entry_p:.2f} -> 现价 {curr_bid:.2f})")
                return

        # 每期 5m 市场限做 1 单 (风控纪律，杜绝单期频繁换手手续费损耗)
        if self.position is not None or self.last_trade_slug == slug:
            return

        # --------------------------------------------------------------------
        # 跨系统信号提取 (币安量仓 + VWAP + 清算地图)
        # --------------------------------------------------------------------
        sentiment = vol_oi.get("sentiment", {}) if vol_oi else {}
        regime = sentiment.get("regime", "NEUTRAL")
        can_short = sentiment.get("can_short", True)
        can_long = sentiment.get("can_long", True)
        # 从 vol_oi 中准确提取 5m 主动买卖比与持仓变化量
        taker_ratio = vol_oi.get("buy_sell_ratio_5m", 1.0) if vol_oi else 1.0
        oi_delta = vol_oi.get("oi_delta_5m", 0.0) if vol_oi else 0.0

        vwap = vwap_bands.get("vwap") if vwap_bands else None
        mid_upper = vwap_bands.get("mid_upper") if vwap_bands else None
        mid_lower = vwap_bands.get("mid_lower") if vwap_bands else None

        long_zones = liq_zones.get("long_zones", []) if liq_zones else []
        short_zones = liq_zones.get("short_zones", []) if liq_zones else []

        up_token = market.get("up_token")
        down_token = market.get("down_token")

        # --------------------------------------------------------------------
        # 信号判定: 多层级量仓微观动量 + VWAP中心值/清算区极值 (每期 5m 择优触发 1 单)
        # --------------------------------------------------------------------
        # 层级 1: 支撑阻力与清算区极值反转 (最高确定性)
        hit_vwap_support = (mid_lower and eth_price <= mid_lower + 5.0)
        hit_liq_support = (long_zones and eth_price <= long_zones[0] + 4.0)
        up_reversal = ((hit_vwap_support or hit_liq_support) and (regime == "LONG_FLUSH" or can_long))

        hit_vwap_resist = (mid_upper and eth_price >= mid_upper - 5.0)
        hit_liq_resist = (short_zones and eth_price >= short_zones[0] - 4.0)
        down_reversal = ((hit_vwap_resist or hit_liq_resist) and (regime == "SHORT_SQUEEZE" or can_short))

        # 层级 2: 主力进攻态势 (BULL_ATTACK / BEAR_ATTACK)
        up_attack = (regime == "BULL_ATTACK" or (taker_ratio >= 1.08 and oi_delta > 0))
        down_attack = (regime == "BEAR_ATTACK" or (taker_ratio <= 0.94 and oi_delta > 0))

        # 层级 3: 5分钟微观主动流偏向 (买卖比偏离 > 3% 且盘口赔率合理)
        up_micro = (taker_ratio >= 1.03 and can_long and (vwap is None or eth_price >= vwap - 10.0))
        down_micro = (taker_ratio <= 0.97 and can_short and (vwap is None or eth_price <= vwap + 10.0))

        # --------------------------------------------------------------------
        # 执行 1: 买入看涨 Token "UP"
        # --------------------------------------------------------------------
        if (up_reversal or up_attack or up_micro) and up_ask is not None and (0.20 <= up_ask <= 0.65):
            if up_reversal:
                reason_type = "VWAP下轨/清算区支撑企稳反弹"
            elif up_attack:
                reason_type = "币安主力增仓推单 (BULL_ATTACK)"
            else:
                reason_type = "5m主动买盘流优势推升"
            reason = f"{reason_type} | 买入 UP @ {up_ask:.4f} | 买卖比: {taker_ratio:.2f} | 量仓: {sentiment.get('label', '均衡')}"
            self._fill("UP", up_token, up_ask, market, eth_price, reason)
            return

        # --------------------------------------------------------------------
        # 执行 2: 买入看跌 Token "DOWN"
        # --------------------------------------------------------------------
        if (down_reversal or down_attack or down_micro) and down_ask is not None and (0.20 <= down_ask <= 0.65):
            if down_reversal:
                reason_type = "VWAP上轨/清算区阻力承压回落"
            elif down_attack:
                reason_type = "币安主力增仓砸盘 (BEAR_ATTACK)"
            else:
                reason_type = "5m主动卖盘流优势压制"
            reason = f"{reason_type} | 买入 DOWN @ {down_ask:.4f} | 买卖比: {taker_ratio:.2f} | 量仓: {sentiment.get('label', '均衡')}"
            self._fill("DOWN", down_token, down_ask, market, eth_price, reason)
            return

    def snapshot(self, eth_price=None):
        pos = None
        if self.position:
            p = self.position
            curr_p = p.get("current_price", p["entry_price"])
            upnl = round((curr_p - p["entry_price"]) * p["shares"], 2)
            curr_eth = float(eth_price) if eth_price else p["entry_eth_price"]
            eth_delta = round(curr_eth - p["entry_eth_price"], 2)
            is_winning = (eth_delta >= 0) if p["side"] == "UP" else (eth_delta <= 0)
            exp_payout = round(p["shares"] * 1.0, 2) if is_winning else 0.0
            exp_pnl = round(exp_payout - p["cost"], 2)

            pos = {
                "side": p["side"],
                "side_cn": p.get("side_cn", "看涨 UP" if p["side"]=="UP" else "看跌 DOWN"),
                "slug": p["slug"],
                "title": p["title"],
                "period_cst": p.get("period_cst", ""),
                "entry_price": round(p["entry_price"], 4),
                "current_price": round(curr_p, 4),
                "shares": round(p["shares"], 2),
                "cost": round(p["cost"], 2),
                "entry_eth_price": round(p["entry_eth_price"], 2),
                "current_eth_price": round(curr_eth, 2),
                "eth_delta": eth_delta,
                "is_winning": is_winning,
                "status_text": f"当前{'看涨领先' if p['side']=='UP' else '看跌领先'}(获胜领先)" if is_winning else f"当前{'看跌领先' if p['side']=='UP' else '看涨领先'}(博弈中)",
                "expected_payout": exp_payout,
                "expected_pnl": exp_pnl,
                "entry_time": p.get("entry_time", p.get("entry_time_full", "--")),
                "upnl": upnl,
                "upnl_pct": round((upnl / p["cost"]) * 100.0, 2),
                "reason": p["reason"],
            }

        wins = sum(1 for t in self.trades if t["pnl"] > 0)

        # 确保历史与当前所有交易记录均包含基于官网 crypto_fees_v2 计算的官方手续费
        normalized_trades = []
        accumulated_fee = 0.0
        for t in self.trades:
            t_copy = dict(t)
            if "fee" not in t_copy or t_copy["fee"] is None:
                sh = float(t_copy.get("shares", 20.0))
                ep = float(t_copy.get("entry_price", 0.5))
                xp = float(t_copy.get("exit_price", 0.0))
                fee_in = calc_taker_fee(sh, ep)
                fee_out = calc_taker_fee(sh, xp) if (0.0 < xp < 1.0) else 0.0
                f = round(fee_in + fee_out, 4)
                t_copy["fee"] = f
                t_copy["fees"] = f
            accumulated_fee = round(accumulated_fee + float(t_copy.get("fee", 0.0)), 4)
            normalized_trades.append(t_copy)

        fees_paid = round(self.fees_paid if self.fees_paid > 0 else accumulated_fee, 4)

        return {
            "id": self.id,
            "title": self.title,
            "desc": self.desc,
            "equity": self.equity,
            "cash": round(self.cash, 2),
            "init_equity": self.init_equity,
            "pnl": round(self.equity - self.init_equity, 2),
            "pnl_pct": round(100.0 * (self.equity / self.init_equity - 1.0), 3),
            "fees_paid": fees_paid,
            "fee_model": "crypto_fees_v2",
            "upnl": pos["upnl"] if pos else 0.0,
            "position": pos,
            "n_trades": len(self.trades),
            "wins": wins,
            "win_rate": round(100.0 * wins / len(self.trades), 1) if self.trades else 0.0,
            "trade_size": self.trade_size,
            "trades": normalized_trades[-50:],
        }
