# -*- coding: utf-8 -*-
"""
Polymarket ETH 5分钟 双向做市与波动循环策略 (策略四 · 实证版)
============================================================
策略逻辑与用户实证目标:
1. 5分钟开局迅速双向建仓 (UP + DOWN):
   - 结合做市商挂单定价与官方 CLOB 最优盘口, 同时买入 UP 与 DOWN
   - 双边名义资金: 各 10 USDT (合计 20 USDT)
   - 建立初始双边对冲, 记录折价率与点差基准 (P_up + P_down)
2. 0.80 高位获利平仓:
   - 盘中实时监听, 当任一方向 Token (UP 或 DOWN) 买一/中间价 >= 0.80 时,
     立即平仓止盈该获胜腿, 锁定现金利润 (获利约 +3~+4 U)
   - 此时另一条腿处于单边裸持状态 (若不回落到期将归零)
3. 0.50 附近回落接回 (高抛低吸闭环):
   - 该方向平仓后, 盘中持续监控其卖一/中间价是否回踩 <= 0.52 (0.50 附近)
   - 若回踩成功, 再次买入补齐该方向, 恢复双边对冲, 循环次数 cycle_count + 1
4. 5分钟到期清算与事实归因:
   - 到期根据 ETH 涨跌交割未平仓头寸 (收盘 >= 开盘 UP 兑现 $1.00, 反之 DOWN 兑现 $1.00)
   - 深度统计归因:
     * 场景 A (循环成功): 成功在 0.8 平仓并在 0.5 接回, 双边收割
     * 场景 B (单边裸持归零): 0.8 平仓后单边持续冲刺未回踩, 留存腿归零
     * 场景 C (窄幅未破微亏点差): 双边均未触及 0.8, 到期一胜一负对冲打平, 仅损耗手续费
"""
import time
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

ORDER_COST = 10.0      # 每边固定 10 USDT
INIT_EQUITY = 1000.0   # 初始资金 1000 USDT
POLY_FEE_RATE = 0.07   # Polymarket 官方加密市场 Taker 系数
POLY_REBATE_RATE = 0.20 # Maker 挂单返佣比例

def calc_taker_fee(shares, price):
    """Polymarket 官方动态 Taker 手续费公式 (crypto_fees_v2)"""
    if not price or price <= 0.0 or price >= 1.0:
        return 0.0
    return round(float(shares) * POLY_FEE_RATE * float(price) * (1.0 - float(price)), 4)


class PolymarketMakerStrategy:
    def __init__(self, engine, equity=INIT_EQUITY, trade_size=ORDER_COST):
        self.id = "poly_maker"
        self.title = "策略四 · 5m 双向做市与波动循环"
        self.desc = "开盘买双项 + 0.8高抛 + 0.5低吸 + 做市折价对冲 (用户实证版)"
        self.engine = engine
        self.init_equity = float(equity)
        self.cash = float(equity)
        self.trade_size = float(trade_size)

        # 当前周期双边持仓状态
        self.position = None
        self.trades = []       # 历史详细交易记录
        self.round_history = []# 周期整体验证统计
        self.fees_paid = 0.0
        self.cycle_count = 0   # 全局成功完成高抛低吸循环次数
        self.market_start_eth_price = {}
        self.last_trade_slug = None
        self._last_tick_time = 0.0

    @property
    def equity(self):
        """动态总权益 = 现金 + 未平仓腿以实时市价计量的浮动资产"""
        pos_val = 0.0
        if self.position:
            up = self.position.get("up", {})
            down = self.position.get("down", {})
            if up.get("status") in ("HOLDING", "RE_ENTERED"):
                pos_val += up.get("shares", 0.0) * up.get("current_price", up.get("entry_price", 0.5))
            if down.get("status") in ("HOLDING", "RE_ENTERED"):
                pos_val += down.get("shares", 0.0) * down.get("current_price", down.get("entry_price", 0.5))
        return round(self.cash + pos_val, 2)

    @equity.setter
    def equity(self, val):
        if not self.position and val is not None:
            self.cash = float(val)

    def on_market_switch(self, old_market, new_market, current_eth_price):
        """市场轮换事件"""
        if not old_market:
            return
        old_slug = old_market.get("slug")
        if new_market and current_eth_price:
            self.market_start_eth_price[new_market.get("slug")] = current_eth_price

        if self.position and self.position.get("slug") == old_slug:
            self._settle_market(old_market, current_eth_price)

    def _settle_market(self, market, end_eth_price):
        """5 分钟周期到期清算交割"""
        pos = self.position
        if not pos:
            return
        self.position = None  # 置空，防重复触发
        slug = market.get("slug")
        start_eth_price = self.market_start_eth_price.get(slug, pos.get("entry_eth_price", end_eth_price))

        is_up_win = end_eth_price >= start_eth_price
        winning_side = "UP" if is_up_win else "DOWN"
        eth_delta = round(end_eth_price - start_eth_price, 2)

        up = pos["up"]
        down = pos["down"]

        # 计算双边期末收益
        up_payout = 0.0
        up_won = False
        if up["status"] in ("HOLDING", "RE_ENTERED"):
            if winning_side == "UP":
                up_payout = round(up["shares"] * 1.00, 4)
                up_won = True
            self.cash += up_payout

        down_payout = 0.0
        down_won = False
        if down["status"] in ("HOLDING", "RE_ENTERED"):
            if winning_side == "DOWN":
                down_payout = round(down["shares"] * 1.00, 4)
                down_won = True
            self.cash += down_payout

        # 计算本期总投入与总回收
        total_cost = pos.get("total_cost", self.trade_size * 2)
        total_payout = round(up.get("exit_payout", 0.0) + down.get("exit_payout", 0.0) + up_payout + down_payout, 4)
        total_fee = round(pos.get("total_fee", 0.0), 4)
        round_pnl = round(total_payout - total_cost, 4)
        round_won = (round_pnl > 0)

        # 深度事实归因诊断
        cyc = pos.get("cycle_count", 0)
        up_tp = (up["status"] == "TP_EXITED")
        down_tp = (down["status"] == "TP_EXITED")

        if cyc > 0 and round_pnl > 0:
            diag = f"【实证·循环做市成功】完成 {cyc} 次高抛低吸循环，双边对冲大胜，净利 {round_pnl:+.2f} U"
            diag_tag = "循环成功"
        elif (up_tp and not is_up_win) or (down_tp and is_up_win):
            # 止盈方赢了，但是另一方是输家，导致另一方归零
            diag = f"【实证·单边裸持归零】高位0.80平仓获利，但行情未回踩0.50未接回，留存腿归零 | 净盈亏 {round_pnl:+.2f} U"
            diag_tag = "单边归零"
        elif not up_tp and not down_tp:
            diag = f"【实证·窄幅到期对冲】双边均未冲上0.80，到期一胜一负兑现 $1.00 | 仅扣点差手续费，净盈亏 {round_pnl:+.2f} U"
            diag_tag = "平局磨损"
        else:
            diag = f"【实证·交割完成】ETH {start_eth_price:.2f}->{end_eth_price:.2f} ({eth_delta:+.2f}) -> {winning_side} 胜 | 净利 {round_pnl:+.2f} U"
            diag_tag = "交割结算"

        reason = f"5m 到期清算 | {diag} | ETH {start_eth_price:.2f} -> {end_eth_price:.2f} ({winning_side}胜)"

        settle_record = {
            "time": datetime.now().isoformat(timespec="seconds"),
            "ts": int(time.time()),
            "market_slug": slug,
            "market_title": market.get("title", slug),
            "period_cst": market.get("period_cst", pos.get("period_cst", "")),
            "action": "到期结算",
            "side": winning_side,
            "side_cn": f"交割 {winning_side}胜",
            "cost": round(total_cost, 2),
            "shares": round(up.get("shares", 0.0) + down.get("shares", 0.0), 2),
            "entry_price": round(pos.get("combined_entry_price", 1.0), 4),
            "exit_price": 1.00 if round_won else 0.00,
            "entry_eth_price": round(start_eth_price, 2),
            "exit_eth_price": round(end_eth_price, 2),
            "eth_delta": eth_delta,
            "won": round_won,
            "fee": total_fee,
            "fees": total_fee,
            "payout": round(total_payout, 2),
            "pnl": round_pnl,
            "pnl_pct": round((round_pnl / total_cost) * 100.0, 2),
            "equity": self.equity,
            "cycle_count": cyc,
            "diag_tag": diag_tag,
            "reason": reason,
        }
        self.trades.append(settle_record)
        self.round_history.append(settle_record)

        verb = "盈利交割" if round_won else "亏损交割"
        try:
            self.engine.emit(self.id, verb, f"{slug} 清算: 投入 {total_cost:.1f}U | 兑现 {total_payout:.2f}U | 净盈亏 {round_pnl:+.2f}U | {diag}")
        except Exception:
            pass

    def _open_dual_positions(self, market, up_ask, down_ask, eth_price):
        """开盘迅速买双项 (做市商挂单定价与最优盘口)"""
        up_shares = round(self.trade_size / up_ask, 4)
        down_shares = round(self.trade_size / down_ask, 4)

        up_fee = calc_taker_fee(up_shares, up_ask)
        down_fee = calc_taker_fee(down_shares, down_ask)
        total_req = self.trade_size * 2 + up_fee + down_fee

        if self.cash < total_req:
            self.engine.emit(self.id, "资金不足", f"可用现金 {self.cash:.2f} U 低于双向建仓所需 {total_req:.2f} U")
            return

        self.cash -= total_req
        total_fee = round(up_fee + down_fee, 4)
        self.fees_paid = round(self.fees_paid + total_fee, 4)

        slug = market.get("slug", "")
        self.last_trade_slug = slug
        combined_price = round(up_ask + down_ask, 4)

        self.position = {
            "slug": slug,
            "title": market.get("title", slug),
            "period_cst": market.get("period_cst", ""),
            "entry_time": datetime.now().strftime("%H:%M:%S"),
            "entry_time_full": datetime.now().isoformat(timespec="seconds"),
            "entry_eth_price": eth_price,
            "cycle_count": 0,
            "total_cost": self.trade_size * 2,
            "total_fee": total_fee,
            "combined_entry_price": combined_price,
            "up": {
                "status": "HOLDING",
                "token_id": market.get("up_token"),
                "shares": up_shares,
                "entry_price": up_ask,
                "current_price": up_ask,
                "cost": self.trade_size,
                "fee": up_fee,
                "exit_payout": 0.0,
                "exit_price": None,
                "exit_pnl": 0.0,
            },
            "down": {
                "status": "HOLDING",
                "token_id": market.get("down_token"),
                "shares": down_shares,
                "entry_price": down_ask,
                "current_price": down_ask,
                "cost": self.trade_size,
                "fee": down_fee,
                "exit_payout": 0.0,
                "exit_price": None,
                "exit_pnl": 0.0,
            }
        }

        # 记录建仓流水
        entry_reason = f"5m开局迅速买双项 | 买入UP@{up_ask:.4f}({up_shares:.2f}份) + DOWN@{down_ask:.4f}({down_shares:.2f}份) | 合计价差基准 {combined_price:.4f} | 双向对冲监控开启"
        self.trades.append({
            "time": datetime.now().isoformat(timespec="seconds"),
            "ts": int(time.time()),
            "market_slug": slug,
            "market_title": market.get("title", slug),
            "period_cst": market.get("period_cst", ""),
            "action": "开盘双向建仓",
            "side": "DUAL",
            "side_cn": "双向做市建仓",
            "cost": self.trade_size * 2,
            "shares": round(up_shares + down_shares, 2),
            "entry_price": combined_price,
            "exit_price": combined_price,
            "fee": total_fee,
            "fees": total_fee,
            "payout": 0.0,
            "pnl": 0.0,
            "pnl_pct": 0.0,
            "won": None,
            "equity": self.equity,
            "reason": entry_reason,
        })

        self.engine.emit(self.id, "双向建仓",
                         f"Polymarket 双向建仓: 买入 UP @ {up_ask:.4f} + DOWN @ {down_ask:.4f} (总本金 {self.trade_size*2:.1f}U, 手续费 {total_fee:.3f}U) | 组合合价 {combined_price:.4f} | ETH @ {eth_price:.2f}")

    def _tp_exit_leg(self, side, bid_price, eth_price):
        """0.80 高位获利平仓"""
        pos = self.position
        if not pos:
            return
        leg = pos["up"] if side == "UP" else pos["down"]
        if leg["status"] not in ("HOLDING", "RE_ENTERED"):
            return

        shares = leg["shares"]
        gross = round(shares * bid_price, 4)
        fee = calc_taker_fee(shares, bid_price)
        net = round(gross - fee, 4)
        pnl = round(net - leg["cost"], 4)

        self.cash += net
        self.fees_paid = round(self.fees_paid + fee, 4)
        pos["total_fee"] = round(pos["total_fee"] + fee, 4)

        leg["status"] = "TP_EXITED"
        leg["exit_price"] = bid_price
        leg["exit_payout"] = net
        leg["exit_pnl"] = pnl
        leg["exit_time"] = datetime.now().strftime("%H:%M:%S")

        other_side = "DOWN" if side == "UP" else "UP"
        other_leg = pos["down"] if side == "UP" else pos["up"]
        other_curr = other_leg.get("current_price", 0.20)

        reason = f"【用户逻辑触发】{side}冲高触及 {bid_price:.4f} (>=0.80) 平仓止盈 | 回收 {net:.2f}U (净利 {pnl:+.2f}U) | ⚠️ 留存 {other_side} 单边裸持中(现价 {other_curr:.4f})，等待回落0.50接回"

        self.trades.append({
            "time": datetime.now().isoformat(timespec="seconds"),
            "ts": int(time.time()),
            "market_slug": pos["slug"],
            "market_title": pos.get("title", pos["slug"]),
            "period_cst": pos.get("period_cst", ""),
            "action": f"0.80平仓{side}",
            "side": side,
            "side_cn": f"平仓{side}",
            "cost": leg["cost"],
            "shares": shares,
            "entry_price": leg["entry_price"],
            "exit_price": bid_price,
            "fee": fee,
            "fees": fee,
            "payout": round(net, 2),
            "pnl": pnl,
            "pnl_pct": round((pnl / leg["cost"]) * 100.0, 2),
            "won": True,
            "equity": self.equity,
            "reason": reason,
        })

        self.engine.emit(self.id, f"平仓{side}", f"0.80 止盈: 卖出 {side} @ {bid_price:.4f} | 净收回 {net:.2f} U (净利 {pnl:+.2f} U) | 留存 {other_side} 裸持中")

    def _reenter_leg(self, side, ask_price, eth_price):
        """价格回落 0.50 附近再买入 (低位接回，完成波动循环)"""
        pos = self.position
        if not pos:
            return
        leg = pos["up"] if side == "UP" else pos["down"]
        if leg["status"] != "TP_EXITED":
            return

        shares = round(self.trade_size / ask_price, 4)
        fee = calc_taker_fee(shares, ask_price)
        total_req = self.trade_size + fee

        if self.cash < total_req:
            self.engine.emit(self.id, "资金不足", f"接回 {side} 资金不足: 可用 {self.cash:.2f} U < 所需 {total_req:.2f} U")
            return

        self.cash -= total_req
        self.fees_paid = round(self.fees_paid + fee, 4)
        pos["total_fee"] = round(pos["total_fee"] + fee, 4)
        pos["total_cost"] = round(pos["total_cost"] + self.trade_size, 4)

        leg["status"] = "RE_ENTERED"
        leg["shares"] = shares
        leg["entry_price"] = ask_price
        leg["current_price"] = ask_price
        leg["cost"] = self.trade_size
        leg["fee"] = fee

        pos["cycle_count"] = pos.get("cycle_count", 0) + 1
        self.cycle_count += 1

        reason = f"【用户逻辑触发】{side}回落至 {ask_price:.4f} (<=0.52) 成功接回 | 波动做市循环成功+1次 | 重新恢复双边对冲锁定"

        self.trades.append({
            "time": datetime.now().isoformat(timespec="seconds"),
            "ts": int(time.time()),
            "market_slug": pos["slug"],
            "market_title": pos.get("title", pos["slug"]),
            "period_cst": pos.get("period_cst", ""),
            "action": f"0.50接回{side}",
            "side": side,
            "side_cn": f"接回{side}",
            "cost": self.trade_size,
            "shares": shares,
            "entry_price": ask_price,
            "exit_price": ask_price,
            "fee": fee,
            "fees": fee,
            "payout": 0.0,
            "pnl": 0.0,
            "pnl_pct": 0.0,
            "won": None,
            "equity": self.equity,
            "cycle_count": pos["cycle_count"],
            "reason": reason,
        })

        self.engine.emit(self.id, f"接回{side}", f"0.50 回补: 重新买入 {side} @ {ask_price:.4f} (获 {shares:.2f}份) | 循环完成第 {pos['cycle_count']} 次 | 恢复双边锁定")

    def on_tick(self, eth_price, vol_oi, vwap_bands, liq_zones, poly_snapshot):
        """毫秒级盘口与行情更新"""
        if not poly_snapshot or not poly_snapshot.get("connected"):
            return
        market = poly_snapshot.get("market")
        if not market or not eth_price:
            return

        slug = market.get("slug", "")
        if slug not in self.market_start_eth_price:
            self.market_start_eth_price[slug] = eth_price

        # 历史遗留过期市场强制交割
        if self.position and self.position.get("slug") != slug:
            past_m = {"slug": self.position["slug"], "title": self.position.get("title", self.position["slug"]), "period_cst": self.position.get("period_cst", "")}
            self._settle_market(past_m, eth_price)

        up_book = poly_snapshot.get("up", {})
        down_book = poly_snapshot.get("down", {})
        up_bid = up_book.get("best_bid", 0.49)
        up_ask = up_book.get("best_ask", 0.51)
        down_bid = down_book.get("best_bid", 0.49)
        down_ask = down_book.get("best_ask", 0.51)

        # -------------------------------------------------------------
        # 1. 若当前持有本期头寸: 更新市价并检测 0.80平仓 与 0.50接回
        # -------------------------------------------------------------
        if self.position and self.position.get("slug") == slug:
            pos = self.position
            up = pos["up"]
            down = pos["down"]

            up["current_price"] = up_book.get("mid", up_bid)
            down["current_price"] = down_book.get("mid", down_bid)

            # 检测 0.80 高位获利平仓 (UP)
            if up["status"] in ("HOLDING", "RE_ENTERED") and up_bid is not None and up_bid >= 0.80:
                self._tp_exit_leg("UP", up_bid, eth_price)
                return

            # 检测 0.80 高位获利平仓 (DOWN)
            if down["status"] in ("HOLDING", "RE_ENTERED") and down_bid is not None and down_bid >= 0.80:
                self._tp_exit_leg("DOWN", down_bid, eth_price)
                return

            # 检测 0.50 附近回落接回 (UP)
            if up["status"] == "TP_EXITED" and up_ask is not None and up_ask <= 0.52:
                self._reenter_leg("UP", up_ask, eth_price)
                return

            # 检测 0.50 附近回落接回 (DOWN)
            if down["status"] == "TP_EXITED" and down_ask is not None and down_ask <= 0.52:
                self._reenter_leg("DOWN", down_ask, eth_price)
                return

            return

        # -------------------------------------------------------------
        # 2. 若当前无持仓: 检测新 5m 周期开启，迅速买双项
        # -------------------------------------------------------------
        if self.position is None and self.last_trade_slug != slug:
            rem = poly_snapshot.get("remaining_sec", 300)
            # 在 5m 周期内 (剩余秒数 >= 100) 迅速双向建仓
            if rem >= 100 and up_ask is not None and down_ask is not None:
                if 0.10 <= up_ask <= 0.90 and 0.10 <= down_ask <= 0.90:
                    self._open_dual_positions(market, up_ask, down_ask, eth_price)

    def snapshot(self, eth_price=None):
        """Web 端快照与展示"""
        pos = None
        if self.position:
            p = self.position
            up = p["up"]
            down = p["down"]

            up_val = (up["shares"] * up["current_price"]) if up["status"] in ("HOLDING", "RE_ENTERED") else up.get("exit_payout", 0.0)
            down_val = (down["shares"] * down["current_price"]) if down["status"] in ("HOLDING", "RE_ENTERED") else down.get("exit_payout", 0.0)
            curr_val = round(up_val + down_val, 2)
            tot_cost = round(p.get("total_cost", self.trade_size * 2), 2)
            upnl = round(curr_val - tot_cost, 2)

            curr_eth = float(eth_price) if eth_price else p["entry_eth_price"]
            eth_delta = round(curr_eth - p["entry_eth_price"], 2)

            pos = {
                "slug": p["slug"],
                "title": p["title"],
                "period_cst": p.get("period_cst", ""),
                "entry_time": p.get("entry_time", "--"),
                "entry_eth_price": round(p["entry_eth_price"], 2),
                "current_eth_price": round(curr_eth, 2),
                "eth_delta": eth_delta,
                "cycle_count": p.get("cycle_count", 0),
                "total_cost": tot_cost,
                "current_value": curr_val,
                "upnl": upnl,
                "upnl_pct": round((upnl / tot_cost) * 100.0, 2) if tot_cost > 0 else 0.0,
                "up": {
                    "status": up["status"],
                    "status_cn": "持仓中" if up["status"]=="HOLDING" else ("已0.8平仓" if up["status"]=="TP_EXITED" else "已0.5接回"),
                    "shares": round(up["shares"], 2),
                    "entry_price": round(up["entry_price"], 4),
                    "current_price": round(up["current_price"], 4),
                    "exit_price": round(up["exit_price"], 4) if up.get("exit_price") else None,
                    "exit_pnl": round(up.get("exit_pnl", 0.0), 2),
                },
                "down": {
                    "status": down["status"],
                    "status_cn": "持仓中" if down["status"]=="HOLDING" else ("已0.8平仓" if down["status"]=="TP_EXITED" else "已0.5接回"),
                    "shares": round(down["shares"], 2),
                    "entry_price": round(down["entry_price"], 4),
                    "current_price": round(down["current_price"], 4),
                    "exit_price": round(down.get("exit_price"), 4) if down.get("exit_price") else None,
                    "exit_pnl": round(down.get("exit_pnl", 0.0), 2),
                }
            }

        # 胜率与统计
        settled_trades = [t for t in self.trades if t.get("action") == "到期结算"]
        wins = sum(1 for t in settled_trades if t.get("pnl", 0) > 0)
        total_rounds = len(settled_trades)

        return {
            "id": self.id,
            "title": self.title,
            "desc": self.desc,
            "equity": self.equity,
            "cash": round(self.cash, 2),
            "init_equity": self.init_equity,
            "pnl": round(self.equity - self.init_equity, 2),
            "pnl_pct": round(100.0 * (self.equity / self.init_equity - 1.0), 3),
            "fees_paid": round(self.fees_paid, 4),
            "fee_model": "Maker 0% (返佣 20%) / Taker 7%",
            "upnl": pos["upnl"] if pos else 0.0,
            "position": pos,
            "cycle_count": self.cycle_count,
            "n_trades": len(self.trades),
            "total_rounds": total_rounds,
            "wins": wins,
            "win_rate": round(100.0 * wins / total_rounds, 1) if total_rounds else 0.0,
            "trade_size": self.trade_size,
            "trades": self.trades[-50:],
        }
