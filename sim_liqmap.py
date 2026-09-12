# -*- coding: utf-8 -*-
"""
模拟交易系统 2 —— ETH 清算地图策略(纸面模拟, 不接交易所, 模拟价格)
====================================================================
数据源(文件夹10): test_data/exliqmap_eth_raw.json
  (三所聚合 ETH 清算地图快照, Binance/OKX/Bybit 逐价格档合并)

清算区提取:
  - 多头清算区(价格下方, 跌爆多) / 空头清算区(价格上方, 涨爆空)
  - 显著档 = 清算额 >= 该侧最大档 50% 的价格档
  - 清算区 1/2/3 = 显著档中离当前价最近的三档(按距离排序)

策略规则(用户确认):
  - 开仓方向按 DIRECTION 配置(见下方说明), 默认 fade
  - fade:   跌破多头清算区1 -> 开多, 跌破区2/区3 各加仓; 涨破空头清算区1 -> 开空, 同理加仓
  - 平仓:  价格到达 多空清算中心 TP = (最低多头清算价 + 最高空头清算价) / 2 时全平

方向说明(重要):
  按"跌破多头清算区->开空"直译(momentum 模式), 开仓价(约2359-2370)在
  平仓目标 TP(约2392.9)的下方, 每笔必然亏损。结合平仓公式(两侧清算区
  极值的中点, 位于两簇之间), 自洽的方向是逆瀑布(fade): 跌入多头清算簇
  开多(买爆仓底), 涨入空头清算簇开空。如需直译方向, 改 DIRECTION="momentum"。

用法:
  python sim_liqmap.py                  # 无限循环, Ctrl+C 结束并打印总结
  python sim_liqmap.py --ticks 8000     # 固定步数
  python sim_liqmap.py --interval 0    # 不休眠(快速回放)
成交记录: data/sim/liqmap_trades.json
"""
import argparse
import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE = Path(__file__).resolve().parent
DATA_FILE = BASE / "test_data" / "exliqmap_eth_raw.json"
OUT_FILE = BASE / "data" / "sim" / "liqmap_trades.json"

DIRECTION = "fade"        # "fade"=逆瀑布(默认) / "momentum"=直译突破方向
SIG_FRAC = 0.5            # 显著清算档阈值: 金额 >= 该侧最大档的 50%

LONG_NAME = "多头清算区"   # 下方, 跌爆多
SHORT_NAME = "空头清算区"  # 上方, 涨爆空


def load_zones(path):
    """解析三所聚合清算地图 -> 多/空两侧清算区 1/2/3 与 TP"""
    d = json.loads(Path(path).read_text(encoding="utf-8"))
    last_price = float(d["lastPrice"])

    agg = {}  # 价格 -> 三所合计清算额
    for ex in d.get("data", []):
        for p, rows in (ex.get("liqMapV2") or {}).items():
            price = float(p)
            agg[price] = agg.get(price, 0.0) + sum(r[1] for r in rows)

    long_lv = {p: a for p, a in agg.items() if p < last_price}    # 多头爆仓区
    short_lv = {p: a for p, a in agg.items() if p > last_price}   # 空头爆仓区

    def zones(levels, from_high):
        """显著档中离当前价最近的 3 档, 区1=最近"""
        if not levels:
            return []
        mx = max(levels.values())
        sig = [p for p, a in levels.items() if a >= mx * SIG_FRAC]
        sig.sort(reverse=from_high)  # 下方簇: 高价在前=最近; 上方簇: 低价在前=最近
        return sig[:3]

    L = zones(long_lv, from_high=True)     # 例: [2369.7, 2361.6, 2358.9]
    S = zones(short_lv, from_high=False)  # 例: [2412.9, 2415.6, 2418.3]

    min_long = min(long_lv) if long_lv else None
    max_short = max(short_lv) if short_lv else None
    tp = (min_long + max_short) / 2.0 if (min_long and max_short) else None
    return {"last_price": last_price, "long_zones": L, "short_zones": S, "tp": tp}


class OUPrice:
    """OU 随机游走: 围绕锚点(多空清算中心)波动"""

    def __init__(self, start, anchor, vol=0.0005, theta=0.005):
        self.p = float(start)
        self.anchor = float(anchor)
        self.vol = vol
        self.theta = theta

    def next(self):
        shock = random.gauss(0.0, self.vol * self.p)
        self.p += self.theta * (self.anchor - self.p) + shock
        return self.p


def main():
    ap = argparse.ArgumentParser(description="ETH 清算地图模拟盘")
    ap.add_argument("--data", default=str(DATA_FILE), help="清算地图 JSON 路径")
    ap.add_argument("--ticks", type=int, default=0, help="模拟步数, 0=无限(Ctrl+C 结束)")
    ap.add_argument("--interval", type=float, default=0.3, help="每步间隔秒")
    ap.add_argument("--equity", type=float, default=10000.0, help="初始权益(USDT)")
    ap.add_argument("--size", type=float, default=1.0, help="每次开仓/加仓数量(ETH)")
    ap.add_argument("--vol", type=float, default=0.0005, help="价格波动率(每步, 相对值)")
    ap.add_argument("--seed", type=int, default=None, help="随机种子(复现用)")
    args = ap.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    z = load_zones(args.data)
    L, S, tp, last_price = z["long_zones"], z["short_zones"], z["tp"], z["last_price"]
    if not L or not S or tp is None:
        print("[x] 清算地图数据不完整: 多头区 %d 档 / 空头区 %d 档" % (len(L), len(S)))
        sys.exit(1)

    # fade: 触及下方多头清算区 -> 开多; momentum: 触及上方空头清算区 -> 开多
    long_zones = L if DIRECTION == "fade" else S
    short_zones = S if DIRECTION == "fade" else L
    long_label = LONG_NAME if DIRECTION == "fade" else SHORT_NAME
    short_label = SHORT_NAME if DIRECTION == "fade" else LONG_NAME

    def crossed(side, lv, price):
        """该方向下, price 是否算"突破"清算档 lv"""
        below = (DIRECTION == "fade") == (side > 0)
        return price <= lv if below else price >= lv

    def fmt(levels):
        return " | ".join("区%d %.1f" % (i + 1, p) for i, p in enumerate(levels))

    print("=" * 68)
    print("模拟交易系统 2 —— ETH 清算地图策略(纸面模拟)  方向模式: %s" % DIRECTION)
    print("当前价 %.2f" % last_price)
    print("%s(下方): %s" % (LONG_NAME, fmt(L)))
    print("%s(上方): %s" % (SHORT_NAME, fmt(S)))
    print("多空清算中心 TP = (最低多头清算价 + 最高空头清算价)/2 = %.2f" % tp)
    if DIRECTION == "fade":
        print("规则: 跌破%s1开多/区2区3加仓, 涨破%s1开空/区2区3加仓, 到 TP 全平"
              % (LONG_NAME, SHORT_NAME))
    else:
        print("规则: 跌破%s1开空/区2区3加仓, 涨破%s1开多/区2区3加仓, 到 TP 全平"
              % (LONG_NAME, SHORT_NAME))
    print("=" * 68)

    sim = OUPrice(last_price, tp, vol=args.vol)
    pos = None  # {"side": +1/-1, "entries": [price...], "filled": [bool x3]}
    equity = args.equity
    peak = equity
    max_dd = 0.0
    trades = []
    log = []
    tick = 0

    def record(**kw):
        kw["tick"] = tick
        kw["time"] = datetime.now().isoformat(timespec="seconds")
        log.append(kw)

    def fill_zone(zone_list, label, i):
        """对当前 pos 记一笔开仓/加仓"""
        pos["entries"].append(price_now)
        pos["filled"][i] = True
        n = len(pos["entries"])
        avg = sum(pos["entries"]) / n
        verb = "开多" if (n == 1 and pos["side"] > 0) else \
               ("开空" if n == 1 else "加仓")
        print("[%6d] %s %.2f @ %9.2f  (突破%s%d %.1f)  持仓 %.2f 均价 %.2f"
              % (tick, verb, args.size, price_now, label, i + 1,
                 zone_list[i], n * args.size, avg))
        record(action=verb, side="多" if pos["side"] > 0 else "空",
               price=price_now, size=args.size,
               reason="突破%s%d %.1f" % (label, i + 1, zone_list[i]))

    try:
        while args.ticks == 0 or tick < args.ticks:
            tick += 1
            price_now = sim.next()

            if pos is None:
                for side, zone_list, label in (
                        (+1, long_zones, long_label),
                        (-1, short_zones, short_label)):
                    if crossed(side, zone_list[0], price_now):
                        pos = {"side": side, "entries": [], "filled": [False] * 3}
                        # 一步跨越多档时全部触发
                        for i, lv in enumerate(zone_list):
                            if crossed(side, lv, price_now):
                                fill_zone(zone_list, label, i)
                        break
            else:
                # 加仓: 检查未触发的 2/3 档
                zone_list = long_zones if pos["side"] > 0 else short_zones
                label = long_label if pos["side"] > 0 else short_label
                for i, lv in enumerate(zone_list):
                    if not pos["filled"][i] and crossed(pos["side"], lv, price_now):
                        fill_zone(zone_list, label, i)
                # 平仓: 到达多空清算中心
                if (price_now >= tp) if pos["side"] > 0 else (price_now <= tp):
                    n = len(pos["entries"]) * args.size
                    avg = sum(pos["entries"]) / len(pos["entries"])
                    pnl = (price_now - avg) * n * pos["side"]
                    equity += pnl
                    peak = max(peak, equity)
                    max_dd = max(max_dd, peak - equity)
                    side_txt = "多" if pos["side"] > 0 else "空"
                    trades.append({"side": side_txt, "size": n, "entry": avg,
                                   "exit": price_now, "pnl": pnl, "equity": equity})
                    print("[%6d] 平%s %.2f @ %9.2f  盈亏 %+8.2f  (到达多空清算中心 %.2f)  权益 %.2f"
                          % (tick, side_txt, n, price_now, pnl, tp, equity))
                    record(action="close", side=side_txt, size=n, avg_entry=avg,
                           price=price_now, pnl=pnl, equity=equity,
                           reason="到达多空清算中心 %.2f" % tp)
                    pos = None

            if tick % 200 == 0:
                if pos:
                    n = len(pos["entries"]) * args.size
                    avg = sum(pos["entries"]) / len(pos["entries"])
                    upnl = (price_now - avg) * n * pos["side"]
                    print("[%6d] ... 价格 %9.2f  持仓%s %.2f 浮动盈亏 %+8.2f  权益 %.2f"
                          % (tick, price_now, "多" if pos["side"] > 0 else "空",
                             n, upnl, equity))
                else:
                    print("[%6d] ... 价格 %9.2f  空仓  权益 %.2f"
                          % (tick, price_now, equity))

            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[i] 手动停止")

    open_info = None
    if pos:
        n = len(pos["entries"]) * args.size
        avg = sum(pos["entries"]) / len(pos["entries"])
        open_info = {"side": "多" if pos["side"] > 0 else "空", "size": n,
                     "avg_entry": avg, "last_price": sim.p,
                     "unrealized_pnl": (sim.p - avg) * n * pos["side"]}

    wins = sum(1 for t in trades if t["pnl"] > 0)
    total_pnl = sum(t["pnl"] for t in trades)
    print("\n" + "=" * 68)
    print("总结: 交易 %d 笔 | 胜 %d 负 %d | 胜率 %.1f%% | 总盈亏 %+.2f | 最终权益 %.2f"
          % (len(trades), wins, len(trades) - wins,
             100.0 * wins / len(trades) if trades else 0.0, total_pnl, equity))
    print("最大回撤(已实现权益): %.2f" % max_dd)
    if open_info:
        print("未平仓位: %s %.2f @ %.2f  浮动盈亏 %+.2f"
              % (open_info["side"], open_info["size"], open_info["avg_entry"],
                 open_info["unrealized_pnl"]))

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps({
        "strategy": "ETH 清算地图策略(模拟)",
        "direction": DIRECTION,
        "zones": {"last_price": last_price, "long_zones": L, "short_zones": S, "tp": tp},
        "config": vars(args),
        "trades": trades,
        "log": log,
        "summary": {"n_trades": len(trades), "wins": wins,
                    "win_rate": (100.0 * wins / len(trades)) if trades else 0.0,
                    "total_pnl": total_pnl, "final_equity": equity,
                    "max_drawdown": max_dd, "open_position": open_info},
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print("成交记录已保存: %s" % OUT_FILE)


if __name__ == "__main__":
    main()
