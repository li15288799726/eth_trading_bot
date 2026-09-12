# -*- coding: utf-8 -*-
"""
模拟交易系统 1 —— VWAP #3 带均值回归(纸面模拟, 不接交易所, 模拟价格)
======================================================================
数据源(文件夹10):
  - data/legend/vwap_legend_values.json  实时图例数值(vwap_read.py 每 5 秒刷新,
    本系统每步热加载, 带数值更新后自动跟随)
  - data/legend/vwap_bands_m5.json       M5 历史带数据(图例文件缺失时回退用最后一根)

策略规则:
  1) 价格上穿 #3 上轨 band3_upper (+3σ) -> 开空 1 单位(均值回归)
  2) 价格下穿 #3 下轨 band3_lower (-3σ) -> 开多 1 单位
  3) 持仓期间价格回到 VWAP 主线 -> 平仓
  (无止损, 均值回归假设价格终将回归 VWAP)

价格引擎: OU(Ornstein-Uhlenbeck)随机游走, 锚点跟随 VWAP,
模拟"围绕 VWAP 波动、偶尔冲到 ±3σ 后回归"的价格路径。

用法:
  python sim_vwap.py                    # 无限循环, Ctrl+C 结束并打印总结
  python sim_vwap.py --ticks 5000       # 固定步数
  python sim_vwap.py --interval 0      # 不休眠(快速回放)
成交记录: data/sim/vwap_trades.json
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
LEGEND_FILE = BASE / "data" / "legend" / "vwap_legend_values.json"
M5_FILE = BASE / "data" / "legend" / "vwap_bands_m5.json"
OUT_FILE = BASE / "data" / "sim" / "vwap_trades.json"

FIELDS = ("vwap", "band1_upper", "band1_lower", "band2_upper",
          "band2_lower", "band3_upper", "band3_lower")


def _pick(d):
    return {k: float(d[k]) for k in FIELDS if k in d}


def load_bands():
    """读取 VWAP 带数据: 优先实时图例文件, 失败回退 M5 序列最后一根"""
    try:
        d = json.loads(LEGEND_FILE.read_text(encoding="utf-8"))
        if d.get("ok") and "vwap" in d:
            return _pick(d), "实时图例 vwap_legend_values.json"
    except Exception:
        pass
    try:
        rows = json.loads(M5_FILE.read_text(encoding="utf-8"))
        if rows:
            return _pick(rows[-1]), "M5 历史 %s" % rows[-1].get("time_str", "")
    except Exception:
        pass
    return None, None


class OUPrice:
    """OU 随机游走: 围绕锚点波动, 兼具均值回归拉力与随机冲击"""

    def __init__(self, start, anchor, vol=0.0005, theta=0.005):
        self.p = float(start)
        self.anchor = float(anchor)
        self.vol = vol        # 每步波动率(相对价格)
        self.theta = theta    # 均值回归速度

    def next(self):
        shock = random.gauss(0.0, self.vol * self.p)
        self.p += self.theta * (self.anchor - self.p) + shock
        return self.p


def main():
    ap = argparse.ArgumentParser(description="VWAP #3 均值回归模拟盘")
    ap.add_argument("--ticks", type=int, default=0, help="模拟步数, 0=无限(Ctrl+C 结束)")
    ap.add_argument("--interval", type=float, default=0.3, help="每步间隔秒")
    ap.add_argument("--equity", type=float, default=10000.0, help="初始权益(USDT)")
    ap.add_argument("--size", type=float, default=1.0, help="开仓数量(ETH)")
    ap.add_argument("--vol", type=float, default=0.0005, help="价格波动率(每步, 相对值)")
    ap.add_argument("--seed", type=int, default=None, help="随机种子(复现用)")
    args = ap.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    bands, src = load_bands()
    if not bands:
        print("[x] 找不到 VWAP 数据: data/legend/vwap_legend_values.json 或 vwap_bands_m5.json")
        sys.exit(1)
    if not (bands["band3_lower"] < bands["vwap"] < bands["band3_upper"]):
        print("[x] 带数据异常: #3 下轨 %.2f / VWAP %.2f / #3 上轨 %.2f"
              % (bands["band3_lower"], bands["vwap"], bands["band3_upper"]))
        sys.exit(1)

    print("=" * 68)
    print("模拟交易系统 1 —— VWAP #3 带均值回归(纸面模拟)")
    print("数据源: %s" % src)
    print("VWAP=%.2f  #3上轨=%.2f  #3下轨=%.2f  (带宽 ±%.2f)"
          % (bands["vwap"], bands["band3_upper"], bands["band3_lower"],
             (bands["band3_upper"] - bands["vwap"])))
    print("规则: 上穿#3上轨开空 / 下穿#3下轨开多 / 回到VWAP平仓")
    print("=" * 68)

    sim = OUPrice(bands["vwap"], bands["vwap"], vol=args.vol)
    pos = None            # {"side": +1多/-1空, "entries": [price, ...]}
    equity = args.equity
    peak = equity
    max_dd = 0.0
    trades = []
    log = []
    tick = 0
    last_mtime = None

    def record(**kw):
        kw["tick"] = tick
        kw["time"] = datetime.now().isoformat(timespec="seconds")
        log.append(kw)

    try:
        while args.ticks == 0 or tick < args.ticks:
            tick += 1

            # 数据热加载: 图例文件被 vwap_read.py 刷新后自动跟随
            try:
                m = LEGEND_FILE.stat().st_mtime
            except OSError:
                m = None
            if m != last_mtime:
                nb, _ = load_bands()
                if nb and (nb["vwap"] != bands["vwap"]
                           or nb["band3_upper"] != bands["band3_upper"]):
                    print("[i] 带数据更新: VWAP=%.2f #3上轨=%.2f #3下轨=%.2f"
                          % (nb["vwap"], nb["band3_upper"], nb["band3_lower"]))
                    bands = nb
                last_mtime = m

            sim.anchor = bands["vwap"]
            price = sim.next()

            if pos is None:
                if price >= bands["band3_upper"]:
                    pos = {"side": -1, "entries": [price]}
                    print("[%6d] 开空 %.2f @ %9.2f  (上穿#3上轨 %.2f)"
                          % (tick, args.size, price, bands["band3_upper"]))
                    record(action="open_short", price=price, size=args.size,
                           reason="上穿#3上轨 %.2f" % bands["band3_upper"])
                elif price <= bands["band3_lower"]:
                    pos = {"side": +1, "entries": [price]}
                    print("[%6d] 开多 %.2f @ %9.2f  (下穿#3下轨 %.2f)"
                          % (tick, args.size, price, bands["band3_lower"]))
                    record(action="open_long", price=price, size=args.size,
                           reason="下穿#3下轨 %.2f" % bands["band3_lower"])
            else:
                vwap = bands["vwap"]
                hit_vwap = (pos["side"] < 0 and price <= vwap) or \
                           (pos["side"] > 0 and price >= vwap)
                if hit_vwap:
                    n = len(pos["entries"]) * args.size
                    avg = sum(pos["entries"]) / len(pos["entries"])
                    pnl = (price - avg) * n * pos["side"]
                    equity += pnl
                    peak = max(peak, equity)
                    max_dd = max(max_dd, peak - equity)
                    side_txt = "空" if pos["side"] < 0 else "多"
                    trades.append({"side": side_txt, "size": n, "entry": avg,
                                   "exit": price, "pnl": pnl, "equity": equity})
                    print("[%6d] 平%s %.2f @ %9.2f  盈亏 %+8.2f  (回到VWAP %.2f)  权益 %.2f"
                          % (tick, side_txt, n, price, pnl, vwap, equity))
                    record(action="close", side=side_txt, size=n, avg_entry=avg,
                           price=price, pnl=pnl, equity=equity, reason="回到VWAP")
                    pos = None

            if tick % 200 == 0:
                if pos:
                    n = len(pos["entries"]) * args.size
                    avg = sum(pos["entries"]) / len(pos["entries"])
                    upnl = (price - avg) * n * pos["side"]
                    print("[%6d] ... 价格 %9.2f  持仓%s 浮动盈亏 %+8.2f  权益 %.2f"
                          % (tick, price, "空" if pos["side"] < 0 else "多", upnl, equity))
                else:
                    print("[%6d] ... 价格 %9.2f  空仓  权益 %.2f"
                          % (tick, price, equity))

            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[i] 手动停止")

    # 未平仓位按最后价格浮亏展示(不强制结算)
    open_info = None
    if pos:
        n = len(pos["entries"]) * args.size
        avg = sum(pos["entries"]) / len(pos["entries"])
        open_info = {"side": "空" if pos["side"] < 0 else "多", "size": n,
                     "avg_entry": avg, "last_price": sim.p,
                     "unrealized_pnl": (sim.p - avg) * n * pos["side"]}

    wins = sum(1 for t in trades if t["pnl"] > 0)
    total_pnl = sum(t["pnl"] for t in trades)
    print("\n" + "=" * 68)
    print("总结: 交易 %d 笔 | 胜 %d 负 %d | 胜率 %.1f%% | 总盈亏 %+.2f | 最终权益 %.2f"
          % (len(trades), wins, len(trades) - wins,
             100.0 * wins / len(trades) if trades else 0.0,
             total_pnl, equity))
    print("最大回撤(已实现权益): %.2f" % max_dd)
    if open_info:
        print("未平仓位: %s %.2f @ %.2f  浮动盈亏 %+.2f"
              % (open_info["side"], open_info["size"], open_info["avg_entry"],
                 open_info["unrealized_pnl"]))

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps({
        "strategy": "VWAP #3 均值回归(模拟)",
        "started bands": bands,
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
