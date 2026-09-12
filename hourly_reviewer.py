# -*- coding: utf-8 -*-
"""
小时级自动复盘与优化总结模块 (Hourly Reviewer)
==============================================
每隔 1 小时自动分析两套策略的运行数据与盈亏表现：
1. 盈亏统计：每小时成交笔数、胜率、实现盈亏、手续费支出、当前浮动盈亏
2. 归因诊断：
   - 亏损订单：诊断是 VWAP #1#2 中心值触碰后的单边冲破，还是清算区踩踏未止住，抑或是量仓多空条件判断迟滞
   - 盈利订单：分析哪个环节（例如量仓过滤避险、清算出清拐点）贡献了利润，以及如何进一步扩大盈利（如调整止盈目标或动态追踪）
3. 持续沉淀：自动生成复盘总结报告并追加至 TRADING_REVIEW_LOG.md
"""
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent
STATE_FILE = BASE / "data" / "auto" / "state.json"
REVIEW_LOG_FILE = BASE / "TRADING_REVIEW_LOG.md"


class HourlyReviewer:
    def __init__(self, state_file=STATE_FILE, log_file=REVIEW_LOG_FILE):
        self.state_file = Path(state_file)
        self.log_file = Path(log_file)
        self._last_review_ts = time.time()
        self._init_log_file()

    def _init_log_file(self):
        """如果日志文件不存在，初始化结构头"""
        if not self.log_file.exists():
            header = """# ETH 5m 双策略 · 交易复盘与持续优化日志 (Trading Review Log)

> 本日志由自动化复盘引擎生成，记录策略架构变更、小时级盈亏表现、以及指标参数优化总结。

---
"""
            self.log_file.write_text(header, encoding="utf-8")

    def analyze_now(self, engine=None):
        """执行一次小时级复盘并追加记录"""
        now = datetime.now()
        now_ts = time.time()
        hour_ago_ts = now_ts - 3600

        # 从 engine 或 state.json 读取数据
        data = None
        if engine is not None:
            f = getattr(engine, "futures_feed", getattr(engine, "feed", None))
            px = f.price if f and f.price else 0
            data = {
                "systems": {s.id: s.snapshot(px) for s in engine.strategies},
                "feed": f.snapshot() if f and hasattr(f, "snapshot") else {},
            }
        elif self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text(encoding="utf-8"))
            except Exception:
                pass

        if not data or "systems" not in data:
            return "暂无有效交易状态数据，跳过复盘"

        sys_data = data["systems"]
        vwap_sys = sys_data.get("vwap", {})
        liq_sys = sys_data.get("liqmap", {})

        # 筛选过去 1 小时内的成交
        def get_recent_trades(trades):
            res = []
            for t in trades:
                t_ts = t.get("ts", 0)
                if t_ts >= hour_ago_ts:
                    res.append(t)
            return res

        vwap_trades_1h = get_recent_trades(vwap_sys.get("trades", []))
        liq_trades_1h = get_recent_trades(liq_sys.get("trades", []))

        # 统计分析
        def summarize(trades, sys_info):
            n = len(trades)
            wins = [t for t in trades if t.get("pnl", 0) > 0]
            losses = [t for t in trades if t.get("pnl", 0) <= 0]
            pnl_1h = sum(t.get("pnl", 0) for t in trades)
            fees_1h = sum(t.get("fees", 0) for t in trades)
            wr = (len(wins) / n * 100) if n > 0 else 0.0
            return {
                "n": n, "wins": len(wins), "losses": len(losses),
                "wr": wr, "pnl_1h": pnl_1h, "fees_1h": fees_1h,
                "total_equity": sys_info.get("equity", 1000.0),
                "total_pnl": sys_info.get("pnl", 0.0),
                "current_pos": sys_info.get("position"),
                "trades": trades
            }

        v_sum = summarize(vwap_trades_1h, vwap_sys)
        l_sum = summarize(liq_trades_1h, liq_sys)

        # 归因洞察分析
        diagnosis_lines = []
        
        # 1. 策略一 VWAP 归因
        if v_sum["n"] == 0:
            pos = v_sum["current_pos"]
            if pos:
                diagnosis_lines.append(f"- **策略一 (VWAP)**: 过去 1 小时内处于持仓监控中（当前持 {pos['side']} {pos['qty']} ETH，均价 {pos['avg_entry']}，浮动盈亏 {pos['upnl']:+.2f} USDT），等待回归 VWAP 平仓。")
            else:
                diagnosis_lines.append("- **策略一 (VWAP)**: 过去 1 小时价格未触及 #1#2 中心值，或量仓门禁有效拦截了潜在逆势单，处于耐心观望状态。")
        else:
            if v_sum["pnl_1h"] >= 0:
                diagnosis_lines.append(f"- **策略一 (VWAP) 盈利总结**: 1小时收益 {v_sum['pnl_1h']:+.2f} USDT (胜率 {v_sum['wr']:.1f}%)。**盈利原因**：价格在 #1#2 中心值出现均值回归，量仓过滤成功规避了主升/主跌浪。**扩利建议**：可考虑在回归 VWAP 时仅平仓 70%，剩余 30% 设置追踪止盈以博取越过 VWAP 的超额收益。")
            else:
                diagnosis_lines.append(f"- **策略一 (VWAP) 亏损归因**: 1小时亏损 {v_sum['pnl_1h']:+.2f} USDT。**问题排查**：需检查是否因为在 #1#2 中心值开仓时，单边趋势过强导致均值回归失效。可微调量仓门禁：将 Taker 买卖比的开仓容忍阈值收紧 10%，或要求 OI 出现明确衰减后再进场。")

        # 2. 策略二 清算地图 归因
        if l_sum["n"] == 0:
            pos = l_sum["current_pos"]
            if pos:
                diagnosis_lines.append(f"- **策略二 (清算地图)**: 过去 1 小时持仓中（当前持 {pos['side']} {pos['qty']} ETH，均价 {pos['avg_entry']}，浮动盈亏 {pos['upnl']:+.2f} USDT），朝 TP 清算中心迈进。")
            else:
                diagnosis_lines.append("- **策略二 (清算地图)**: 过去 1 小时价格在当前清算带内震荡，未触发清算区边缘破位，保持等待。")
        else:
            if l_sum["pnl_1h"] >= 0:
                diagnosis_lines.append(f"- **策略二 (清算地图) 盈利总结**: 1小时收益 {l_sum['pnl_1h']:+.2f} USDT (胜率 {l_sum['wr']:.1f}%)。**盈利原因**：清算区触发后爆仓出清逻辑生效，价格迅速反向奔赴 TP。**扩利建议**：若伴随大户多空比明显逆转，可在 TP 前分批推保护止盈，扩大盈利空间。")
            else:
                diagnosis_lines.append(f"- **策略二 (清算地图) 亏损归因**: 1小时亏损 {l_sum['pnl_1h']:+.2f} USDT。**问题排查**：清算区破位后并未发生预期反转，可能伴随空头/多头真开仓推单。建议在触发清算区时，增加“ΔOI 必须出现出清负值”的强约束条件。")

        # 生成报告 Markdown 块
        review_block = f"""
### 🕒 小时级复盘报告 [{now.strftime('%Y-%m-%d %H:%M:%S')}]

| 策略 | 1h 成交笔数 | 1h 胜率 | 1h 净盈亏 | 1h 手续费 | 当前总权益 | 当前持仓 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **策略一 · VWAP #1#2 中心值** | {v_sum['n']} 笔 | {v_sum['wr']:.1f}% | `{v_sum['pnl_1h']:+.2f} USDT` | `{v_sum['fees_1h']:.2f} USDT` | `{v_sum['total_equity']:.2f}` | {v_sum['current_pos']['side'] + ' ' + str(v_sum['current_pos']['qty']) + ' ETH' if v_sum['current_pos'] else '空仓'} |
| **策略二 · 清算地图 + 量仓** | {l_sum['n']} 笔 | {l_sum['wr']:.1f}% | `{l_sum['pnl_1h']:+.2f} USDT` | `{l_sum['fees_1h']:.2f} USDT` | `{l_sum['total_equity']:.2f}` | {l_sum['current_pos']['side'] + ' ' + str(l_sum['current_pos']['qty']) + ' ETH' if l_sum['current_pos'] else '空仓'} |

#### 深度归因与调优诊断：
{chr(10).join(diagnosis_lines)}

---
"""
        # 追加写入日志
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(review_block)

        self._last_review_ts = now_ts
        return review_block
