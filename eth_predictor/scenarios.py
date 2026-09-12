# -*- coding: utf-8 -*-
"""
ETH 预测系统 · 多情景走势概率树生成引擎 (scenarios.py)
============================================================
为每个时间周期 (5m / 1h / 1d) 构建三条互斥且完备的动态概率走势路径：
1. 主路径 (Primary Scenario): 顺势进攻 / 突破推进 (高概率)
2. 次路径 (Alternative Scenario): 假突破/诱盘/插针洗盘反抽 Sweep & Reverse (中概率)
3. 失效路径 (Invalidation Scenario): 结构破位与防守证伪 (低概率)
结合枢轴支撑阻力位、布林带轨线、清算池分布与市场范式自适应计算关键枢纽节点。
"""
from typing import Dict, Any, List


def generate_scenario_tree(
    tf: str,
    direction: str,
    base_price: float,
    tp1: float,
    tp2: float,
    sl: float,
    atr: float,
    market_regime: Dict[str, Any] = None,
    bb_bands: Dict[str, Any] = None,
    pivot_levels: Dict[str, Any] = None,
    liq_gravity: Dict[str, Any] = None,
    btc_lead_lag: Dict[str, Any] = None,
    confidence: float = 70.0
) -> Dict[str, Any]:
    """
    计算并生成结构化多情景概率树
    """
    regime_code = (market_regime or {}).get("regime", "RANGE_CONSOLIDATION")
    atr_val = max(1.0, atr)
    
    # 1. 动态概率配比 (根据市场范式与置信度自适应调整，三者和严格为 100%)
    if regime_code in ("BULL_TREND", "BEAR_TREND"):
        # 强单边趋势：主路径概率显著提升，洗盘概率压缩
        p_primary = min(78, max(60, int(round(confidence * 0.85))))
        p_inval = max(8, min(14, int(round((100 - p_primary) * 0.35))))
        p_alt = 100 - p_primary - p_inval
    elif regime_code == "EXPANSION_WHIPSAW":
        # 剧烈多空双杀拉锯：插针洗盘概率大幅上升
        p_primary = 52
        p_alt = 33
        p_inval = 15
    else:
        # 常规区间震荡 (RANGE_CONSOLIDATION)
        p_primary = min(68, max(55, int(round(confidence * 0.78))))
        p_alt = max(20, min(30, int(round((100 - p_primary) * 0.65))))
        p_inval = 100 - p_primary - p_alt

    # 2. 关键阻力与支撑枢纽节点定位
    pivots = pivot_levels or {}
    bb = bb_bands or {}

    if direction == "UP":
        # -------------------------------------------------------------
        # 主路径: 顺势多头拉升推进 (突破 -> 目标1 -> 冲刺目标2)
        # -------------------------------------------------------------
        breakout_p = round(max(base_price + 0.5, base_price + (tp1 - base_price) * 0.45), 2)
        r1_candidate = pivots.get("r1")
        if r1_candidate and base_price < r1_candidate < tp1:
            breakout_p = round(r1_candidate, 2)

        primary_nodes = [
            {"step": 1, "action": "START", "label": f"基准起点", "price": round(base_price, 2), "desc": "当前开仓基准"},
            {"step": 2, "action": "BREAKOUT", "label": f"突破确认位", "price": breakout_p, "desc": "突破站稳启动加速"},
            {"step": 3, "action": "TARGET_1", "label": f"目标 1 (TP1)", "price": round(tp1, 2), "desc": "第一阶段止盈"},
            {"step": 4, "action": "TARGET_2", "label": f"冲刺目标 2 (TP2)", "price": round(tp2, 2), "desc": "波段极限拓展"}
        ]
        primary_desc = f"买盘动能充沛，价格突破 {breakout_p:.2f} 后直取目标 1 ({tp1:.2f})，顺势波段冲刺 {tp2:.2f}。"

        # -------------------------------------------------------------
        # 次路径: 诱空插针洗盘后 V 转回抽 (Sweep & Reverse)
        # -------------------------------------------------------------
        # 插针深度：在基准价与止损线之间寻找布林下轨或近端支撑，诱扫多头止损但不打碎大结构
        sweep_candidate = bb.get("lb") or bb.get("lower")
        if sweep_candidate and sl < sweep_candidate < base_price:
            sweep_p = round(sweep_candidate, 2)
        else:
            sweep_p = round(max(sl + atr_val * 0.25, base_price - atr_val * 0.6), 2)

        alt_nodes = [
            {"step": 1, "action": "START", "label": f"基准起点", "price": round(base_price, 2), "desc": "当前开仓基准"},
            {"step": 2, "action": "SWEEP", "label": f"诱空插针低点", "price": sweep_p, "desc": "假摔扫荡多头止损池"},
            {"step": 3, "action": "RECOVERY", "label": f"V转收复线", "price": round(base_price, 2), "desc": "重回基准确认反抽"},
            {"step": 4, "action": "TARGET_1", "label": f"反抽目标 1", "price": round(tp1, 2), "desc": "洗盘后重拾升势"}
        ]
        alt_desc = f"主力先向下插针扫荡 {sweep_p:.2f} 密集止损池制造恐慌，随后迅速 V 转收复基准价并冲击目标 1 ({tp1:.2f})。"

        # -------------------------------------------------------------
        # 失效路径: 击穿防守线，多头结构证伪退出 (Invalidation)
        # -------------------------------------------------------------
        deep_p = round(sl - atr_val * 0.8, 2)
        s2_candidate = pivots.get("s2")
        if s2_candidate and s2_candidate < sl:
            deep_p = round(s2_candidate, 2)

        inval_nodes = [
            {"step": 1, "action": "START", "label": f"基准起点", "price": round(base_price, 2), "desc": "当前开仓基准"},
            {"step": 2, "action": "BREAK", "label": f"结构失效线 (SL)", "price": round(sl, 2), "desc": "多头防守破位"},
            {"step": 3, "action": "DEEP_DOWN", "label": f"破位延伸下探", "price": deep_p, "desc": "转入空头深调"}
        ]
        inval_desc = f"若价格有效失守 {sl:.2f} 结构防守线，多头逻辑证伪，系统立即触发偏离终止并进入观望。"

    elif direction == "NEUTRAL":
        # -------------------------------------------------------------
        # 震荡观望方向 (NEUTRAL): 拍卖市场箱体拉锯
        # -------------------------------------------------------------
        upper_bound = round(pivots.get("r1") or (bb.get("ub") or (base_price + atr_val * 0.8)), 2)
        lower_bound = round(pivots.get("s1") or (bb.get("lb") or (base_price - atr_val * 0.8)), 2)
        mid_p = round((upper_bound + lower_bound) / 2.0, 2)

        primary_nodes = [
            {"step": 1, "action": "START", "label": "基准起点", "price": round(base_price, 2), "desc": "区间中轴"},
            {"step": 2, "action": "RANGE_TEST", "label": "箱体上沿", "price": upper_bound, "desc": "阻力遇阻回落"},
            {"step": 3, "action": "RANGE_BOUNCE", "label": "箱体下沿", "price": lower_bound, "desc": "支撑吸筹回弹"},
            {"step": 4, "action": "EQUILIBRIUM", "label": "价值回归中轴", "price": mid_p, "desc": "均值均衡整固"}
        ]
        primary_desc = f"价格在 {lower_bound:.2f} ~ {upper_bound:.2f} 核心箱体内窄幅整固，量仓均衡，等待单边催化剂。"

        alt_nodes = [
            {"step": 1, "action": "START", "label": "基准起点", "price": round(base_price, 2), "desc": "区间中轴"},
            {"step": 2, "action": "UP_TEST", "label": "上探阻力", "price": upper_bound, "desc": "试探突破阻力"},
            {"step": 3, "action": "REJECT", "label": "承压回落", "price": mid_p, "desc": "承压重回区间"}
        ]
        alt_desc = f"价格试探箱体上沿 {upper_bound:.2f}，遇阻后重回中轴 {mid_p:.2f} 震荡。"

        inval_nodes = [
            {"step": 1, "action": "START", "label": "基准起点", "price": round(base_price, 2), "desc": "区间中轴"},
            {"step": 2, "action": "BREAK", "label": "单边破位", "price": round(lower_bound - atr_val * 0.5, 2), "desc": "破位选择单边"}
        ]
        inval_desc = f"价格放量击穿 {lower_bound:.2f} 箱体边界，震荡格局打破，转入新单边趋势。"

        return {
            "primary": {
                "name": "主路径 (区间收敛震荡)",
                "type": "RANGE_OSCILLATION",
                "probability": 60,
                "direction": "NEUTRAL",
                "nodes": primary_nodes,
                "description": primary_desc,
                "condition": "多空持仓与买卖盘均衡，缺乏增量资金"
            },
            "alternative": {
                "name": "次路径 (试探边界承压)",
                "type": "BOUNDARY_TEST",
                "probability": 25,
                "direction": "NEUTRAL",
                "nodes": alt_nodes,
                "description": alt_desc,
                "condition": "微量冲高受阻回落"
            },
            "invalidation": {
                "name": "失效路径 (单边放量破位)",
                "type": "BREAKOUT_EXPANSION",
                "probability": 15,
                "direction": "BREAKOUT",
                "nodes": inval_nodes,
                "description": inval_desc,
                "condition": "突发爆量打破箱体平衡"
            }
        }

    else:
        # -------------------------------------------------------------
        # 空头方向 (DOWN)
        # -------------------------------------------------------------
        # 主路径: 顺势下砸推进
        breakout_p = round(min(base_price - 0.5, base_price - (base_price - tp1) * 0.45), 2)
        s1_candidate = pivots.get("s1")
        if s1_candidate and tp1 < s1_candidate < base_price:
            breakout_p = round(s1_candidate, 2)

        primary_nodes = [
            {"step": 1, "action": "START", "label": f"基准起点", "price": round(base_price, 2), "desc": "当前开仓基准"},
            {"step": 2, "action": "BREAKOUT", "label": f"破位加速位", "price": breakout_p, "desc": "跌破支撑引发踩踏"},
            {"step": 3, "action": "TARGET_1", "label": f"目标 1 (TP1)", "price": round(tp1, 2), "desc": "第一阶段止盈"},
            {"step": 4, "action": "TARGET_2", "label": f"冲刺目标 2 (TP2)", "price": round(tp2, 2), "desc": "波段极限拓展"}
        ]
        primary_desc = f"空头抛压主导，价格击穿 {breakout_p:.2f} 后直取目标 1 ({tp1:.2f})，顺势波段下探 {tp2:.2f}。"

        # 次路径: 诱多冲高插针后拐头下杀 (Sweep & Reverse)
        sweep_candidate = bb.get("ub") or bb.get("upper")
        if sweep_candidate and base_price < sweep_candidate < sl:
            sweep_p = round(sweep_candidate, 2)
        else:
            sweep_p = round(min(sl - atr_val * 0.25, base_price + atr_val * 0.6), 2)

        alt_nodes = [
            {"step": 1, "action": "START", "label": f"基准起点", "price": round(base_price, 2), "desc": "当前开仓基准"},
            {"step": 2, "action": "SWEEP", "label": f"诱多冲高插针", "price": sweep_p, "desc": "假突破扫荡空头止损"},
            {"step": 3, "action": "RECOVERY", "label": f"倒V回落确认", "price": round(base_price, 2), "desc": "重回基准确认诱多"},
            {"step": 4, "action": "TARGET_1", "label": f"下探目标 1", "price": round(tp1, 2), "desc": "诱多后放量砸盘"}
        ]
        alt_desc = f"主力先向上冲高扫荡 {sweep_p:.2f} 密集空头止损池诱多，随后倒 V 拐头回落并击穿目标 1 ({tp1:.2f})。"

        # 失效路径: 升破防守线，空头结构证伪
        deep_p = round(sl + atr_val * 0.8, 2)
        r2_candidate = pivots.get("r2")
        if r2_candidate and r2_candidate > sl:
            deep_p = round(r2_candidate, 2)

        inval_nodes = [
            {"step": 1, "action": "START", "label": f"基准起点", "price": round(base_price, 2), "desc": "当前开仓基准"},
            {"step": 2, "action": "BREAK", "label": f"结构失效线 (SL)", "price": round(sl, 2), "desc": "空头防守破位"},
            {"step": 3, "action": "DEEP_UP", "label": f"破位延伸逼空", "price": deep_p, "desc": "转入多头强势逼空"}
        ]
        inval_desc = f"若价格向上冲破 {sl:.2f} 结构防守线，空头逻辑证伪，系统立即触发偏离终止并进入观望。"

    return {
        "primary": {
            "name": "主路径 (顺势突破推进)",
            "type": "PRIMARY_EXPANSION",
            "probability": p_primary,
            "direction": direction,
            "nodes": primary_nodes,
            "description": primary_desc,
            "condition": "CVD 主动买卖盘持续同向流入，量价共振良好"
        },
        "alternative": {
            "name": "次路径 (插针洗盘反抽)",
            "type": "SWEEP_AND_REVERSE",
            "probability": p_alt,
            "direction": direction,
            "nodes": alt_nodes,
            "description": alt_desc,
            "condition": "流动性密集池诱扫散户止损后迅速缩量回抽"
        },
        "invalidation": {
            "name": "失效路径 (结构破位证伪)",
            "type": "STRUCTURE_INVALIDATION",
            "probability": p_inval,
            "direction": "DOWN" if direction == "UP" else "UP",
            "nodes": inval_nodes,
            "description": inval_desc,
            "condition": f"触及结构线 {sl:.2f}，量仓严重反向恶化"
        }
    }
