# -*- coding: utf-8 -*-
"""
ETH 预测系统 · 微观特征交换层引擎 (feature_cross.py)
===================================================
【阶段二核心架构规范】：
1. 严格限制在「特征交换/交叉层」，坚决不进入「决策层」；
2. 负责挖掘高维微观因子的非线性交互与协同效应：
   - 清算踩踏与逼空加速 (Liquidation Cascade Squeeze)
   - 隐蔽多空吸收与见顶/见底陷阱 (Institutional Absorption Trap)
   - BTC 跨品种外溢与剪刀差滞后冲击 (Lead-Lag Spillover)
   - 波动率蓄势挤压 (Volatility Compression & Expansion)
3. 仅向决策层暴露安全、有界、标准化的交叉特征项：
   - s_cross_momentum ∈ [-1.0, 1.0] (非线性量仓共振推进分)
   - s_cross_absorption ∈ [-1.0, 1.0] (非线性暗流吸收预警分)
   - gamma_elasticity ∈ [0.80, 1.35] (目标波幅弹性缩放系数)
4. 支持 LightGBM 极速 C++ 推断与纯 NumPy 优雅降级双模架构。
"""
import math
import os
from pathlib import Path
import numpy as np

try:
    import lightgbm as lgb
    HAS_LIGHTGBM = True
except ImportError:
    HAS_LIGHTGBM = False

from eth_predictor.indicators import safe_float

FEATURE_NAMES = [
    "oi_delta_5m_norm",       # 0: 5M 持仓增减量 (归一化)
    "oi_delta_1h_norm",       # 1: 1H 持仓增减量 (归一化)
    "cvd_score_5m",           # 2: 5M CVD 买卖量差评分 [-1, 1]
    "liq_net_bias",           # 3: 清算引力净偏向 [-1, 1]
    "vwap_z_score",           # 4: VWAP Z-Score 偏离度
    "top_ls_diff",            # 5: 大户多空比相对动态中位数的偏离
    "taker_ratio_log",        # 6: Taker 买卖比对数
    "funding_rate_scaled",    # 7: 资金费率缩放 (万分比)
    "vol_z_score",            # 8: 5M 成交量 Z-Score
    "wick_imbalance",         # 9: 影线失衡度 (下影线比率 - 上影线比率)
    "bb_pct_b",               # 10: 布林带 %B 位置 [0, 1]
    "btc_lead_spillover",     # 11: BTC 领先剪刀差外溢分
]


class FeatureCrossEngine:
    def __init__(self, model_dir=None):
        self.feature_names = FEATURE_NAMES
        self.num_features = len(FEATURE_NAMES)
        
        base_dir = Path(__file__).resolve().parent.parent
        self.model_dir = Path(model_dir) if model_dir else base_dir / "data" / "models"
        self.model_dir.mkdir(parents=True, exist_ok=True)
        
        self.model_momentum_path = self.model_dir / "gbm_cross_momentum.txt"
        self.model_absorption_path = self.model_dir / "gbm_cross_absorption.txt"
        
        self.booster_momentum = None
        self.booster_absorption = None
        self.is_ready = False
        
        self._init_models()

    def _init_models(self):
        """初始化 LightGBM 特征交叉模型（若模型文件不存在则依据微观领域先验生成校准模型）"""
        if not HAS_LIGHTGBM:
            print("[FeatureCrossEngine] LightGBM 未安装，已无缝启用纯 NumPy 备用推断模式", flush=True)
            self.is_ready = True
            return

        try:
            if self.model_momentum_path.exists() and self.model_absorption_path.exists():
                self.booster_momentum = lgb.Booster(model_file=str(self.model_momentum_path))
                self.booster_absorption = lgb.Booster(model_file=str(self.model_absorption_path))
            else:
                self._train_bootstrap_models()
            self.is_ready = True
        except Exception as e:
            print(f"[FeatureCrossEngine] 初始化 LightGBM 模型异常，回退至纯 NumPy 模式: {e}", flush=True)
            self.booster_momentum = None
            self.booster_absorption = None
            self.is_ready = True

    def _train_bootstrap_models(self):
        """
        构建并训练轻量特征交叉树模型 (Bootstrap Prior Calibration):
        基于衍生品微观结构（清算踩踏、暗流吸收、大户背离）的非线性物理规律生成校准样本，
        训练浅层决策树 (max_depth=3, n_estimators=40)，防止过拟合，专职输出非线性交互项。
        """
        np.random.seed(42)
        n_samples = 4000
        
        # 1. 模拟标准化特征空间
        x_oi_5m = np.random.uniform(-2.0, 2.0, n_samples)
        x_oi_1h = np.random.uniform(-2.0, 2.0, n_samples)
        x_cvd = np.random.uniform(-1.0, 1.0, n_samples)
        x_liq = np.random.uniform(-1.0, 1.0, n_samples)
        x_vwap_z = np.random.uniform(-2.5, 2.5, n_samples)
        x_top_ls = np.random.uniform(-1.5, 1.5, n_samples)
        x_taker = np.random.uniform(-1.5, 1.5, n_samples)
        x_fr = np.random.uniform(-1.5, 1.5, n_samples)
        x_vol_z = np.random.uniform(-2.0, 3.0, n_samples)
        x_wick = np.random.uniform(-1.0, 1.0, n_samples)
        x_pct_b = np.random.uniform(0.0, 1.0, n_samples)
        x_btc = np.random.uniform(-1.0, 1.0, n_samples)

        X = np.column_stack([
            x_oi_5m, x_oi_1h, x_cvd, x_liq, x_vwap_z, x_top_ls,
            x_taker, x_fr, x_vol_z, x_wick, x_pct_b, x_btc
        ])

        # 2. 目标 1：非线性动量协同度 (Momentum Squeeze Target)
        # 核心逻辑：清算池引力 × 增仓推进 × CVD 顺势买盘 × BTC先导 的非线性聚变效应
        # 只有在条件同时满足时才产生超额爆发加速度
        y_mom = (
            0.35 * x_liq * (x_oi_5m > 0.3) * (x_cvd > 0.2) +
            0.35 * x_liq * (x_oi_5m < -0.3) * (x_cvd < -0.2) +
            0.20 * x_btc * (x_vol_z > 0.5) +
            0.10 * x_taker
        )
        y_mom = np.clip(y_mom, -1.0, 1.0)

        # 3. 目标 2：非线性暗流吸收与见顶/见底陷阱 (Absorption Trap Target)
        # 核心逻辑：高位脉冲 + CVD相反流出 + 上影线受阻 (诱多见顶)
        # 或 低位下探 + CVD相反买入 + 下影线吸收 (诱空V转)
        y_abs = (
            # 底部吸收做多信号 (正向): 价格低 (pct_b<0.25), 下影线长 (wick>0.3), CVD 买盘暗中潜伏 (cvd>0.1)
            0.60 * (x_pct_b < 0.28) * (x_wick > 0.25) * (x_cvd > 0.1) +
            # 顶部出货诱多见顶信号 (负向): 价格高 (pct_b>0.72), 上影线长 (wick<-0.25), CVD 卖盘暗中砸 (cvd<-0.1)
            -0.60 * (x_pct_b > 0.72) * (x_wick < -0.25) * (x_cvd < -0.1) +
            0.25 * (-x_vwap_z) * (abs(x_vwap_z) > 1.4)
        )
        y_abs = np.clip(y_abs, -1.0, 1.0)

        train_data_mom = lgb.Dataset(X, label=y_mom, feature_name=FEATURE_NAMES)
        train_data_abs = lgb.Dataset(X, label=y_abs, feature_name=FEATURE_NAMES)

        params = {
            "objective": "regression",
            "metric": "l2",
            "learning_rate": 0.08,
            "max_depth": 3,               # 浅层决策树，严防过拟合
            "num_leaves": 8,
            "min_child_samples": 25,
            "verbosity": -1,
            "seed": 42
        }

        self.booster_momentum = lgb.train(params, train_data_mom, num_boost_round=45)
        self.booster_absorption = lgb.train(params, train_data_abs, num_boost_round=45)

        self.booster_momentum.save_model(str(self.model_momentum_path))
        self.booster_absorption.save_model(str(self.model_absorption_path))
        print("[FeatureCrossEngine] ✅ LightGBM 特征交换校准模型训练并持久化完成！", flush=True)

    def extract_feature_vector(self, snapshot_data):
        """
        从行情快照中提取并标准化 12 维微观物理特征向量
        """
        oi_5m = safe_float(snapshot_data.get("oi_delta_5m", 0.0))
        oi_1h = safe_float(snapshot_data.get("oi_delta_1h", 0.0))
        cvd_info = snapshot_data.get("cvd_5m") or {}
        cvd_score = safe_float(cvd_info.get("score", 0.0))
        liq_gravity = snapshot_data.get("liq_gravity") or {}
        liq_bias = safe_float(liq_gravity.get("net_score", 0.0))
        vwap_daily = snapshot_data.get("vwap_daily") or {}
        vwap_z = safe_float(vwap_daily.get("z_score", 0.0))

        pos_info = snapshot_data.get("pos_info") or {}
        top_ls = safe_float(pos_info.get("top_ls_ratio", 1.25))
        dyn_base = safe_float(snapshot_data.get("dyn_ls_baseline", 1.25))
        top_ls_diff = (top_ls - dyn_base) / 0.25 if dyn_base > 0 else 0.0

        taker_ratio = safe_float(snapshot_data.get("taker_ratio_5m", 1.0))
        taker_log = (math.log(taker_ratio) / math.log(1.5)) if taker_ratio > 0 else 0.0

        fr = safe_float(snapshot_data.get("funding_rate", 0.0001))
        fr_scaled = fr * 10000.0  # 放大为万分比

        vol_regime = snapshot_data.get("vol_regime_5m") or {}
        vol_z = safe_float(vol_regime.get("vol_z_score", 0.0))

        lower_wick = safe_float(snapshot_data.get("lower_wick_ratio", 0.0))
        upper_wick = safe_float(snapshot_data.get("upper_wick_ratio", 0.0))
        wick_imb = lower_wick - upper_wick

        bb_5m = snapshot_data.get("bb_5m") or {}
        pct_b = safe_float(bb_5m.get("pct_b", 0.5))

        btc_lead = snapshot_data.get("btc_lead") or {}
        btc_spill = safe_float(btc_lead.get("spillover_score", 0.0))

        # 裁剪边界保障数值稳定性
        x = np.array([
            max(-2.0, min(2.0, oi_5m / 3000.0)),
            max(-2.0, min(2.0, oi_1h / 10000.0)),
            max(-1.0, min(1.0, cvd_score)),
            max(-1.0, min(1.0, liq_bias)),
            max(-3.0, min(3.0, vwap_z)),
            max(-2.0, min(2.0, top_ls_diff)),
            max(-2.0, min(2.0, taker_log)),
            max(-2.0, min(2.0, fr_scaled)),
            max(-3.0, min(3.0, vol_z)),
            max(-1.0, min(1.0, wick_imb)),
            max(0.0, min(1.0, pct_b)),
            max(-1.0, min(1.0, btc_spill)),
        ], dtype=np.float32)

        return x

    def evaluate(self, snapshot_data):
        """
        特征交换层核心推断入口：
        输入微观快照，输出标准有界交互项与定性诊断，供白盒决策层调用。
        """
        x = self.extract_feature_vector(snapshot_data)
        
        # 1. 优先采用 LightGBM Booster 推断
        if self.booster_momentum and self.booster_absorption:
            try:
                x_input = x.reshape(1, -1)
                pred_mom = float(self.booster_momentum.predict(x_input)[0])
                pred_abs = float(self.booster_absorption.predict(x_input)[0])
            except Exception:
                pred_mom, pred_abs = self._evaluate_fallback_numpy(x)
        else:
            # 2. NumPy 纯数学推断备用
            pred_mom, pred_abs = self._evaluate_fallback_numpy(x)

        # 严格约束有界值域 [-1.0, 1.0]
        s_cross_momentum = round(max(-1.0, min(1.0, pred_mom)), 3)
        s_cross_absorption = round(max(-1.0, min(1.0, pred_abs)), 3)

        # 3. 目标波幅弹性系数 (gamma_elasticity ∈ [0.80, 1.35])
        vol_z = x[8]
        is_surge = (vol_z >= 1.5)
        is_dry = (vol_z <= -0.8)
        
        if is_surge and abs(s_cross_momentum) >= 0.25:
            gamma_elasticity = float(round(min(1.35, 1.0 + (float(vol_z) - 1.0) * 0.15), 2))
        elif is_dry or (abs(s_cross_momentum) < 0.1 and abs(s_cross_absorption) < 0.1):
            gamma_elasticity = float(round(max(0.80, 0.90 + float(vol_z) * 0.1), 2))
        else:
            gamma_elasticity = 1.0

        # 4. 微观交互定性范式归因
        regime_tag = "BALANCED"
        summary = "微观量仓动力学均衡，无显著非线性冲击"

        if s_cross_momentum >= 0.35:
            regime_tag = "CASCADE_SQUEEZE_LONG"
            summary = f"检测到清算引力与多头增仓买盘强共振 (协同度={s_cross_momentum:+.2f})，动能具爆发性"
        elif s_cross_momentum <= -0.35:
            regime_tag = "CASCADE_SQUEEZE_SHORT"
            summary = f"检测到清算引力与空头增仓砸盘强共振 (协同度={s_cross_momentum:+.2f})，加速下探"
        elif s_cross_absorption >= 0.30:
            regime_tag = "ABSORPTION_LONG_REVERSAL"
            summary = f"检测到底部诱空吸收形态 (吸收分={s_cross_absorption:+.2f})，空头抛压耗竭有反抽需求"
        elif s_cross_absorption <= -0.30:
            regime_tag = "ABSORPTION_SHORT_REVERSAL"
            summary = f"检测到顶部诱多出货陷阱 (吸收分={s_cross_absorption:+.2f})，买盘衰竭谨防高位反转"

        return {
            "s_cross_momentum": float(s_cross_momentum),
            "s_cross_absorption": float(s_cross_absorption),
            "gamma_elasticity": float(gamma_elasticity),
            "cross_regime_tag": regime_tag,
            "interaction_summary": summary,
            "feature_vector": [float(val) for val in x.tolist()]
        }

    def _evaluate_fallback_numpy(self, x):
        """纯 NumPy 高维非线性交叉备用算子 (保障 100% 高可用)"""
        # x: [0:oi_5m, 1:oi_1h, 2:cvd, 3:liq, 4:vwap_z, 5:top_ls, 6:taker, 7:fr, 8:vol_z, 9:wick, 10:pct_b, 11:btc]
        oi_5m, oi_1h, cvd, liq, vwap_z, top_ls, taker, fr, vol_z, wick, pct_b, btc = x

        # 动量协同：清算引力 × OI 增仓 × CVD
        mom_pos = (liq > 0.15) * (oi_5m > 0.2) * (cvd > 0.1) * (0.6 * liq + 0.4 * cvd)
        mom_neg = (liq < -0.15) * (oi_5m > 0.2) * (cvd < -0.1) * (0.6 * liq + 0.4 * cvd)
        btc_spill = 0.25 * btc * (vol_z > 0.5)
        mom = mom_pos + mom_neg + btc_spill

        # 暗流吸收：价值区极值 × 影线失衡 × CVD 背离
        abs_long = (pct_b < 0.28) * (wick > 0.20) * (cvd > -0.1) * 0.70
        abs_short = (pct_b > 0.72) * (wick < -0.20) * (cvd < 0.1) * -0.70
        vwap_rev = (-vwap_z * 0.2) if abs(vwap_z) > 1.5 else 0.0
        absorption = abs_long + abs_short + vwap_rev

        return float(mom), float(absorption)
