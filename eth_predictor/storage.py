# -*- coding: utf-8 -*-
"""
ETH 预测系统 · 数据存储与持久化管理 (storage.py)
==============================================
负责预测流水、检验结果、复盘记录、模型超参数以及 90% 达标收敛状态的落盘。
"""
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

TZ_BJT = timezone(timedelta(hours=8))
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "predictions"
HISTORY_FILE = DATA_DIR / "prediction_history.json"
CONFIG_FILE = DATA_DIR / "optimizer_state.json"
REVIEW_MD_FILE = BASE_DIR / "PREDICTION_REVIEW_LOG.md"


class PredictionStorage:
    def __init__(self, data_dir=DATA_DIR):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.history_file = HISTORY_FILE
        self.config_file = CONFIG_FILE
        self.review_md_file = REVIEW_MD_FILE
        self.lock = threading.Lock()
        self._init_files()

    def _atomic_write_json(self, target_file, data, max_keep=None):
        """原子级文件写入：先写临时文件再原子 replace，杜绝并发竞争损坏文件"""
        if max_keep and isinstance(data, list) and len(data) > max_keep:
            data = data[-max_keep:]
        tmp_file = target_file.with_suffix(f".tmp.{os.getpid()}.{int(time.time()*1000)}")
        try:
            tmp_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp_file.replace(target_file)
        except Exception as e:
            if tmp_file.exists():
                try: tmp_file.unlink()
                except Exception: pass
            raise e

    def _init_files(self):
        """确保基础文件与日志头部就绪"""
        if not self.history_file.exists():
            self.history_file.write_text("[]", encoding="utf-8")

        if not self.config_file.exists():
            from eth_predictor.models import DEFAULT_TIMEFRAME_WEIGHTS
            initial_state = {
                "generation": 0,
                "status": "OPTIMIZING",            # OPTIMIZING | OPTIMAL_CONVERGED
                "target_accuracy": 90.0,
                "current_accuracy": 0.0,
                "accuracy_5m": 0.0,
                "accuracy_1h": 0.0,
                "accuracy_1d": 0.0,
                "is_converged": False,
                "last_review_ts": int(time.time()),
                "last_review_iso": datetime.now(TZ_BJT).strftime("%Y-%m-%d %H:%M:%S"),
                "weights": DEFAULT_TIMEFRAME_WEIGHTS,
                "optimization_history": []
            }
            self.config_file.write_text(json.dumps(initial_state, indent=2, ensure_ascii=False), encoding="utf-8")

        if not self.review_md_file.exists():
            header = """# ETH 涨跌与目标位置预测 · 12小时持续复盘与自优化日志

> 本系统通过 **VWAP + OI + 持仓量/多空比 + 清算地图 + CVD/RSI/ATR** 复合指标，持续预测未来 5分钟、1小时、1日涨跌与目标位置。
> 每隔 12 小时执行深度复盘与自主意愿优化，**目标综合准确率达到 90.0% 时停止优化，锁定最优参数**。

---
"""
            self.review_md_file.write_text(header, encoding="utf-8")

    def load_optimizer_state(self):
        with self.lock:
            try:
                return json.loads(self.config_file.read_text(encoding="utf-8"))
            except Exception:
                return {}

    def save_optimizer_state(self, state):
        """保存优化器状态"""
        with self.lock:
            try:
                self._atomic_write_json(self.config_file, state)
            except Exception as e:
                print(f"[!] 保存优化器状态失败: {e}", flush=True)

    def load_history(self, limit=500):
        """读取预测历史记录"""
        with self.lock:
            try:
                data = json.loads(self.history_file.read_text(encoding="utf-8"))
                if limit:
                    return data[-limit:]
                return data
            except Exception:
                return []

    def save_history(self, history, max_keep=2000):
        """保存预测流水 (原子化写入)"""
        with self.lock:
            try:
                self._atomic_write_json(self.history_file, history, max_keep=max_keep)
            except Exception as e:
                print(f"[!] 保存预测历史失败: {e}", flush=True)

    def record_prediction(self, pred_record):
        """记录一条新生成的预测"""
        with self.lock:
            try:
                data = json.loads(self.history_file.read_text(encoding="utf-8"))
            except Exception:
                data = []
            data.append(pred_record)
            self._atomic_write_json(self.history_file, data, max_keep=2000)

    def sync_active_prediction(self, act_obj):
        """实时同步进行中预测的最新状态到历史数据库中，确保二级页面即时同频更新"""
        if not act_obj or not act_obj.get("pred_id"):
            return
        pred_id = act_obj["pred_id"]
        with self.lock:
            try:
                data = json.loads(self.history_file.read_text(encoding="utf-8"))
            except Exception:
                data = []
            updated = False
            for p in data:
                if p.get("pred_id") == pred_id and p.get("status") == "ACTIVE":
                    for k in ["stage", "stage_step", "stage_label", "tp1_status", "tp2_status",
                              "tp1_hit_ts", "tp1_hit_price", "secondary_eval", "highest_seen",
                              "lowest_seen", "exit_info", "sl_breached", "timeout_ts", "timeout_iso", "timeout_candles"]:
                        if k in act_obj:
                            p[k] = act_obj[k]
                    updated = True
                    break
            if updated:
                self._atomic_write_json(self.history_file, data, max_keep=2000)

    def update_prediction(self, pred_id, verified_result, act_obj=None):
        """更新已到期或已终结预测的检验结果与全程轨迹，保证历史档案完整"""
        with self.lock:
            try:
                data = json.loads(self.history_file.read_text(encoding="utf-8"))
            except Exception:
                data = []
            updated = False
            for p in data:
                if p.get("pred_id") == pred_id:
                    p["status"] = "VERIFIED"
                    p["verified_result"] = verified_result
                    if act_obj:
                        for k in ["stage", "stage_step", "stage_label", "tp1_status", "tp2_status",
                                  "tp1_hit_ts", "tp1_hit_price", "secondary_eval", "highest_seen",
                                  "lowest_seen", "exit_info", "sl_breached", "timeout_ts", "timeout_iso", "timeout_candles"]:
                            if k in act_obj:
                                p[k] = act_obj[k]
                    updated = True
                    break
            if updated:
                self._atomic_write_json(self.history_file, data, max_keep=2000)

    def reset_history(self):
        """重置历史预测数据库并自动生成备份"""
        with self.lock:
            try:
                old_data = []
                if self.history_file.exists():
                    try:
                        old_data = json.loads(self.history_file.read_text(encoding="utf-8"))
                    except Exception:
                        pass
                if old_data:
                    backup_file = self.data_dir / f"prediction_history_backup_{int(time.time())}.json"
                    backup_file.write_text(json.dumps(old_data, indent=2, ensure_ascii=False), encoding="utf-8")
                    print(f"[*] 已备份旧历史数据至 {backup_file.name}", flush=True)
                self._atomic_write_json(self.history_file, [])
                return True
            except Exception as e:
                print(f"[!] 重置预测历史异常: {e}", flush=True)
                return False

    def append_review_markdown(self, markdown_text):
        """追加复盘优化报告至 Markdown 日志"""
        try:
            with open(self.review_md_file, "a", encoding="utf-8") as f:
                f.write(markdown_text)
        except Exception as e:
            print(f"[!] 写入复盘 Markdown 日志失败: {e}", flush=True)

