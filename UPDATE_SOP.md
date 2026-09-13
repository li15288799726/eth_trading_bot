# ETH 预测程序 · 代码更新与沟通归档标准作业流程 (SOP)

> **生效日期**：2026-09-13
> **用户核心指示**：“再之后的每次更新代码时，保存咱俩的沟通记录，和程序代码。”

---

## 📌 核心执行原则（三位一体更新机制）

从 2026-09-13 起，对本预测系统的任何代码更新、参数调优或架构升级，必须严格遵循以下三位一体流程：

### 1. 沟通记录同步追加 (Sync Communication Archive)
- 在每次讨论产生决策或代码更新前后，将该轮对话中的：
  - **用户核心需求与指示**
  - **实盘问题与数据诊断**
  - **双方讨论共识与选定方案**
  - **具体改动点与代码影响**
- 同步追加至 [`ETH_PREDICTION_COMMUNICATION_ARCHIVE.md`](file:///home/ubuntu/eth_trading_bot/ETH_PREDICTION_COMMUNICATION_ARCHIVE.md) 的对应新章节中。

### 2. 代码更新与严格测试 (Code Implementation & Test)
- 修改核心逻辑（如 `eth_predictor/core.py`, `binance_futures_feed.py`, `auto_trading.py` 等）。
- 运行针对性单元测试或微观数据回放，确保无语法错误、无时序泄漏、无端口死锁。

### 3. Git 双重提交与版本固化 (Git Commit & Versioning)
- 将**更新后的程序代码**与**更新后的沟通纪要文档**一同提交至 Git 版本库：
  ```bash
  git add <修改的代码文件> ETH_PREDICTION_COMMUNICATION_ARCHIVE.md
  git commit -m "<规范前缀>: <改动概述> (同步归档沟通记录)"
  ```
- 确保每一次代码提交，都有精确对应的沟通上下文与决策依据，实现 100% 历史可回溯与一键安全回滚。
