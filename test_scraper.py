#!/usr/bin/env python3
"""
CoinGlass Scraper 稳定性测试脚本

独立测试 coinglass_scraper.py 模块的稳定性，包含三项测试:
1. 多币种切换测试 (BTC/ETH 数据结构一致性)
2. 爬取频率与防爬极限测试 (每 5 分钟一次，连续 12 次)
3. 控制台测试报告打印器

用法:
    # 跑全部测试 (多币种 + 1 小时频率测试)
    python test_scraper.py

    # 只跑多币种一致性测试
    python test_scraper.py --test consistency

    # 只跑频率测试，自定义间隔和次数
    python test_scraper.py --test frequency --interval 60 --runs 5

    # 调试模式 (有头浏览器)
    python test_scraper.py --headed
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# 复用原爬虫模块的核心组件
from coinglass_scraper import (
    DEFAULT_CONFIG,
    JSON_PARSE_HOOK_JS,
    _detect_full_chromium_path,
    extract_liquidation_hotspots,
    try_switch_symbol,
    url_matches,
)
from playwright.async_api import Response, TimeoutError as PlaywrightTimeoutError, async_playwright

# ============== 测试配置 ==============
TEST_CONFIG = {
    "interval_seconds": 300,   # 默认 5 分钟轮询一次
    "total_runs": 12,          # 默认连续 12 次 (共 1 小时)
    "symbols": ["BTC", "ETH"],
    "output_dir": "test_data",
    "screenshot_dir": "test_data/screenshots",
    "wait_seconds": 15,        # 页面加载后等待抓取的时长
    "cookies_file": "",        # 登录 cookies 文件路径 (非 BTC 币种需要)
}

# ============== 日志 ==============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("test_scraper")


# ============== 测试结果数据结构 ==============
class TestResult:
    """单次爬虫测试的完整结果"""

    def __init__(self, run_id: int, symbol: str):
        self.run_id = run_id
        self.symbol = symbol
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.duration_sec: float = 0.0
        self.success: bool = False
        self.api_captured: int = 0       # 通过 API 拦截命中数
        self.hook_captured: int = 0     # 通过 JSON.parse hook 命中数
        self.error: str | None = None
        self.error_type: str | None = None  # cloudflare / forbidden_403 / format_changed / timeout / other
        self.hotspots: list = []
        self.screenshot_path: str | None = None
        # 数据结构验证字段
        self.data_structure: dict = {
            "top_level_keys": [],
            "liq_row_length": 0,
            "y_length": 0,
            "instrument": {},
            "instrument_base_asset": "",
            "symbol_verified": False,  # instrument.baseAsset 是否与请求 symbol 一致
            "has_liq": False,
            "has_y": False,
            "has_prices": False,
        }


# ============== 核心测试执行 ==============
async def run_test_capture(
    run_id: int,
    symbol: str,
    output_dir: Path,
    screenshot_dir: Path,
    headless: bool = True,
    wait_seconds: int = 15,
    cookies_file: str = "",
) -> TestResult:
    """
    执行一次完整的爬虫测试:
    - 启动浏览器
    - 拦截 CoinGlass 网络请求
    - 检索 JSON.parse hook 数据
    - 异常时截图
    - 返回 TestResult
    """
    result = TestResult(run_id, symbol)
    result.start_time = datetime.now()
    start_ts = time.time()

    output_dir.mkdir(parents=True, exist_ok=True)
    screenshot_dir.mkdir(parents=True, exist_ok=True)

    executable_path = (
        os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH")
        or _detect_full_chromium_path()
    )

    captured_data: list[dict] = []  # 所有捕获到的数据块
    api_count = 0
    forbidden_count = 0

    async with async_playwright() as p:
        launch_kwargs = {
            "headless": headless,
            "args": [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        }
        if executable_path:
            launch_kwargs["executable_path"] = executable_path

        try:
            browser = await p.chromium.launch(**launch_kwargs)
        except Exception as e:
            result.error = f"浏览器启动失败: {e}"
            result.error_type = "other"
            result.duration_sec = time.time() - start_ts
            return result

        context = await browser.new_context(
            user_agent=DEFAULT_CONFIG["user_agent"],
            viewport={"width": 1600, "height": 900},
            locale="en-US",
        )

        # 反自动化检测 + JSON.parse hook
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        await context.add_init_script(JSON_PARSE_HOOK_JS)

        page = await context.new_page()

        # 加载登录状态 (支持两种格式: storageState 或 纯 cookies 数组)
        if cookies_file and Path(cookies_file).exists():
            try:
                raw_data = json.loads(Path(cookies_file).read_text(encoding="utf-8"))
                # 格式 1: Playwright storageState ({"cookies": [...], "origins": [...]})
                if isinstance(raw_data, dict) and "cookies" in raw_data:
                    # 用 add_cookies + 注入 localStorage
                    pw_cookies = raw_data.get("cookies", [])
                    await context.add_cookies(pw_cookies)
                    log.info(f"已加载 {len(pw_cookies)} 个 cookies (storageState 格式)")

                    # 注入 localStorage (origins 字段)
                    origins = raw_data.get("origins", [])
                    if origins:
                        ls_script_parts = []
                        for origin in origins:
                            host = origin.get("origin", "")
                            # 只处理 coinglass 的 origin
                            if "coinglass" not in host:
                                continue
                            ls = origin.get("localStorage", [])
                            for item in ls:
                                key = item.get("name", "").replace("'", "\\'")
                                val = item.get("value", "").replace("'", "\\'")
                                ls_script_parts.append(f"localStorage.setItem('{key}', '{val}');")
                        if ls_script_parts:
                            ls_js = "try { " + " ".join(ls_script_parts) + " } catch(e) {}"
                            # 先导航到目标域名再注入 localStorage
                            await page.goto("https://www.coinglass.com", wait_until="domcontentloaded", timeout=60000)
                            await page.evaluate(ls_js)
                            log.info(f"已注入 {len(ls_script_parts)} 个 localStorage 项")
                # 格式 2: 纯 cookies 数组 (浏览器扩展导出格式)
                elif isinstance(raw_data, list):
                    pw_cookies = []
                    for c in raw_data:
                        ss = c.get("sameSite")
                        if ss == "no_restriction" or ss is None:
                            ss_pw = "None"
                        elif ss == "lax":
                            ss_pw = "Lax"
                        elif ss == "strict":
                            ss_pw = "Strict"
                        else:
                            ss_pw = "None"
                        pw_cookies.append({
                            "name": c["name"],
                            "value": c["value"],
                            "domain": c["domain"],
                            "path": c.get("path", "/"),
                            "expires": c.get("expirationDate", -1),
                            "httpOnly": bool(c.get("httpOnly", False)),
                            "secure": bool(c.get("secure", False)),
                            "sameSite": ss_pw,
                        })
                    await context.add_cookies(pw_cookies)
                    log.info(f"已加载 {len(pw_cookies)} 个 cookies (纯数组格式)")
                else:
                    log.warning(f"cookies 文件格式无法识别: {cookies_file}")
            except Exception as e:
                log.warning(f"加载登录状态失败: {e}")

        # 网络响应拦截
        async def on_response(response: Response):
            nonlocal api_count, forbidden_count
            url = response.url
            if not url_matches(url):
                return
            # 检测 403
            if response.status == 403:
                forbidden_count += 1
                return
            if response.status != 200:
                return
            try:
                data = await response.json()
                api_count += 1
                captured_data.append({"source": "api", "data": data, "url": url})
            except Exception:
                pass

        page.on("response", on_response)

        # 构建 URL: 非 BTC 币种直接用 ?coin= 参数加载，避免切换导致 token 失效
        base_url = DEFAULT_CONFIG["url"]
        if symbol.upper() != "BTC":
            url = f"{base_url}?coin={symbol.upper()}"
        else:
            url = base_url
        log.info(f"[Run #{run_id}] 加载页面: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=120000)
        except PlaywrightTimeoutError:
            result.error = "页面加载超时"
            result.error_type = "timeout"

        # 检测 Cloudflare 验证页
        try:
            content = await page.content()
            if "Just a moment" in content or "cf-browser-verification" in content:
                result.error = "检测到 Cloudflare 验证码"
                result.error_type = "cloudflare"
                result.screenshot_path = await _take_screenshot(
                    page, screenshot_dir, symbol, run_id, "cloudflare"
                )
                await _safe_close(context, browser)
                result.duration_sec = time.time() - start_ts
                result.end_time = datetime.now()
                return result
        except Exception:
            pass

        # 检测登录需求 (ETH 等非 BTC 币种需要登录)
        # 现象: API 返回 40000 + canvas 未渲染 + 页面显示 "Sign in"
        try:
            login_check = await page.evaluate("""() => ({
                has_login_text: document.body.innerText.includes('Sign in') || document.body.innerText.includes('Log in'),
                canvas_count: document.querySelectorAll('canvas').length,
            })""")
            if login_check["has_login_text"] and login_check["canvas_count"] == 0 and symbol.upper() != "BTC":
                result.error = (
                    f"币种 {symbol} 需要登录 CoinGlass 才能获取热力图数据 "
                    f"(API 返回 40000, canvas 未渲染)。"
                    f"请提供 cookies 文件或登录后重试。"
                )
                result.error_type = "login_required"
                result.screenshot_path = await _take_screenshot(
                    page, screenshot_dir, symbol, run_id, "login_required"
                )
                await _safe_close(context, browser)
                result.duration_sec = time.time() - start_ts
                result.end_time = datetime.now()
                return result
        except Exception:
            pass

        # 检测 403 禁止访问 (主文档)
        try:
            if forbidden_count > 0 and api_count == 0:
                result.error = f"遇到 403 禁止访问 ({forbidden_count} 次)"
                result.error_type = "forbidden_403"
                result.screenshot_path = await _take_screenshot(
                    page, screenshot_dir, symbol, run_id, "403"
                )
                await _safe_close(context, browser)
                result.duration_sec = time.time() - start_ts
                result.end_time = datetime.now()
                return result
        except Exception:
            pass

        # 等待 networkidle
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except PlaywrightTimeoutError:
            log.debug("networkidle 未达成，继续等待显式时长...")

        # 等待数据加载 (直接 URL 加载，无需 UI 切换)
        log.info(f"[Run #{run_id}] 等待 {symbol} 数据加载...")
        await asyncio.sleep(wait_seconds)

        # 检索 JSON.parse hook 拦截的数据
        hook_count = 0
        try:
            hook_result = await page.evaluate("""() => ({
                heatmap: window.__captured_heatmap_data__ || [],
                all_json: window.__captured_all_json__ || [],
            })""")
            hook_data = hook_result.get("heatmap", [])
            for item in hook_data:
                data = item.get("data")
                if data:
                    captured_data.append({"source": "json_parse_hook", "data": data})
                    hook_count += 1
        except Exception as e:
            log.warning(f"检索 hook 数据失败: {e}")

        # 如果完全没捕获到数据，视为异常并截图
        if not captured_data:
            result.error = result.error or "未捕获到任何热力图数据 (数据包格式可能已改变)"
            result.error_type = result.error_type or "format_changed"
            result.screenshot_path = await _take_screenshot(
                page, screenshot_dir, symbol, run_id, "no_data"
            )
            await _safe_close(context, browser)
            result.duration_sec = time.time() - start_ts
            result.end_time = datetime.now()
            return result

        # 找出具备 CoinGlass 解密格式 (liq + y) 的数据块
        # 优先选择 instrument.baseAsset 与请求 symbol 匹配的数据块
        all_candidates = []
        for item in captured_data:
            data = item.get("data")
            if isinstance(data, dict) and "liq" in data and "y" in data:
                all_candidates.append(data)
            elif isinstance(data, dict) and isinstance(data.get("data"), dict):
                inner = data["data"]
                if isinstance(inner, dict) and "liq" in inner and "y" in inner:
                    all_candidates.append(inner)

        primary_data = None
        symbol_verified = False
        if all_candidates:
            # 优先匹配 instrument.baseAsset
            target = symbol.upper()
            for cand in all_candidates:
                inst = cand.get("instrument", {}) or {}
                base = str(inst.get("baseAsset", "")).upper()
                if base == target:
                    primary_data = cand
                    symbol_verified = True
                    break
            # 兜底: 没匹配上就取第一个
            if primary_data is None:
                primary_data = all_candidates[0]

        if primary_data is None:
            # 捕获到了数据但结构不匹配，视为格式改变
            result.error = "捕获到数据但结构不匹配 CoinGlass 解密格式 (liq+y 缺失)"
            result.error_type = "format_changed"
            result.screenshot_path = await _take_screenshot(
                page, screenshot_dir, symbol, run_id, "format"
            )
            # 保存原始捕获以便排查
            _dump_captured(captured_data, output_dir, symbol, run_id)
            await _safe_close(context, browser)
            result.duration_sec = time.time() - start_ts
            result.end_time = datetime.now()
            return result

        # ===== 关键: 验证 instrument.baseAsset 与请求 symbol 一致 =====
        inst = primary_data.get("instrument", {}) or {}
        base_asset = str(inst.get("baseAsset", "")).upper()
        y_prices = primary_data.get("y", [])
        y_range_str = f"{y_prices[0]:.2f} - {y_prices[-1]:.2f}" if y_prices else "N/A"
        log.info(
            f"[Run #{run_id}] instrument.baseAsset = '{base_asset}' | "
            f"请求 symbol = '{symbol.upper()}' | "
            f"y 价格范围 = {y_range_str} | "
            f"匹配 = {symbol_verified}"
        )

        if not symbol_verified:
            # 币种未切换成功: instrument.baseAsset 与请求不符
            result.error = (
                f"币种切换失败: instrument.baseAsset='{base_asset}' "
                f"与请求 symbol='{symbol.upper()}' 不一致 "
                f"(y 价格范围: {y_range_str})"
            )
            result.error_type = "symbol_mismatch"
            result.screenshot_path = await _take_screenshot(
                page, screenshot_dir, symbol, run_id, "symbol_mismatch"
            )
            _dump_captured(captured_data, output_dir, symbol, run_id)
            await _safe_close(context, browser)
            result.duration_sec = time.time() - start_ts
            result.end_time = datetime.now()
            return result

        # 成功: 记录数据结构和清算密集区
        result.success = True
        result.api_captured = api_count
        result.hook_captured = hook_count
        result.hotspots = extract_liquidation_hotspots(primary_data) or []
        result.data_structure = {
            "top_level_keys": list(primary_data.keys()),
            "liq_row_length": (
                len(primary_data["liq"][0])
                if primary_data.get("liq") and isinstance(primary_data["liq"][0], (list, tuple))
                else 0
            ),
            "y_length": len(primary_data.get("y", [])),
            "liq_count": len(primary_data.get("liq", [])),
            "prices_count": len(primary_data.get("prices", [])),
            "instrument": primary_data.get("instrument", {}),
            "instrument_base_asset": base_asset,
            "symbol_verified": symbol_verified,
            "has_liq": True,
            "has_y": True,
            "has_prices": "prices" in primary_data,
            "range_high": primary_data.get("rangeHigh"),
            "range_low": primary_data.get("rangeLow"),
            "update_time": primary_data.get("updateTime"),
        }

        # 保存捕获数据到文件以便复查
        _dump_captured(captured_data, output_dir, symbol, run_id)

        await _safe_close(context, browser)

    result.duration_sec = time.time() - start_ts
    result.end_time = datetime.now()
    return result


async def _take_screenshot(page, screenshot_dir: Path, symbol: str, run_id: int, tag: str) -> str:
    """异常时截图保存 (同时保留带时间戳的历史 + error_screenshot.png 最新)"""
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = screenshot_dir / f"error_{tag}_{symbol}_run{run_id}_{ts}.png"
        await page.screenshot(path=str(path), full_page=True)
        # 复制一份为 error_screenshot.png 作为最新错误
        latest = screenshot_dir / "error_screenshot.png"
        try:
            latest.write_bytes(path.read_bytes())
        except Exception:
            pass
        log.warning(f"截图已保存: {path}")
        return str(path)
    except Exception as e:
        log.error(f"截图失败: {e}")
        return ""


async def _safe_close(context, browser):
    """安全关闭浏览器"""
    try:
        await context.close()
    except Exception:
        pass
    try:
        await browser.close()
    except Exception:
        pass


async def _try_trigger_refresh(page):
    """
    尝试通过 UI 交互触发 CoinGlass 重新加载热力图数据
    (当切换币种后 API 返回 40000 时，点击 Model 标签切换可触发新 API 调用)
    """
    # 方案 1: 点击 Model 2 再点回 Model 1 (触发 API 重新请求)
    model_selectors = [
        "button:has-text('Model 2')",
        "button:has-text('Model 1')",
        "[role='tab']:has-text('Model 2')",
        "[role='tab']:has-text('Model 1')",
        ".MuiTab-root:has-text('Model 2')",
        ".MuiTab-root:has-text('Model 1')",
    ]
    for sel in model_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.click(timeout=3000)
                log.info(f"点击 {sel} 触发刷新")
                await asyncio.sleep(3)
                return
        except Exception:
            continue

    # 方案 2: 点击 "Pair" 标签切换
    pair_selectors = [
        "button:has-text('Pair')",
        "[role='tab']:has-text('Pair')",
    ]
    for sel in pair_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.click(timeout=3000)
                log.info(f"点击 {sel} 触发刷新")
                await asyncio.sleep(3)
                return
        except Exception:
            continue

    log.warning("未能找到可触刷新的 UI 元素")


def _dump_captured(captured_data: list, output_dir: Path, symbol: str, run_id: int):
    """保存捕获的数据到文件以便复查"""
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = output_dir / f"test_capture_{symbol}_run{run_id}_{ts}.json"
        path.write_text(
            json.dumps(captured_data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        log.info(f"捕获数据已保存: {path}")
    except Exception as e:
        log.error(f"保存捕获数据失败: {e}")


# ============== 测试 1: 多币种切换 + 数据结构一致性 ==============
async def test_multi_symbol_consistency(
    output_dir: Path,
    screenshot_dir: Path,
    headless: bool = True,
    wait_seconds: int = 15,
    cookies_file: str = "",
    symbols: list[str] | None = None,
) -> dict:
    """
    测试 1: 交替请求 BTC 和 ETH，验证拦截机制和数据结构一致性
    返回: {
        "btc": TestResult,
        "eth": TestResult,
        "structure_consistent": bool,
        "diff": dict,
    }
    """
    print("\n" + "#" * 60)
    print("#  测试 1: 多币种切换 + 数据结构一致性测试")
    print("#" * 60 + "\n")

    results = {}
    test_symbols = symbols or TEST_CONFIG["symbols"]
    for symbol in test_symbols:
        print(f"\n>>> 开始测试币种: {symbol}")
        result = await run_test_capture(
            run_id=0,
            symbol=symbol,
            output_dir=output_dir,
            screenshot_dir=screenshot_dir,
            headless=headless,
            wait_seconds=wait_seconds,
            cookies_file=cookies_file,
        )
        results[symbol.lower()] = result
        print_test_report(result, next_interval=None)
        # 币种之间稍微停一下避免连击
        await asyncio.sleep(3)

    # 对比数据结构
    btc = results.get("btc")
    eth = results.get("eth")
    diff = {"consistent": False, "details": {}}
    if btc and eth and btc.success and eth.success:
        btc_keys = set(btc.data_structure.get("top_level_keys", []))
        eth_keys = set(eth.data_structure.get("top_level_keys", []))
        btc_row_len = btc.data_structure.get("liq_row_length", 0)
        eth_row_len = eth.data_structure.get("liq_row_length", 0)
        diff["details"] = {
            "btc_top_level_keys": sorted(btc_keys),
            "eth_top_level_keys": sorted(eth_keys),
            "keys_only_in_btc": sorted(btc_keys - eth_keys),
            "keys_only_in_eth": sorted(eth_keys - btc_keys),
            "btc_liq_row_length": btc_row_len,
            "eth_liq_row_length": eth_row_len,
            "btc_y_length": btc.data_structure.get("y_length", 0),
            "eth_y_length": eth.data_structure.get("y_length", 0),
            "btc_has_prices": btc.data_structure.get("has_prices", False),
            "eth_has_prices": eth.data_structure.get("has_prices", False),
        }
        # 一致性判定: 顶层字段相同 + liq row 长度相同 + 都有 prices
        diff["consistent"] = (
            btc_keys == eth_keys
            and btc_row_len == eth_row_len
            and btc.data_structure.get("has_prices") == eth.data_structure.get("has_prices")
        )

    # 打印一致性报告
    print("\n" + "-" * 60)
    print("  数据结构一致性对比报告")
    print("-" * 60)
    if not (btc and eth):
        print(f"  BTC 成功: {btc.success if btc else 'N/A'} | ETH 成功: {eth.success if eth else 'N/A'}")
        print("  无法完成一致性对比 (至少一个币种未捕获到数据)")
    elif not (btc.success and eth.success):
        print(f"  BTC 成功: {btc.success} | ETH 成功: {eth.success}")
        failed = []
        if not btc.success:
            failed.append(f"BTC({btc.error_type or '失败'})")
        if not eth.success:
            failed.append(f"ETH({eth.error_type or '失败'})")
        print(f"  失败币种: {', '.join(failed)}")
        print("  建议检查网络/反爬机制后重试")
    else:
        print(f"  BTC 顶层字段: {diff['details']['btc_top_level_keys']}")
        print(f"  ETH 顶层字段: {diff['details']['eth_top_level_keys']}")
        print(f"  BTC only: {diff['details']['keys_only_in_btc']}")
        print(f"  ETH only: {diff['details']['keys_only_in_eth']}")
        print(f"  liq 行长度: BTC={diff['details']['btc_liq_row_length']} | ETH={diff['details']['eth_liq_row_length']}")
        print(f"  y 档位数: BTC={diff['details']['btc_y_length']} | ETH={diff['details']['eth_y_length']}")
        print(f"  prices 字段: BTC={diff['details']['btc_has_prices']} | ETH={diff['details']['eth_has_prices']}")
        status = "✓ 一致" if diff["consistent"] else "✗ 不一致"
        print(f"\n  一致性结论: {status}")
        if diff["consistent"]:
            print("  >> BTC/ETH 数据结构完全一致，清洗代码可通用")
        else:
            print("  >> 数据结构存在差异，需要为不同币种做兼容处理")
    print("-" * 60 + "\n")

    return {
        "btc": btc,
        "eth": eth,
        "structure_consistent": diff.get("consistent", False),
        "diff": diff,
    }


# ============== 测试 2: 爬取频率与防爬极限测试 ==============
async def test_frequency_loop(
    interval_seconds: int,
    total_runs: int,
    output_dir: Path,
    screenshot_dir: Path,
    headless: bool = True,
    wait_seconds: int = 15,
    symbols: list[str] | None = None,
    cookies_file: str = "",
) -> dict:
    """
    测试 2: 循环运行爬虫，每 interval_seconds 秒一次，共 total_runs 次
    监控: 成功率、耗时、异常类型
    """
    print("\n" + "#" * 60)
    print(f"#  测试 2: 爬取频率与防爬极限测试")
    print(f"#  间隔: {interval_seconds}s | 总次数: {total_runs}")
    print("#" * 60 + "\n")

    symbols = symbols or TEST_CONFIG["symbols"]
    results: list[TestResult] = []
    success_count = 0
    total_duration = 0.0
    error_breakdown: dict[str, int] = {}

    for run_id in range(1, total_runs + 1):
        # 轮换币种
        symbol = symbols[(run_id - 1) % len(symbols)]
        print(f"\n>>> 第 {run_id}/{total_runs} 次测试 | 币种: {symbol}")

        try:
            result = await run_test_capture(
                run_id=run_id,
                symbol=symbol,
                output_dir=output_dir,
                screenshot_dir=screenshot_dir,
                headless=headless,
                wait_seconds=wait_seconds,
                cookies_file=cookies_file,
            )
        except KeyboardInterrupt:
            print("\n[!] 用户中断测试，打印部分结果...")
            break
        except Exception as e:
            result = TestResult(run_id, symbol)
            result.error = f"未预期异常: {e}"
            result.error_type = "other"
            result.duration_sec = 0.0
            log.error(f"未预期异常: {e}", exc_info=True)

        results.append(result)
        if result.success:
            success_count += 1
        else:
            et = result.error_type or "unknown"
            error_breakdown[et] = error_breakdown.get(et, 0) + 1
        total_duration += result.duration_sec

        # 计算下一次运行的等待时间
        next_interval = None
        if run_id < total_runs:
            next_interval = max(0, interval_seconds)

        # 打印本次报告
        print_test_report(result, next_interval=next_interval)

        # 等待下一次 (最后一次不等待)
        if run_id < total_runs:
            print(f"\n[STATUS] 爬虫运行{'正常' if result.success else '异常'}，等待下一次 {interval_seconds}s 轮询...")
            print("=" * 50)
            try:
                await asyncio.sleep(interval_seconds)
            except KeyboardInterrupt:
                print("\n[!] 用户中断等待，打印部分结果...")
                break

    # 汇总报告
    print_frequency_summary(results, success_count, total_runs, total_duration, error_breakdown, interval_seconds)

    return {
        "results": results,
        "total_runs": len(results),
        "success_count": success_count,
        "success_rate": success_count / len(results) if results else 0,
        "total_duration": total_duration,
        "error_breakdown": error_breakdown,
    }


def print_frequency_summary(
    results: list[TestResult],
    success_count: int,
    total_runs: int,
    total_duration: float,
    error_breakdown: dict,
    interval_seconds: int,
):
    """打印频率测试汇总报告"""
    actual_runs = len(results)
    print("\n" + "=" * 60)
    print("  频率测试汇总报告")
    print("=" * 60)
    print(f"  计划运行次数: {total_runs}")
    print(f"  实际运行次数: {actual_runs}")
    print(f"  成功次数: {success_count}")
    print(f"  失败次数: {actual_runs - success_count}")
    print(f"  成功率: {(success_count / actual_runs * 100):.1f}%" if actual_runs else "  成功率: N/A")
    print(f"  间隔: {interval_seconds}s (共约 {interval_seconds * total_runs / 60:.1f} 分钟)")
    print(f"  累计耗时: {total_duration:.1f}s (平均 {total_duration / actual_runs:.1f}s/次)" if actual_runs else "")

    # 耗时分布
    durations = [r.duration_sec for r in results if r.success]
    if durations:
        print(f"\n  耗时统计 (仅成功次数):")
        print(f"    最快: {min(durations):.2f}s")
        print(f"    最慢: {max(durations):.2f}s")
        print(f"    平均: {sum(durations) / len(durations):.2f}s")

    # 异常分类
    if error_breakdown:
        print(f"\n  异常分类:")
        type_map = {
            "cloudflare": "Cloudflare 验证码",
            "forbidden_403": "403 禁止访问",
            "format_changed": "数据包格式改变",
            "symbol_mismatch": "币种切换失败 (instrument 不符)",
            "login_required": "需要登录 (非 BTC 币种)",
            "timeout": "页面加载超时",
            "other": "其他异常",
            "unknown": "未知异常",
        }
        for et, cnt in error_breakdown.items():
            label = type_map.get(et, et)
            print(f"    {label}: {cnt} 次")

    # 明细表
    print(f"\n  明细 (Run# | 币种 | 结果 | 耗时(s) | 异常类型):")
    print("  " + "-" * 56)
    for r in results:
        status = "SUCCESS" if r.success else "FAIL"
        et = r.error_type or "-"
        print(f"  #{r.run_id:<3} | {r.symbol:<4} | {status:<7} | {r.duration_sec:>7.2f} | {et}")
    print("  " + "-" * 56)

    # 100% 成功率判定
    if actual_runs == total_runs and success_count == total_runs:
        print(f"\n  ✓ 成功率达到 100%，爬虫稳定运行")
    else:
        print(f"\n  ✗ 成功率未达 100%，建议查看 error_screenshot.png 排查")
    print("=" * 60 + "\n")


# ============== 测试报告打印器 ==============
def print_test_report(result: TestResult, next_interval: int | None = None):
    """
    控制台直观打印单次测试报告，格式:
    ==================================================
            CoinGlass Scraper 稳定性测试中...
    ==================================================
    当前测试时间: ...
    请求币种: ETH  |  浏览器模式: Headless (无头)
    --------------------------------------------------
    [INFO] 正在启动 Playwright 拦截网络请求...
    [SUCCESS] 成功截获并解密底层数据包！总耗时: 3.4 秒
    --- 今日 ETH 爆仓密集区 (Top 3) 提取测试 ---
    1. 价格: $XXXX.XX | 预估清算总量: X.XX B | 索引档位: XX
    ...
    [STATUS] 爬虫运行正常，等待下一次 5 分钟轮询...
    ==================================================
    """
    print("\n" + "=" * 50)
    print("        CoinGlass Scraper 稳定性测试中...")
    print("=" * 50)

    now_str = (result.start_time or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    mode = "Headless (无头)" if not getattr(result, "_headed", False) else "Headed (有头)"
    print(f"当前测试时间: {now_str}")
    print(f"请求币种: {result.symbol}  |  浏览器模式: {mode}")
    print("-" * 50)
    print("[INFO] 正在启动 Playwright 拦截网络请求...")

    if result.success:
        print(f"[SUCCESS] 成功截获并解密底层数据包！总耗时: {result.duration_sec:.1f} 秒")
        print(f"         API 命中: {result.api_captured} | Hook 命中: {result.hook_captured}")

        # 数据结构信息 (先打印 instrument 字段验证币种)
        ds = result.data_structure or {}
        inst = ds.get("instrument", {})
        base_asset = ds.get("instrument_base_asset") or inst.get("baseAsset", "?")
        quote_asset = inst.get("quoteAsset", "?")
        ex_name = inst.get("exName", "?")
        # 关键: instrument 验证行，确保输出的是请求币种而非 BTC
        verify_tag = "OK" if ds.get("symbol_verified") else "MISMATCH"
        print(f"         [币种验证 {verify_tag}] instrument.baseAsset = '{base_asset}' (请求: {result.symbol})")
        print(f"         交易对: {base_asset}/{quote_asset} @ {ex_name}")
        # y 价格范围 (直接反映币种是否正确: BTC ~60000+, ETH ~1800+)
        y_len = ds.get("y_length", 0)
        if ds.get("range_low") is not None and ds.get("range_high") is not None:
            print(f"         y 价格档位: {y_len} 个 | 范围: {ds.get('range_low')} - {ds.get('range_high')}")
        if ds.get("liq_count"):
            print(f"         清算数据点: {ds.get('liq_count')} | 蜡烛数: {ds.get('prices_count', 0)}")

        # 多空各 Top 3 清算密集区 (仅最新时间切片，单位 M 百万)
        # 多空语义: 当前价上方 = 空头清算区 (上涨爆空) | 下方 = 多头清算区 (下跌爆多)
        hotspots = result.hotspots or []
        print(f"\n--- 今日 {result.symbol} 多空爆仓密集区 (各 Top 3) 提取测试 ---")
        if not hotspots:
            print("  (无清算密集区数据)")
        else:
            long_hs = [h for h in hotspots if h.get("side") == "long"][:3]
            short_hs = [h for h in hotspots if h.get("side") == "short"][:3]

            print(f"\n  [多头清算区 Top {len(long_hs)}] (当前价下方, 价格下跌爆多):")
            if not long_hs:
                print("    (无)")
            for i, h in enumerate(long_hs, 1):
                total_m = h.get("total_liq", 0) / 1e6
                print(f"  {i}. 价格: ${h.get('price', 0):,.2f} | 预估清算总量: {total_m:.2f} M | 索引档位: {h.get('price_index', '?')}")

            print(f"\n  [空头清算区 Top {len(short_hs)}] (当前价上方, 价格上涨爆空):")
            if not short_hs:
                print("    (无)")
            for i, h in enumerate(short_hs, 1):
                total_m = h.get("total_liq", 0) / 1e6
                print(f"  {i}. 价格: ${h.get('price', 0):,.2f} | 预估清算总量: {total_m:.2f} M | 索引档位: {h.get('price_index', '?')}")
    else:
        # 失败
        print(f"[FAIL] 拦截失败！总耗时: {result.duration_sec:.1f} 秒")
        print(f"       异常类型: {result.error_type or 'unknown'}")
        print(f"       错误信息: {result.error or '未知错误'}")
        if result.screenshot_path:
            print(f"       截图已保存: {result.screenshot_path}")
        # 同时保存一份 error_screenshot.png 作为最新错误
        print("\n  (本次未捕获到清算密集区数据)")

    if next_interval is not None:
        minutes = next_interval // 60
        print(f"\n[STATUS] 爬虫运行{'正常' if result.success else '异常'}，等待下一次 {minutes} 分钟轮询...")
    print("=" * 50)


# ============== 入口 ==============
def parse_args():
    p = argparse.ArgumentParser(description="CoinGlass Scraper 稳定性测试")
    p.add_argument(
        "--test",
        choices=["all", "consistency", "frequency"],
        default="all",
        help="测试类型: all(默认) / consistency(只跑多币种) / frequency(只跑频率)",
    )
    p.add_argument("--interval", type=int, default=TEST_CONFIG["interval_seconds"],
                   help=f"频率测试间隔秒数 (默认 {TEST_CONFIG['interval_seconds']})")
    p.add_argument("--runs", type=int, default=TEST_CONFIG["total_runs"],
                   help=f"频率测试总次数 (默认 {TEST_CONFIG['total_runs']})")
    p.add_argument("--wait", type=int, default=TEST_CONFIG["wait_seconds"],
                   help=f"页面加载后等待秒数 (默认 {TEST_CONFIG['wait_seconds']})")
    p.add_argument("--out", default=TEST_CONFIG["output_dir"], help="输出目录")
    p.add_argument("--headed", action="store_true", help="非 headless 模式 (调试用)")
    p.add_argument("--cookies", default="", help="登录 cookies JSON 文件路径 (非 BTC 币种需要)")
    p.add_argument("--symbols", default=",".join(TEST_CONFIG["symbols"]),
                   help=f"测试币种列表，逗号分隔 (默认 {'/'.join(TEST_CONFIG['symbols'])})，如只测 ETH: --symbols ETH")
    return p.parse_args()


async def async_main(args):
    output_dir = Path(args.out)
    screenshot_dir = output_dir / "screenshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    screenshot_dir.mkdir(parents=True, exist_ok=True)

    headless = not args.headed
    wait_seconds = args.wait

    # 给 TestResult 加一个标记用于打印模式
    def _make_result_factory(headed_flag):
        def factory(run_id, symbol):
            r = TestResult(run_id, symbol)
            r._headed = headed_flag
            return r
        return factory

    # 覆盖 run_test_capture 中的 TestResult 创建以传递 headed 标记
    # (简单做法: 修改类属性)
    TestResult._headed = not headless

    print("\n" + "=" * 60)
    print("  CoinGlass Scraper 稳定性测试套件")
    print("=" * 60)
    print(f"  测试类型: {args.test}")
    print(f"  浏览器模式: {'Headed (有头)' if args.headed else 'Headless (无头)'}")
    if args.test in ("all", "frequency"):
        print(f"  频率测试: 间隔 {args.interval}s, 共 {args.runs} 次 (约 {args.interval * args.runs / 60:.1f} 分钟)")
    print(f"  输出目录: {output_dir}")
    print(f"  截图目录: {screenshot_dir}")
    print("=" * 60 + "\n")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    if args.test in ("all", "consistency"):
        await test_multi_symbol_consistency(
            output_dir=output_dir,
            screenshot_dir=screenshot_dir,
            headless=headless,
            wait_seconds=wait_seconds,
            cookies_file=args.cookies,
            symbols=symbols,
        )

    if args.test in ("all", "frequency"):
        await test_frequency_loop(
            interval_seconds=args.interval,
            total_runs=args.runs,
            output_dir=output_dir,
            screenshot_dir=screenshot_dir,
            headless=headless,
            wait_seconds=wait_seconds,
            cookies_file=args.cookies,
            symbols=symbols,
        )

    print("\n[完成] 所有测试结束。")


def main():
    args = parse_args()
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("\n[!] 用户中断，退出。")
        sys.exit(130)


if __name__ == "__main__":
    main()
