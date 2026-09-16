# -*- coding: utf-8 -*-
"""
CoinGlass 一次性抓取器 (供 auto_trading.py 周期调用)
====================================================
1) 清算地图: 子进程运行 coinglass_scraper.py (ETH 三所聚合图),
   从 data/decrypted_*.json 中定位最新聚合结构(rangeHigh/data/lastPrice),
   原子写入 data/auto/liq_latest.json
2) VWAP 图例: 打开 legend 图表页一次, 解析图例数值(VWAP/#1/#2/#3 带),
   写 data/legend/vwap_legend_values.json (与 vwap_read.py 同格式)
3) 清理 data/ 下超过 24 小时的抓取残留文件

stdout 最后一行输出 JSON 摘要: {"ok":bool, "liq":..., "vwap":..., "detail":...}
"""
import asyncio
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

LIQ_LATEST = BASE / "data" / "auto" / "liq_latest.json"
SCRAPER = BASE / "coinglass_scraper.py"
DATA_DIR = BASE / "data"
LEGEND_OUT = BASE / "data" / "legend" / "vwap_legend_values.json"

# 复用 vwap_read 的图例解析与输出
from vwap_read import URL as LEGEND_URL, read_vwap_legend  # noqa: E402


def log(msg):
    print("[fetch] %s" % msg, flush=True)


# ----------------------------------------------------------------------------
# 1) 清算地图
# ----------------------------------------------------------------------------
def find_latest_agg(max_age_sec=600):
    """在 data/decrypted_*.json 中找最新的聚合 exLiqMap 结构"""
    files = sorted(DATA_DIR.glob("decrypted_*.json"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    now = time.time()
    for f in files[:30]:
        try:
            if now - f.stat().st_mtime > max_age_sec:
                break
            d = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        if (isinstance(d, dict) and "rangeHigh" in d and "lastPrice" in d
                and isinstance(d.get("data"), list) and d["data"]):
            return f, d
    return None, None


def fetch_liq(timeout=420):
    """子进程运行爬虫, 成功后把最新聚合数据固化到 liq_latest.json"""
    cookies_candidates = [
        BASE / "coinglass_manual.json",
        Path.home() / "coinglass_manual.json",
        BASE / "coinglass_auth.json",
    ]
    cookies_file = next((c for c in cookies_candidates if c.exists()), None)

    cmd = [sys.executable, str(SCRAPER), "--symbol", "ETH",
           "--out", str(DATA_DIR), "--wait", "10"]
    if cookies_file:
        cmd.extend(["--cookies", str(cookies_file)])

    try:
        r = subprocess.run(
            cmd,
            cwd=str(BASE), capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, "爬虫超时(%ds)" % timeout

    out_str = (r.stdout or "") + " " + (r.stderr or "")
    if "code 40000" in out_str or "code=40000" in out_str:
        return False, "CoinGlass ETH 数据需登录 (code 40000)，当前 cookies 已失效，请更新 coinglass_manual.json"

    if r.returncode != 0:
        tail = (r.stdout or "").strip().splitlines()[-3:]
        return False, "爬虫退出码 %d: %s" % (r.returncode, " / ".join(tail))

    src, d = find_latest_agg()
    if not src:
        return False, "未捕获到聚合清算地图数据"
    LIQ_LATEST.parent.mkdir(parents=True, exist_ok=True)
    # AUDIT_FIX_ASOF_001: stamp fetched_ts for point-in-time replay
    payload = dict(d)
    fetched_ts_ms = int(time.time() * 1000)
    try:
        fetched_ts_ms = int(src.stat().st_mtime * 1000)
    except Exception:
        pass
    payload["fetched_ts"] = fetched_ts_ms
    payload["fetched_at"] = datetime.now().isoformat(timespec="seconds")
    payload["_source_file"] = src.name
    tmp = LIQ_LATEST.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(LIQ_LATEST)
    # Also append to history index for as-of selection
    try:
        hist_dir = LIQ_LATEST.parent / "liq_history"
        hist_dir.mkdir(parents=True, exist_ok=True)
        hist_name = "liq_%s.json" % datetime.now().strftime("%Y%m%d_%H%M%S")
        (hist_dir / hist_name).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
    log("清算地图 -> %s (lastPrice %.2f, fetched_ts=%s)" % (LIQ_LATEST.name, d["lastPrice"], fetched_ts_ms))
    return True, "lastPrice %.2f, %d 所" % (d["lastPrice"], len(d["data"]))


# ----------------------------------------------------------------------------
# 2) VWAP 图例
# ----------------------------------------------------------------------------
async def _hover_latest_candle(page):
    """把鼠标悬停到图表最右侧(最新K线), 使图例显示最新时间的 VWAP 值"""
    try:
        box = await page.evaluate("""() => {
            const cands = [...document.querySelectorAll('canvas')].map(c => {
                const r = c.getBoundingClientRect();
                return {w: r.width, h: r.height, x: r.x, y: r.y};
            }).filter(c => c.w > 800 && c.h > 300);
            cands.sort((a, b) => b.w * b.h - a.w * a.h);
            return cands[0] || null;
        }""")
        if box:
            await page.mouse.move(box["x"] + box["w"] - 60, box["y"] + box["h"] / 2)
            await page.wait_for_timeout(1000)
            return True
    except Exception:
        pass
    return False


async def fetch_vwap():
    """打开 legend 页面一次, 解析图例数值并写入 JSON

    每次调用都强制抓取并覆盖(与清算地图同周期, 30 分钟对齐更新)。
    读取前先悬停到最新K线(图例数值跟随鼠标所在K线, 不悬停可能显示旧值)。
    """
    from playwright.async_api import async_playwright
    from coinglass_scraper import _detect_full_chromium_path

    log("打开 VWAP legend 页面: %s" % LEGEND_URL)
    executable = _detect_full_chromium_path()
    async with async_playwright() as p:
        kwargs = {"headless": True, "args": [
            "--no-sandbox", "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled"]}
        if executable:
            kwargs["executable_path"] = executable
        browser = await p.chromium.launch(**kwargs)
        try:
            ctx = await browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/131.0.0.0 Safari/537.36"),
                viewport={"width": 1800, "height": 1000}, locale="zh-CN")
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            cookies_file = BASE / "coinglass_manual.json"
            if cookies_file.exists():
                auth = json.loads(cookies_file.read_text(encoding="utf-8"))
                if auth.get("cookies"):
                    await ctx.add_cookies(auth["cookies"])
            page = await ctx.new_page()
            try:
                await page.goto(LEGEND_URL, wait_until="domcontentloaded",
                                timeout=60000)
            except Exception:
                log("goto 超时, 继续尝试解析")

            # 移除页面遮罩
            try:
                await page.evaluate("""() => {
                    document.querySelectorAll('#termly-code-snippet-support, [data-termly-part], .fc-consent-root, .fc-dialog-overlay, .MuiModal-root, .MuiModal-backdrop, .MuiBackdrop-root').forEach(e => e.remove());
                    document.body.style.overflow = 'auto';
                }""")
            except Exception:
                pass
            await page.wait_for_timeout(3000)

            if "login" in page.url:
                log("VWAP legend 页面已重定向到登录页: %s" % page.url)
                return False, "VWAP 图表需要登录访问"

            # 悬停到最新K线再读图例(图例跟随鼠标所在K线)
            hovered = await _hover_latest_candle(page)
            if hovered:
                log("已悬停最新K线")
            for attempt in range(6):
                values = await read_vwap_legend(page)
                if values:
                    # 复用 vwap_read.write_result: 写入 vwap_legend_values.json
                    from vwap_read import write_result
                    write_result(values, None)
                    log("VWAP=%.2f #3: %.2f/%.2f"
                        % (values["vwap"], values["band3_upper"],
                           values["band3_lower"]))
                    return True, values
                # 重试前再次悬停
                await _hover_latest_candle(page)
                await page.wait_for_timeout(5000)
            return False, "图例解析失败(6 次尝试)"
        finally:
            await browser.close()


# ----------------------------------------------------------------------------
# 3) 清理旧抓取文件
# ----------------------------------------------------------------------------
def cleanupOldData(max_age_h=24):
    now = time.time()
    n = 0
    for pat in ("raw_*.json", "analysis_*.json", "decrypted_*.json",
                "decrypted_analysis_*.json", "json_hook_diag_*.json",
                "echarts_diag_*.json"):
        for f in DATA_DIR.glob(pat):
            try:
                if now - f.stat().st_mtime > max_age_h * 3600:
                    f.unlink()
                    n += 1
            except OSError:
                pass
    if n:
        log("清理 %d 个超过 %dh 的旧抓取文件" % (n, max_age_h))


async def main():
    liq_ok, liq_msg = fetch_liq()
    vwap_ok, vwap_vals = False, None
    vwap_msg = "未执行"
    try:
        vwap_ok, result = await fetch_vwap()
        if vwap_ok:
            vwap_vals, vwap_msg = result, "ok"
        else:
            vwap_msg = result if isinstance(result, str) else "解析失败"
    except Exception as e:
        vwap_msg = "异常: %s" % e
    cleanupOldData()
    summary = {
        "ok": bool(liq_ok and vwap_ok),
        "liq": {"ok": bool(liq_ok), "detail": liq_msg},
        "vwap": {"ok": bool(vwap_ok), "detail": vwap_msg, "values": vwap_vals},
        "time": datetime.now().isoformat(timespec="seconds"),
    }
    print("FETCH_RESULT " + json.dumps(summary, ensure_ascii=False), flush=True)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
