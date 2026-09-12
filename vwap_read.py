# -*- coding: utf-8 -*-
"""
CoinGlass Legend VWAP 实时读取器 (5秒轮询)
打开页面一次, 图例自动跟随最新 K 线刷新, 每 5 秒解析一次图例数值并写入 JSON:
  data/legend/vwap_legend_values.json
后续程序直接读该文件即可。

字段说明:
  updated_at   : 本地更新时间
  ok           : 本次解析是否成功
  vwap         : VWAP 主线
  band1_upper/lower, band2_..., band3_...: 各带上下轨 (#3 = ±3σ)
"""
import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).parent))
from coinglass_scraper import _detect_full_chromium_path  # noqa: E402

URL = "https://legend.coinglass.com/zh/chart/7882cceb5fdc421c997e27d47b5798c9"
OUT_DIR = Path(__file__).parent / "data" / "legend"
OUT_FILE = OUT_DIR / "vwap_legend_values.json"
OUT_DIR.mkdir(parents=True, exist_ok=True)

POLL_INTERVAL = 5  # 秒

# 图例匹配: VWAP <num> #1 <up> <low> #2 <up> <low> #3 <up> <low>
VWAP_RE = re.compile(
    r"VWAP\s*([\d,]+\.?\d*)\s*#1\s*([\d,]+\.?\d*)\s*([\d,]+\.?\d*)"
    r"\s*#2\s*([\d,]+\.?\d*)\s*([\d,]+\.?\d*)"
    r"\s*#3\s*([\d,]+\.?\d*)\s*([\d,]+\.?\d*)"
)


async def read_vwap_legend(page):
    """从图例 DOM 中解析 VWAP + 各 Band 数值, 失败返回 None"""
    blocks = await page.evaluate(
        r"""
        () => {
            const out = [];
            for (const e of document.querySelectorAll('div,section,main,article')) {
                const t = (e.innerText || '').trim();
                if (t && /vwap/i.test(t)) {
                    out.push(t);
                }
            }
            out.sort((a, b) => a.length - b.length);
            const bodyText = (document.body && document.body.innerText) || '';
            if (bodyText) out.push(bodyText);
            return out;
        }
        """
    )
    for block in blocks:
        m = VWAP_RE.search(block.replace("\n", " "))
        if m:
            f = lambda s: float(s.replace(",", ""))
            return {
                "vwap": f(m.group(1)),
                "band1_upper": f(m.group(2)),
                "band1_lower": f(m.group(3)),
                "band2_upper": f(m.group(4)),
                "band2_lower": f(m.group(5)),
                "band3_upper": f(m.group(6)),
                "band3_lower": f(m.group(7)),
            }

    # 结构化 DOM .font2 兜底解析
    try:
        font2_items = await page.evaluate("""() => {
            return [...document.querySelectorAll('.font2')].map(el => {
                const parent = el.parentElement;
                return {
                    label: el.innerText,
                    parentText: parent ? parent.innerText : ''
                };
            });
        }""")
        parsed = {}
        for it in font2_items:
            t = it['parentText']
            if 'VWAP\n' in t:
                m = re.search(r'VWAP\n([\d,]+\.?\d*)', t)
                if m: parsed['vwap'] = float(m.group(1).replace(',', ''))
            elif '#1\n' in t:
                m = re.search(r'#1\n([\d,]+\.?\d*)', t)
                if m: parsed['band1_upper'] = float(m.group(1).replace(',', ''))
            elif '#2\n' in t:
                m = re.search(r'#2\n([\d,]+\.?\d*)', t)
                if m: parsed['band2_upper'] = float(m.group(1).replace(',', ''))
            elif '#3\n' in t:
                m = re.search(r'#3\n([\d,]+\.?\d*)', t)
                if m: parsed['band3_upper'] = float(m.group(1).replace(',', ''))
            elif it['label'] == '' and re.match(r'^[\d,]+\.?\d*$', t.strip()):
                val = float(t.strip().replace(',', ''))
                if 'band1_lower' not in parsed:
                    parsed['band1_lower'] = val
                elif 'band2_lower' not in parsed:
                    parsed['band2_lower'] = val
                elif 'band3_lower' not in parsed:
                    parsed['band3_lower'] = val
        if len(parsed) >= 7:
            return parsed
    except Exception:
        pass

    return None


def write_result(values: dict | None, err: str | None):
    """写入 JSON (ok=False 时保留上次 values 便于下游判断)"""
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "ok": values is not None,
    }
    if values:
        payload.update(values)
    if err:
        payload["error"] = err
    tmp = OUT_FILE.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    tmp.replace(OUT_FILE)  # 原子替换, 避免下游读到半截文件


async def main():
    executable_path = _detect_full_chromium_path()
    async with async_playwright() as p:
        launch_kwargs = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        }
        if executable_path:
            launch_kwargs["executable_path"] = executable_path
        browser = await p.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1800, "height": 1000},
            locale="zh-CN",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        cookies_file = Path(__file__).parent / "coinglass_manual.json"
        if cookies_file.exists():
            auth = json.loads(cookies_file.read_text(encoding="utf-8"))
            if auth.get("cookies"):
                await context.add_cookies(auth["cookies"])
                print(f"[i] 已加载 {len(auth['cookies'])} cookies")

        page = await context.new_page()
        print(f"[i] 打开: {URL}")
        try:
            await page.goto(URL, wait_until="domcontentloaded", timeout=90000)
        except Exception:
            print("[!] goto 超时, 继续")

        # 首次等待图例渲染
        await page.wait_for_timeout(15000)
        print(f"[i] 开始轮询, 每 {POLL_INTERVAL} 秒一次, 输出: {OUT_FILE}")

        last = None
        fail_count = 0
        round_no = 0
        while True:
            round_no += 1
            err = None
            try:
                values = await read_vwap_legend(page)
            except Exception as e:
                values = None
                err = str(e)

            if values:
                write_result(values, None)
                last = values
                fail_count = 0
                print(
                    f"[{datetime.now():%H:%M:%S}] #{round_no} VWAP={values['vwap']:,.2f} "
                    f"#3: {values['band3_upper']:,.2f} / {values['band3_lower']:,.2f}"
                )
            else:
                fail_count += 1
                # 失败时也写时间戳 (ok=False), 保留上次数值供下游参考
                write_result(last, err or "legend parse failed")
                print(f"[{datetime.now():%H:%M:%S}] #{round_no} 读取失败 x{fail_count}: {err or 'legend parse failed'}")
                # 连续多次失败 (页面可能崩溃/掉线) 则重载页面
                if fail_count >= 12:
                    print("[!] 连续 60 秒无数据, 重载页面...")
                    try:
                        await page.goto(URL, wait_until="domcontentloaded", timeout=90000)
                        await page.wait_for_timeout(15000)
                    except Exception as e2:
                        print(f"[!] 重载失败: {e2}")
                    fail_count = 0

            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[i] 已停止")
