"""
CoinGlass 登录助手 - 使用 Edge 浏览器手动登录后保存完整状态

用法:
    py login_helper.py

流程:
    1. 启动 Edge (有头模式)，打开 CoinGlass 页面
    2. 你手动完成登录 (Google/邮箱/手机号)
    3. 脚本自动轮询检测登录状态 (无需按 Enter)
    4. 检测到登录成功后自动保存 cookies + localStorage 到 coinglass_auth.json
    5. 自动验证 ETH 热力图渲染
    6. 后续用 --cookies coinglass_auth.json 即可抓取 ETH

登录检测原理:
    CoinGlass 登录后会请求 /coin-community/api/userapi/info 且 success=true
    (未登录时该接口返回 success=false / code=40000)
"""
import asyncio
import json
import os
from pathlib import Path

from playwright.async_api import async_playwright

# 登录检测超时 (秒)
LOGIN_TIMEOUT = 300
# 轮询间隔 (秒)
POLL_INTERVAL = 3

# Edge 可执行文件候选路径
EDGE_PATHS = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
]


def _detect_edge_path() -> str | None:
    """探测系统安装的 Edge 路径"""
    for path in EDGE_PATHS:
        if Path(path).exists():
            return path
    return None


async def main():
    edge_path = _detect_edge_path()
    if edge_path is None:
        print("[错误] 未找到 Edge 浏览器，请确认已安装 Microsoft Edge")
        return 1

    print("=" * 60)
    print("  CoinGlass 登录助手 (Edge 版)")
    print("=" * 60)
    print()
    print(f"  Edge 路径: {edge_path}")
    print(f"  1. 浏览器即将打开 CoinGlass 页面")
    print(f"  2. 请在浏览器中完成登录 (Google/邮箱/手机号)")
    print(f"  3. 脚本每 {POLL_INTERVAL} 秒自动检测登录状态 (无需按键)")
    print(f"  4. 检测超时: {LOGIN_TIMEOUT // 60} 分钟")
    print("=" * 60)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path=edge_path,
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        # 反自动化检测
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()

        # 监听 userapi/info 接口响应，作为登录成功的标志
        login_confirmed = False

        async def on_response(resp):
            nonlocal login_confirmed
            if "userapi/info" in resp.url:
                try:
                    body = await resp.json()
                    if isinstance(body, dict) and body.get("success") is True:
                        login_confirmed = True
                        print(f"\n  [检测] userapi/info 返回 success=true -> 已登录!")
                except Exception:
                    pass

        page.on("response", on_response)

        # 登录页加载后主动触发一次 userapi/info 检测
        # (页面初始加载就会调用它，但保险起见轮询时也检查)

        def _has_auth_cookie(cookies: list) -> bool:
            """
            判断是否包含真正的登录认证 cookie
            注意: csrf_token 只是防 CSRF 令牌，未登录也有，不能作为登录依据
            CoinGlass 登录后的典型认证 cookie:
            - id: 格式如 "xxxx||t=时间戳|et=...|cs=..."
            - secret_key / access_token 等
            """
            for c in cookies:
                name = c["name"].lower()
                value = c.get("value", "")
                if name == "csrf_token":
                    continue  # 明确排除 csrf_token
                if name == "id" and "||t=" in value:
                    return True  # CoinGlass 用户 id cookie 格式
                if any(x in name for x in ["secret_key", "access_token", "token_id"]):
                    return True
            return False

        # 打开 CoinGlass 页面
        try:
            await page.goto(
                "https://www.coinglass.com/pro/futures/LiquidationHeatMap",
                wait_until="domcontentloaded",
                timeout=120000,
            )
        except Exception as e:
            print(f"[警告] 页面加载异常: {e}")

        # 轮询等待登录
        print("\n  等待登录中... (请在浏览器窗口中操作)")
        elapsed = 0
        while elapsed < LOGIN_TIMEOUT:
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

            # 方式 1: API 响应已确认登录
            if login_confirmed:
                break

            # 方式 2: 检查 cookies 中是否出现真正的认证 cookie
            try:
                cookies = await context.cookies()
                if _has_auth_cookie(cookies):
                    auth_names = [c["name"] for c in cookies if _has_auth_cookie([c])]
                    print(f"\n  [检测] 发现认证 cookies: {auth_names}")
                    # 再等一个周期确认稳定
                    await asyncio.sleep(POLL_INTERVAL)
                    login_confirmed = True
                    break
            except Exception:
                pass

            # 进度提示 (每 30 秒)
            if elapsed % 30 == 0:
                print(f"  ... 已等待 {elapsed}s")

        if not login_confirmed:
            print(f"\n  [超时] {LOGIN_TIMEOUT // 60} 分钟内未检测到登录")
            await browser.close()
            return 1

        # 登录成功，等几秒让状态稳定
        print("\n  登录成功! 等待状态稳定...")
        await asyncio.sleep(5)

        # 保存完整状态 (cookies + localStorage)
        state = await context.storage_state()

        # 提取 localStorage 中的认证相关信息 (仅用于日志展示)
        local_storage = await page.evaluate("""() => {
            const result = {};
            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                result[key] = localStorage.getItem(key);
            }
            return result;
        }""")

        # 展示认证信息摘要
        print(f"\n  Cookies 总数: {len(state.get('cookies', []))}")
        for c in state.get("cookies", []):
            if any(x in c["name"].lower()
                   for x in ["uid", "token", "secret", "access", "user", "csrf"]):
                print(f"    [认证] {c['name']} = {c['value'][:50]}...")

        print(f"  localStorage 总键数: {len(local_storage)}")
        auth_keys = [k for k in local_storage
                     if any(x in k.lower() for x in ["token", "user", "auth", "uid"])]
        for k in auth_keys[:10]:
            v = local_storage[k] or ""
            print(f"    [认证] {k} = {v[:60]}{'...' if len(v) > 60 else ''}")

        # 保存 storageState 格式 (test_scraper.py 直接可用)
        output_file = "coinglass_auth.json"
        Path(output_file).write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\n  [已保存] 完整浏览器状态 -> {output_file}")

        # 验证: 加载 ETH 页面检查热力图渲染
        print("\n  [验证] 加载 ETH 页面检查登录是否生效...")
        try:
            await page.goto(
                "https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=ETH",
                wait_until="domcontentloaded",
                timeout=120000,
            )
            await asyncio.sleep(20)  # 等待足够长，避免 SPA 渲染中 evaluate 失败
            check = None
            for attempt in range(3):
                try:
                    check = await page.evaluate("""() => ({
                        has_login_text: document.body.innerText.includes('Sign in') || document.body.innerText.includes('Log in'),
                        canvas_count: document.querySelectorAll('canvas').length,
                    })""")
                    break
                except Exception:
                    await asyncio.sleep(5)  # 页面导航中，稍后重试
            if check is None:
                raise RuntimeError("无法获取页面状态 (多次重试失败)")
            print(f"    canvas 数量: {check['canvas_count']} (热力图 {'已渲染' if check['canvas_count'] > 0 else '未渲染'})")

            if check["canvas_count"] > 0:
                print(f"\n  >> 验证成功! ETH 热力图已渲染")
                print(f"\n  后续抓取 ETH 多空清算数据:")
                print(f"     py test_scraper.py --test frequency --runs 1 --symbols ETH --cookies {output_file}")
            else:
                print(f"\n  >> ETH 热力图未渲染，登录可能未完全生效")
                print(f"     可重新运行本脚本再次登录")
        except Exception as e:
            print(f"    验证失败: {e}")

        await browser.close()

    print("\n" + "=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
