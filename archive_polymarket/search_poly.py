import requests
import json

proxies = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}

# 1. 尝试搜索 Ethereum 相关的 active events
for q in ["Ethereum", "ETH", "Up", "crypto"]:
    url = f"https://gamma-api.polymarket.com/events?closed=false&q={q}&limit=10"
    try:
        r = requests.get(url, proxies=proxies, timeout=8).json()
        print(f"=== Query '{q}': {len(r)} events ===")
        for ev in r[:3]:
            print("  Title:", ev.get("title"), "| slug:", ev.get("slug"))
            for m in ev.get("markets", [])[:2]:
                print("    Market:", m.get("question"))
                print("    clobTokenIds:", m.get("clobTokenIds"))
    except Exception as e:
        print("Err:", e)
