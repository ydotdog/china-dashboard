#!/usr/bin/env python3
"""Pre-fetch all API data to warm the server cache.
Run via cron or manually: python3 scripts/fetch_all_data.py
"""

import urllib.request
import json
import time
import sys

SERVER = "http://localhost:8899"

ENDPOINTS = [
    # Layer 1
    ("/api/worldbank/NE.CON.PRVT.ZS", "居民消费/GDP"),
    ("/api/worldbank/SH.XPD.GHED.GE.ZS", "政府卫生支出"),
    ("/api/fred/BSCICP03CNM665S?years=10", "OECD CLI"),
    # Layer 2
    ("/api/fred/SLUEM1524ZSCHN?years=15", "青年失业率"),
    ("/api/worldbank/NY.GNS.ICTR.ZS", "储蓄率"),
    ("/api/worldbank/SP.URB.TOTL.IN.ZS", "城镇化率"),
    # Layer 3
    ("/api/fred/TRESEGCNM052N?years=10", "外汇储备"),
    ("/api/gdelt?query=censorship+china+internet&months=24", "信息空间(GDELT)"),
    ("/api/worldbank/GC.REV.GOTR.ZS", "非税收入"),
    # Layer 4
    ("/api/gdelt?query=%22china%22+%22united+states%22+military&months=24", "军事互动(GDELT)"),
    ("/api/comtrade?period=2022", "贸易多元化"),
    ("/api/fred/DEXCHUS?years=5", "汇率"),
    # Signals
    ("/api/signals", "信号计算"),
]


def fetch(path, label):
    url = SERVER + path
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
            if isinstance(data, list):
                print(f"  OK  {label:20s}  {len(data)} 数据点")
            elif isinstance(data, dict) and "error" in data:
                print(f"  ERR {label:20s}  {data['error']}")
            elif isinstance(data, dict) and "signals" in data:
                s = data["summary"]
                print(f"  OK  {label:20s}  G:{s['green']} Y:{s['yellow']} R:{s['red']}")
            else:
                print(f"  OK  {label:20s}  {str(data)[:60]}")
    except Exception as e:
        print(f"  ERR {label:20s}  {e}")


def main():
    print(f"Fetching all data from {SERVER}...\n")
    for path, label in ENDPOINTS:
        fetch(path, label)
        # Small delay for GDELT rate limiting
        if "gdelt" in path.lower():
            time.sleep(6)
    print("\nDone.")


if __name__ == "__main__":
    main()
