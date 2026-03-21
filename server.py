#!/usr/bin/env python3
"""China Indicators Dashboard - Data Server
Proxies FRED, World Bank, GDELT, UN Comtrade APIs with caching.
"""

import http.server
import urllib.parse
import subprocess
import json
import os
import time
import csv
import io
from datetime import datetime, timedelta

PORT = 8899

_cache = {}
CACHE_TTL = {
    "fred": 300,
    "worldbank": 3600,
    "gdelt": 3600,
    "comtrade": 86400,
    "signals": 1800,
}


def curl_text(url, max_time=20):
    r = subprocess.run(
        ["curl", "-s", "-L", "--http1.1", "--compressed",
         "--max-time", str(max_time), url],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"curl exit {r.returncode}: {r.stderr.strip()}")
    if not r.stdout.strip():
        raise RuntimeError("Empty response")
    return r.stdout


def curl_json(url, max_time=20):
    return json.loads(curl_text(url, max_time))


def get_cached(key, ttl_cat, fetcher):
    now = time.time()
    ttl = CACHE_TTL.get(ttl_cat, 300)
    if key in _cache and now - _cache[key]["ts"] < ttl:
        return _cache[key]["data"]
    data = fetcher()
    _cache[key] = {"ts": now, "data": data}
    return data


# ---- FRED ----
def fetch_fred(series_id, years=10):
    def _f():
        end = datetime.now()
        start = end - timedelta(days=365 * years + 30)
        url = (
            f"https://fred.stlouisfed.org/graph/fredgraph.csv"
            f"?id={series_id}"
            f"&cosd={start.strftime('%Y-%m-%d')}"
            f"&coed={end.strftime('%Y-%m-%d')}"
        )
        txt = None
        for attempt in range(3):
            try:
                txt = curl_text(url)
                break
            except Exception:
                if attempt < 2:
                    time.sleep(2)
        if txt is None:
            raise RuntimeError(f"FRED {series_id} fetch failed")
        data = []
        for line in txt.strip().split("\n")[1:]:
            parts = line.split(",")
            if len(parts) >= 2:
                v = parts[1].strip()
                if v and v != ".":
                    try:
                        data.append({"date": parts[0].strip(), "value": float(v)})
                    except ValueError:
                        pass
        return data
    return get_cached(f"fred_{series_id}_{years}", "fred", _f)


# ---- World Bank ----
def fetch_worldbank(indicator_id, start_year=2000, end_year=2026):
    def _f():
        url = (
            f"https://api.worldbank.org/v2/country/CHN/indicator/{indicator_id}"
            f"?format=json&per_page=50&date={start_year}:{end_year}"
        )
        raw = curl_json(url)
        if not isinstance(raw, list) or len(raw) < 2:
            return []
        data = []
        for item in raw[1] or []:
            if item.get("value") is not None:
                data.append({"date": item["date"], "value": item["value"]})
        data.sort(key=lambda x: x["date"])
        return data
    return get_cached(f"wb_{indicator_id}", "worldbank", _f)


# ---- GDELT ----
def fetch_gdelt(query, months=24):
    def _f():
        end = datetime.now()
        start = end - timedelta(days=30 * months)
        encoded = urllib.parse.quote(query)
        url = (
            f"https://api.gdeltproject.org/api/v2/doc/doc"
            f"?query={encoded}&mode=timelinevol&format=csv"
            f"&startdatetime={start.strftime('%Y%m%d')}000000"
            f"&enddatetime={end.strftime('%Y%m%d')}235959"
        )
        # Retry with backoff for GDELT rate limiting
        txt = None
        for attempt in range(3):
            try:
                txt = curl_text(url, 30)
                if "Please limit requests" in txt:
                    time.sleep(6 * (attempt + 1))
                    txt = None
                    continue
                break
            except Exception:
                if attempt < 2:
                    time.sleep(6)
        if not txt or "Please limit requests" in txt:
            return []
        data = []
        for row in csv.reader(io.StringIO(txt)):
            if len(row) >= 3:
                try:
                    data.append({"date": row[0].strip(), "value": float(row[2].strip())})
                except (ValueError, IndexError):
                    pass
            elif len(row) == 2:
                try:
                    data.append({"date": row[0].strip(), "value": float(row[1].strip())})
                except (ValueError, IndexError):
                    pass
        return data
    return get_cached(f"gdelt_{query}_{months}", "gdelt", _f)


# ---- UN Comtrade ----
COMTRADE_NAMES = {
    842: "美国", 344: "香港", 392: "日本", 410: "韩国", 704: "越南",
    528: "荷兰", 276: "德国", 826: "英国", 356: "印度", 458: "马来西亚",
    764: "泰国", 702: "新加坡", 36: "澳大利亚", 360: "印尼", 643: "俄罗斯",
    608: "菲律宾", 484: "墨西哥", 124: "加拿大", 76: "巴西", 682: "沙特",
    784: "阿联酋", 710: "南非", 792: "土耳其", 616: "波兰", 380: "意大利",
    250: "法国", 724: "西班牙", 158: "台湾",
}


def fetch_comtrade_hhi(period="2022"):
    def _f():
        url = (
            f"https://comtradeapi.un.org/public/v1/preview/C/A/HS"
            f"?reporterCode=156&period={period}&flowCode=X&cmdCode=TOTAL"
        )
        raw = curl_json(url, 30)
        rows = raw.get("data", [])
        total = sum(
            r.get("primaryValue", 0) for r in rows
            if r.get("partnerCode") and r["partnerCode"] != 0
        )
        if total == 0:
            return {"period": period, "hhi": None, "top_partners": []}
        shares = []
        for r in rows:
            pc = r.get("partnerCode")
            if pc and pc != 0:
                s = r.get("primaryValue", 0) / total
                name = COMTRADE_NAMES.get(pc, r.get("partnerDesc") or f"Code {pc}")
                shares.append({
                    "partner": name,
                    "value": r.get("primaryValue", 0),
                    "share": round(s, 6),
                })
        shares.sort(key=lambda x: -x["share"])
        hhi = sum(s["share"] ** 2 for s in shares)
        return {
            "period": period,
            "hhi": round(hhi, 6),
            "top_partners": shares[:10],
        }
    return get_cached(f"comtrade_{period}", "comtrade", _f)


# ---- Signals ----
def load_signal_config():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "signal_config.json")
    with open(p) as f:
        return json.load(f)


def compute_trend(values, window):
    if len(values) < 2:
        return 0
    recent = values[-window:]
    n = len(recent)
    xm = (n - 1) / 2
    ym = sum(recent) / n
    num = sum((i - xm) * (v - ym) for i, v in enumerate(recent))
    den = sum((i - xm) ** 2 for i in range(n))
    return num / den if den else 0


def eval_signal(value, trend, cfg):
    t = cfg.get("type", "higher_is_better")
    g, r = cfg["green"], cfg["red"]
    if t == "higher_is_better":
        level = "green" if value >= g else ("red" if value <= r else "yellow")
    else:
        level = "green" if value <= g else ("red" if value >= r else "yellow")
    noise = cfg.get("noise_threshold", 0.01)
    if abs(trend) < noise:
        direction = "stable"
    elif t == "higher_is_better":
        direction = "improving" if trend > 0 else "worsening"
    else:
        direction = "improving" if trend < 0 else "worsening"
    return {"level": level, "direction": direction, "value": round(value, 4), "trend": round(trend, 6)}


def gdelt_signal(data):
    values = [d["value"] for d in data if isinstance(d.get("value"), (int, float))]
    if not values:
        return {"level": "yellow", "direction": "stable", "value": 0}
    avg = sum(values) / len(values)
    # Use last 30 data points (~1 month of daily data) for recent average
    n = min(30, len(values))
    recent = sum(values[-n:]) / n
    ratio = recent / avg if avg > 0 else 1
    return {
        "level": "green" if ratio <= 0.8 else ("red" if ratio >= 1.5 else "yellow"),
        "direction": "improving" if ratio < 1 else ("worsening" if ratio > 1.1 else "stable"),
        "value": round(recent, 6),
        "baseline": round(avg, 6),
        "ratio": round(ratio, 2),
    }


def compute_all_signals():
    def _compute():
        cfg = load_signal_config()
        signals = {}

        # Helper for WB indicators
        def wb_signal(key, indicator, window=3):
            try:
                data = fetch_worldbank(indicator)
                vals = [d["value"] for d in data]
                if vals:
                    sig = eval_signal(vals[-1], compute_trend(vals, window), cfg[key])
                    sig["latest_date"] = data[-1]["date"]
                    signals[key] = sig
            except Exception as e:
                signals[key] = {"error": str(e)}

        # Helper for FRED indicators
        def fred_signal(key, series, years=10, window=3, transform=None):
            try:
                data = fetch_fred(series, years)
                vals = [d["value"] for d in data]
                if transform:
                    vals = [transform(v) for v in vals]
                if vals:
                    sig = eval_signal(vals[-1], compute_trend(vals, window), cfg[key])
                    sig["latest_date"] = data[-1]["date"]
                    signals[key] = sig
            except Exception as e:
                signals[key] = {"error": str(e)}

        # Layer 1
        wb_signal("consumption_gdp", "NE.CON.PRVT.ZS")
        wb_signal("social_security", "SH.XPD.GHED.GE.ZS")
        fred_signal("oecd_cli", "BSCICP03CNM665S", 5, 6)

        # Layer 2
        fred_signal("youth_unemployment", "SLUEM1524ZSCHN", 15, 3)
        wb_signal("savings_rate", "NY.GNS.ICTR.ZS")
        wb_signal("urbanization", "SP.URB.TOTL.IN.ZS")

        # Layer 3
        fred_signal("fx_reserves", "TRESEGCNM052N", 10, 6, lambda v: v / 1e6)
        try:
            data = fetch_gdelt("censorship china internet", 24)
            signals["info_space"] = gdelt_signal(data)
        except Exception as e:
            signals["info_space"] = {"error": str(e)}
        wb_signal("nontax_revenue", "GC.REV.GOTR.ZS")

        # Layer 4 (delay between GDELT calls to avoid rate limit)
        time.sleep(6)
        try:
            data = fetch_gdelt('"china" "united states" military', 24)
            signals["military"] = gdelt_signal(data)
        except Exception as e:
            signals["military"] = {"error": str(e)}

        try:
            hhi = fetch_comtrade_hhi("2022")
            if hhi.get("hhi") is not None:
                sig = eval_signal(hhi["hhi"], 0, cfg["trade_hhi"])
                sig["latest_date"] = hhi["period"]
                signals["trade_hhi"] = sig
        except Exception as e:
            signals["trade_hhi"] = {"error": str(e)}

        fred_signal("exchange_rate", "DEXCHUS", 5, 60)

        # Summary
        gc = sum(1 for s in signals.values() if s.get("level") == "green")
        rc = sum(1 for s in signals.values() if s.get("level") == "red")
        total = sum(1 for s in signals.values() if "level" in s)
        overall = "reform_convergence" if gc >= 7 else ("stress_convergence" if gc <= 3 else "mixed")

        return {
            "signals": signals,
            "summary": {"overall": overall, "green": gc, "yellow": total - gc - rc, "red": rc, "total": total},
            "computed_at": datetime.now().isoformat(),
        }

    return get_cached("all_signals", "signals", _compute)


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        p = urllib.parse.urlparse(self.path)
        if p.path.startswith("/api/fred/"):
            self._api(self._fred, p)
        elif p.path.startswith("/api/worldbank/"):
            self._api(self._worldbank, p)
        elif p.path == "/api/gdelt":
            self._api(self._gdelt, p)
        elif p.path == "/api/comtrade":
            self._api(self._comtrade, p)
        elif p.path == "/api/signals":
            self._api(self._signals, p)
        else:
            super().do_GET()

    def _api(self, handler, parsed):
        try:
            data, ttl = handler(parsed)
            body = json.dumps(data, ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", f"max-age={ttl}")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            body = json.dumps({"error": str(e)}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

    def _fred(self, p):
        sid = p.path.replace("/api/fred/", "").strip("/")
        q = urllib.parse.parse_qs(p.query)
        years = int(q.get("years", ["10"])[0])
        return fetch_fred(sid, years), 300

    def _worldbank(self, p):
        ind = p.path.replace("/api/worldbank/", "").strip("/")
        q = urllib.parse.parse_qs(p.query)
        s = int(q.get("start", ["2000"])[0])
        e = int(q.get("end", ["2026"])[0])
        return fetch_worldbank(ind, s, e), 3600

    def _gdelt(self, p):
        q = urllib.parse.parse_qs(p.query)
        query = q.get("query", [""])[0]
        months = int(q.get("months", ["24"])[0])
        return fetch_gdelt(query, months), 3600

    def _comtrade(self, p):
        q = urllib.parse.parse_qs(p.query)
        period = q.get("period", ["2022"])[0]
        return fetch_comtrade_hhi(period), 86400

    def _signals(self, p):
        return compute_all_signals(), 1800

    def log_message(self, fmt, *args):
        if args and "/api/" in str(args[0]):
            super().log_message(fmt, *args)


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    http.server.ThreadingHTTPServer.allow_reuse_address = True
    srv = http.server.ThreadingHTTPServer(("localhost", PORT), Handler)
    print(f"\n  China Indicators Dashboard")
    print(f"  http://localhost:{PORT}\n")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopped")
        srv.server_close()
