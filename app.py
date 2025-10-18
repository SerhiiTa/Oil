import os
import sys
import json
import time
from datetime import datetime, timezone
import requests
import yfinance as yf
from flask import Flask, jsonify, request
from openai import OpenAI
from bs4 import BeautifulSoup
import concurrent.futures

EIA_API_KEY = os.getenv("EIA_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

REQUEST_TIMEOUT = 20
app = Flask(__name__)

# ------------------------------
# Helpers
# ------------------------------

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def http_get(url):
    return requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "oil-analyzer/2.0"})

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.ok
    except Exception:
        return False

# ------------------------------
# Yahoo Finance (DXY + WTI)
# ------------------------------

def get_prices():
    out = {"WTI": None, "WTI_change": None, "DXY": None, "DXY_change": None, "source": "Yahoo Finance"}
    try:
        w = yf.download("CL=F", period="2d", interval="1h", progress=False)
        if len(w) > 0:
            wti = float(w["Close"].dropna().iloc[-1])
            w_prev_close = float(yf.Ticker("CL=F").history(period="2d")["Close"].dropna().iloc[-2])
            out["WTI"] = round(wti, 2)
            out["WTI_change"] = round((wti - w_prev_close) / w_prev_close * 100, 2)
        d = yf.download("DX-Y.NYB", period="2d", interval="1h", progress=False)
        if len(d) > 0:
            dxy = float(d["Close"].dropna().iloc[-1])
            d_prev_close = float(yf.Ticker("DX-Y.NYB").history(period="2d")["Close"].dropna().iloc[-2])
            out["DXY"] = round(dxy, 2)
            out["DXY_change"] = round((dxy - d_prev_close) / d_prev_close * 100, 2)
    except Exception as e:
        out["error"] = f"prices: {e}"
    return out

# ------------------------------
# EIA Weekly
# ------------------------------

def get_eia_weekly():
    if not EIA_API_KEY:
        return {"error": "EIA_API_KEY missing"}
    try:
        url = (
            "https://api.eia.gov/v2/petroleum/sum/sndw/data/"
            f"?api_key={EIA_API_KEY}&frequency=weekly&data[0]=value"
            "&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1"
        )
        js = http_get(url).json()
        rec = js["response"]["data"][0]
        return {"raw": rec, "period": rec.get("period")}
    except Exception as e:
        return {"error": f"eia: {e}"}

# ------------------------------
# CFTC Disaggregated Futures
# ------------------------------

def get_cftc():
    try:
        url = "https://www.cftc.gov/dea/newcot/Crude_oil_fut.txt"
        r = http_get(url)
        if not r or r.status_code != 200:
            return {"error": "CFTC offline or shutdown notice active."}
        txt = r.text
        lines = [ln for ln in txt.splitlines() if ln.strip()]
        target = [ln for ln in lines if "NYMEX" in ln.upper() or "CRUDE OIL" in ln.upper()]
        snippet = target[0][:160] if target else "No Crude Oil data found"
        return {"snippet": snippet, "source": "CFTC Disaggregated Futures"}
    except Exception as e:
        return {"error": f"cftc: {e}"}

# ------------------------------
# Baker Hughes Rig Count
# ------------------------------

def get_baker_hughes():
    try:
        # 1️⃣ Try RSS feed first
        rss = http_get("https://rigcount.bakerhughes.com/feed")
        if rss and rss.status_code == 200:
            soup = BeautifulSoup(rss.text, "xml")
            item = soup.find("item")
            if item:
                title = item.title.text
                pubdate = item.pubDate.text
                return {
                    "title": title,
                    "pubdate": pubdate,
                    "source": "Baker Hughes RSS",
                    "note": "RSS feed active, PDF temporarily unavailable"
                }

        # 2️⃣ Fallback: scrape table from main page
        html = http_get("https://rigcount.bakerhughes.com/")
        if not html:
            return {"error": "Baker Hughes site not reachable"}
        soup = BeautifulSoup(html.text, "html.parser")
        table = soup.find("table")
        snippet = table.get_text(" ", strip=True)[:400] if table else "No rig table found"
        return {"snippet": snippet, "source": "Baker Hughes main page"}

    except Exception as e:
        return {"error": f"rigs: {e}"}

# ------------------------------
# GPT Analysis
# ------------------------------

def gpt_analyze(payload):
    """Отправляет данные в OpenAI и получает краткий анализ (с таймаутом и fallback)."""
    if not OPENAI_API_KEY:
        return "⚠️ GPT disabled: OPENAI_API_KEY not set."

    def _call_openai():
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = f"""Ты опытный аналитик нефтяного рынка.
Суммируй ключевые факторы (бычьи/медвежьи), дай краткую рекомендацию (BUY/SELL/NEUTRAL)
и предложи цель/стоп на основе данных ниже. Форматируй чётко, лаконично и по сути.

Данные:
{json.dumps(payload, ensure_ascii=False, indent=2)}"""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты прагматичный аналитик рынка нефти."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()

    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(_call_openai)
            return future.result(timeout=15)
    except concurrent.futures.TimeoutError:
        return "⏳ GPT response timeout — using last known logic (NEUTRAL)"
    except Exception as e:
        print(f"[GPT ERROR] {e}", file=sys.stderr)
        return f"⚠️ GPT error: {e}"

# ------------------------------
# Message Formatting
# ------------------------------

def format_generic_msg(title, payload, analysis=None):
    lines = [f"🧾 <b>{title}</b>", f"🕒 {utc_now()}"]

    if "eia" in payload:
        e = payload["eia"].get("raw", {})
        lines.append(
            f"📅 Period: {e.get('period', 'N/A')}\n"
            f"📍 Region: {e.get('area-name', 'N/A')}\n"
            f"🛢 Product: {e.get('product-name', 'N/A')}\n"
            f"⚙️ Process: {e.get('process-name', 'N/A')}\n"
            f"📦 Value: {e.get('value', 'N/A')} {e.get('units', '')}"
        )

    if "cot" in payload:
        c = payload["cot"]
        if "snippet" in c:
            lines.append(f"\n📊 <b>CFTC:</b>\n<code>{c['snippet']}</code>")

    if "rigs" in payload:
        r = payload["rigs"]
        if "snippet" in r:
            lines.append(f"\n🏗️ <b>Baker Hughes Rig Count:</b>\n<code>{r['snippet']}</code>")
        elif "title" in r:
            lines.append(f"\n🏗️ <b>{r['title']}</b>\n🗓 {r.get('pubdate','')}")

    if "prices" in payload:
        p = payload["prices"]
        lines.append(
            f"\n💹 <b>Market Snapshot:</b>\n"
            f"🛢 WTI: ${p.get('WTI','N/A')} ({p.get('WTI_change',0):+}%)\n"
            f"💵 DXY: {p.get('DXY','N/A')} ({p.get('DXY_change',0):+}%)"
        )

    if analysis:
        lines.append(f"\n🧠 <b>AI Analysis</b>\n{analysis}")

    return "\n".join(lines)

# ------------------------------
# Main Logic
# ------------------------------

def collect(mode: str):
    mode = (mode or "summary").lower()
    data = {"timestamp": utc_now(), "mode": mode}
    if mode in ("prices", "summary"):
        data["prices"] = get_prices()
    if mode in ("eia", "summary"):
        data["eia"] = get_eia_weekly()
    if mode in ("cot", "summary"):
        data["cot"] = get_cftc()
    if mode in ("rigs", "summary"):
        data["rigs"] = get_baker_hughes()
    return data

def run_once(mode: str):
    """Основная функция: сбор данных, анализ, отправка в Telegram"""
    payload = collect(mode)
    analysis = None
    try:
        if mode in ("summary", "eia", "cot", "rigs"):
            analysis = gpt_analyze(payload)
        if mode == "prices":
            msg = format_prices_msg(payload.get("prices", {}))
        else:
            title = f"Oil Report: {mode.upper()}"
            msg = format_generic_msg(title, payload, analysis)
        sent = send_telegram(msg)
        print(f"[{utc_now()}] Sent: {mode} | Telegram status: {sent}")
        return {"ok": True, "sent": sent, "payload": payload, "analysis": analysis}
    except Exception as e:
        print(f"[ERROR] run_once({mode}): {e}", file=sys.stderr)
        return {"ok": False, "error": str(e)}

# ------------------------------
# Flask Endpoints
# ------------------------------

@app.route("/health")
def health():
    return jsonify({"ok": True, "time": utc_now()})

@app.route("/data")
def data():
    mode = request.args.get("mode", "summary")
    return jsonify(collect(mode))

@app.route("/analyze")
def analyze_endpoint():
    mode = request.args.get("mode", "summary")
    return jsonify(run_once(mode))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        result = run_once(mode)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
