import os
import sys
import json
import time
from datetime import datetime, timezone
import requests
import yfinance as yf
from flask import Flask, jsonify, request
from openai import OpenAI

EIA_API_KEY = os.getenv("EIA_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

REQUEST_TIMEOUT = 20

app = Flask(__name__)

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def http_get(url):
    return requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "oil-analyzer/1.0"})

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

def get_cftc():
    try:
        url = "https://www.cftc.gov/dea/newcot/Crude_oil_fut.txt"
        txt = http_get(url).text
        lines = [ln for ln in txt.splitlines() if ln.strip()]
        target = [ln for ln in lines if "NYMEX" in ln.upper() or "CRUDE OIL" in ln.upper()]
        snippet = target[0][:160] if target else "N/A"
        return {"snippet": snippet, "source": "CFTC newcot crude_oil_fut"}
    except Exception as e:
        return {"error": f"cftc: {e}"}

def get_baker_hughes():
    try:
        url = "https://rigcount.bakerhughes.com/"
        html = http_get(url).text
        start = html.find("<table")
        end = html.find("</table>", start) + 8 if start != -1 else -1
        snippet = html[start:end][:300] if start != -1 and end != -1 else "N/A"
        return {"snippet": snippet, "source": "Baker Hughes main page"}
    except Exception as e:
        return {"error": f"rigs: {e}"}

def gpt_analyze(payload):
    if not OPENAI_API_KEY:
        return "GPT disabled: OPENAI_API_KEY not set."
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = f"""Ты опытный аналитик нефтяного рынка.
Суммируй ключевые факторы (бычьи/медвежьи), дай краткую рекомендацию (BUY/SELL/NEUTRAL)
и предложи цель/стоп на основе данных ниже. Форматируй кратко, без лишней воды.

Данные:
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты лаконичный, точный и прагматичный рыночный аналитик."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"GPT error: {e}"

def format_prices_msg(p):
    w = p.get("WTI"); wchg = p.get("WTI_change")
    d = p.get("DXY"); dchg = p.get("DXY_change")
    parts = ["💹 <b>DXY & WTI update</b>", f"🕒 {utc_now()}"]
    if w is not None: parts.append(f"🛢 WTI: <b>${w}</b>  (24h {wchg:+}%)")
    if d is not None: parts.append(f"💵 DXY: <b>{d}</b>  (24h {dchg:+}%)")
    return "\n".join(parts)

def format_generic_msg(title, payload, analysis=None):
    lines = [f"🧾 <b>{title}</b>", f"🕒 {utc_now()}"]

    # EIA
    if "eia" in payload:
        e = payload["eia"].get("raw", {})
        lines.append(
            f"<b>📅 Period:</b> {e.get('period', 'N/A')}\n"
            f"<b>📍 Region:</b> {e.get('area-name', 'N/A')}\n"
            f"<b>🛢 Product:</b> {e.get('product-name', 'N/A')}\n"
            f"<b>⚙️ Process:</b> {e.get('process-name', 'N/A')}\n"
            f"<b>📦 Value:</b> {e.get('value', 'N/A')} {e.get('units', '')}"
        )

    # CFTC
    if "cot" in payload:
        c = payload["cot"]
        if "snippet" in c:
            lines.append(f"\n📊 <b>CFTC data:</b>\n<code>{c['snippet']}</code>")

    # Baker Hughes
    if "rigs" in payload:
        r = payload["rigs"]
        if "snippet" in r:
            lines.append(f"\n🏗️ <b>Baker Hughes Rig Count:</b>\n<code>{r['snippet'][:300]}</code>")

    # Prices
    if "prices" in payload:
        p = payload["prices"]
        lines.append(
            f"\n💹 <b>Market snapshot:</b>\n"
            f"🛢 WTI: ${p.get('WTI', 'N/A')} ({p.get('WTI_change', 0):+}%)\n"
            f"💵 DXY: {p.get('DXY', 'N/A')} ({p.get('DXY_change', 0):+}%)"
        )

    # GPT analysis (AI)
    if analysis:
        lines.append("\n🧠 <b>AI Analysis</b>\n" + analysis)

    return "\n".join(lines)

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
    if mode == "api":
        data["api_weekly"] = {"note": "API Weekly: можно подключить скрейпер новостей при необходимости."}
    return data

def run_once(mode: str):
    payload = collect(mode)
    analysis = None
    if mode in ("summary", "eia", "cot", "rigs"):
        analysis = gpt_analyze(payload)
    if mode == "prices":
        msg = format_prices_msg(payload.get("prices", {}))
    else:
        title = f"Oil Report: {mode.upper()}"
        msg = format_generic_msg(title, payload, analysis)
    sent = send_telegram(msg)
    return {"ok": True, "sent": sent, "payload": payload, "analysis": analysis}

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
