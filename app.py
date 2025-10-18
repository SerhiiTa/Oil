import os
import sys
import json
import time
import concurrent.futures
from datetime import datetime, timezone
import requests
import yfinance as yf
from flask import Flask, jsonify, request
from openai import OpenAI
from bs4 import BeautifulSoup

# ENV
EIA_API_KEY = os.getenv("EIA_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

REQUEST_TIMEOUT = 10
app = Flask(__name__)

# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def http_get(url):
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "oil-analyzer/1.0"})
        r.raise_for_status()
        return r
    except Exception as e:
        return None

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
        return True
    except Exception:
        return False

# ----------------------------------------------------------------
# Data sources
# ----------------------------------------------------------------

def get_prices():
    out = {"WTI": None, "WTI_change": None, "DXY": None, "DXY_change": None}
    try:
        w = yf.download("CL=F", period="2d", interval="1h", progress=False)
        if not w.empty:
            wti = float(w["Close"].dropna().iloc[-1])
            w_prev = float(w["Close"].dropna().iloc[-2])
            out["WTI"] = round(wti, 2)
            out["WTI_change"] = round((wti - w_prev) / w_prev * 100, 2)
        d = yf.download("DX-Y.NYB", period="2d", interval="1h", progress=False)
        if not d.empty:
            dxy = float(d["Close"].dropna().iloc[-1])
            d_prev = float(d["Close"].dropna().iloc[-2])
            out["DXY"] = round(dxy, 2)
            out["DXY_change"] = round((dxy - d_prev) / d_prev * 100, 2)
    except Exception as e:
        out["error"] = str(e)
    return out

def get_eia_weekly():
    if not EIA_API_KEY:
        return {"error": "EIA_API_KEY missing"}
    try:
        url = f"https://api.eia.gov/v2/petroleum/sum/sndw/data/?api_key={EIA_API_KEY}&frequency=weekly&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1"
        js = http_get(url)
        if js:
            rec = js.json()["response"]["data"][0]
            return {"raw": rec, "period": rec.get("period")}
        return {"error": "no data"}
    except Exception as e:
        return {"error": str(e)}

def get_cftc():
    try:
        url = "https://www.cftc.gov/dea/newcot/Crude_oil_fut.txt"
        r = http_get(url)
        if not r:
            return {"error": "CFTC fetch failed"}
        lines = [ln for ln in r.text.splitlines() if "CRUDE" in ln.upper()]
        return {"snippet": lines[0] if lines else "N/A"}
    except Exception as e:
        return {"error": str(e)}

def get_baker_hughes():
    try:
        url = "https://rigcount.bakerhughes.com/"
        r = http_get(url)
        if not r:
            return {"error": "Baker Hughes fetch failed"}
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table:
            snippet = table.get_text(separator=" ", strip=True)[:300]
            return {"snippet": snippet}
        return {"error": "no table found"}
    except Exception as e:
        return {"error": str(e)}

# ----------------------------------------------------------------
# GPT Analyzer
# ----------------------------------------------------------------

def gpt_analyze(payload):
    if not OPENAI_API_KEY:
        return "GPT disabled"
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = f"""
Ты аналитик нефтяного рынка.
Проанализируй данные, укажи бычьи и медвежьи факторы, дай краткую рекомендацию BUY/SELL/NEUTRAL и диапазон целей/стопов.
Данные:
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Будь кратким и точным."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"GPT error: {e}"

# ----------------------------------------------------------------
# Formatting
# ----------------------------------------------------------------

def format_generic_msg(title, payload, analysis=None):
    lines = [f"🧾 <b>{title}</b>", f"🕒 {utc_now()}"]
    if "eia" in payload:
        e = payload["eia"].get("raw", {})
        lines.append(
            f"<b>📅 Period:</b> {e.get('period','N/A')}\n"
            f"<b>📍 Region:</b> {e.get('area-name','N/A')}\n"
            f"<b>🛢 Product:</b> {e.get('product-name','N/A')}\n"
            f"<b>📦 Value:</b> {e.get('value','N/A')} {e.get('units','')}"
        )
    if "cot" in payload:
        c = payload["cot"]
        if "snippet" in c:
            lines.append(f"\n📊 <b>CFTC:</b>\n<code>{c['snippet']}</code>")
    if "rigs" in payload:
        r = payload["rigs"]
        if "snippet" in r:
            lines.append(f"\n🏗️ <b>Baker Hughes:</b>\n<code>{r['snippet']}</code>")
    if "prices" in payload:
        p = payload["prices"]
        lines.append(
            f"\n💹 <b>Market:</b>\n"
            f"🛢 WTI: ${p.get('WTI','N/A')} ({p.get('WTI_change',0):+}%)\n"
            f"💵 DXY: {p.get('DXY','N/A')} ({p.get('DXY_change',0):+}%)"
        )
    if analysis:
        lines.append("\n🧠 <b>AI Analysis</b>\n" + analysis)
    return "\n".join(lines)

# ----------------------------------------------------------------
# Main logic
# ----------------------------------------------------------------

def collect(mode):
    data = {"timestamp": utc_now(), "mode": mode}
    with concurrent.futures.ThreadPoolExecutor() as ex:
        futures = {}
        if mode in ("prices", "summary"):
            futures["prices"] = ex.submit(get_prices)
        if mode in ("eia", "summary"):
            futures["eia"] = ex.submit(get_eia_weekly)
        if mode in ("cot", "summary"):
            futures["cot"] = ex.submit(get_cftc)
        if mode in ("rigs", "summary"):
            futures["rigs"] = ex.submit(get_baker_hughes)

        for k, f in futures.items():
            try:
                data[k] = f.result(timeout=15)
            except Exception as e:
                data[k] = {"error": str(e)}
    return data

def run_once(mode):
    payload = collect(mode)
    analysis = gpt_analyze(payload) if mode != "prices" else None
    msg = format_generic_msg(f"Oil Report: {mode.upper()}", payload, analysis)
    send_telegram(msg)
    return {"ok": True, "payload": payload}

# ----------------------------------------------------------------
# Flask routes
# ----------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"ok": True, "time": utc_now()})

@app.route("/analyze")
def analyze_endpoint():
    mode = request.args.get("mode", "summary")
    return jsonify(run_once(mode))

# ----------------------------------------------------------------
# ============================================================
# Telegram webhook handler (ручной триггер через /oil_summary)
# ============================================================

@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    try:
        data = request.get_json()
        msg = data.get("message", {}).get("text", "")
        chat_id = data.get("message", {}).get("chat", {}).get("id")

        if msg.strip() == "/oil_summary":
            result = run_once("summary")
            send_telegram("🛢 Manual oil summary triggered via Telegram.")
            return jsonify({"ok": True, "result": result})

        return jsonify({"ok": True, "ignored": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
