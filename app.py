# ============================
#  OIL ANALYZER v7 (GPT-4o-mini)
# ============================
#  Источники: EIA, Baker Hughes, CFTC, FRED, Yahoo Finance
#  Команды: /eia /baker /cot /macro /prices /summary /help
# ============================

import os, sys, json, requests, yfinance as yf
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request
from openai import OpenAI
import concurrent.futures

# ====== ENVIRONMENT ======
EIA_API_KEY = os.getenv("EIA_API_KEY", "")
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
REQUEST_TIMEOUT = 20
CACHE = {}
app = Flask(__name__)

# ====== HELPERS ======
def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def http_get(url):
    return requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "oil-analyzer/3.0"})

def get_cache(key):
    entry = CACHE.get(key)
    if not entry:
        return None
    if datetime.now(timezone.utc) > entry["ts"] + timedelta(seconds=entry["ttl"]):
        return None
    return entry["data"]

def set_cache(key, data, ttl):
    CACHE[key] = {"ts": datetime.now(timezone.utc), "ttl": ttl, "data": data}

# ====== TELEGRAM ======
def send_telegram(text, chat_id=None):
    if not TELEGRAM_BOT_TOKEN:
        return False
    chat_id = chat_id or TELEGRAM_CHAT_ID
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.ok
    except Exception:
        return False

# ====== DATA SOURCES ======
def get_eia():
    if not EIA_API_KEY:
        return {"error": "EIA_API_KEY missing"}
    cached = get_cache("eia")
    if cached: return cached
    try:
        url = f"https://api.eia.gov/v2/petroleum/sum/sndw/data/?api_key={EIA_API_KEY}&frequency=weekly&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&length=1"
        rec = http_get(url).json()["response"]["data"][0]
        data = {"period": rec["period"], "product": rec["product-name"], "value": rec["value"], "units": rec["units"], "area": rec["area-name"]}
        set_cache("eia", data, 21600)
        return data
    except Exception as e:
        return {"error": f"eia: {e}"}

def get_baker():
    cached = get_cache("baker")
    if cached: return cached
    try:
        html = http_get("https://rigcount.bakerhughes.com/").text
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        frag = text[text.find("U.S."):text.find("U.S.") + 200]
        data = {"snippet": frag}
        set_cache("baker", data, 86400)
        return data
    except Exception as e:
        return {"error": f"baker: {e}"}

def get_cftc():
    cached = get_cache("cot")
    if cached: return cached
    try:
        urls = [
            "https://www.cftc.gov/dea/futures/petroleum_lf.htm",
            "https://www.cftc.gov/dea/options/petroleum_lof.htm"
        ]
        snippets = []
        for u in urls:
            html = http_get(u).text
            s = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
            i = s.find("Crude Oil")
            if i > 0: snippets.append(s[i-150:i+250])
        data = {"snippet": "\n\n".join(snippets)[:500]}
        set_cache("cot", data, 86400)
        return data
    except Exception as e:
        return {"error": f"cftc: {e}"}

def get_fred():
    if not FRED_API_KEY:
        return {"error": "FRED_API_KEY missing"}
    cached = get_cache("fred")
    if cached: return cached
    try:
        cpi = http_get(f"https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&api_key={FRED_API_KEY}&file_type=json").json()["observations"][-1]
        rate = http_get(f"https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key={FRED_API_KEY}&file_type=json").json()["observations"][-1]
        data = {"CPI": float(cpi["value"]), "FedRate": float(rate["value"])}
        set_cache("fred", data, 43200)
        return data
    except Exception as e:
        return {"error": f"fred: {e}"}

def get_prices():
    cached = get_cache("prices")
    out = {"WTI": None, "DXY": None}
    try:
        def _fetch(sym):
            u = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=2d&interval=1h"
            js = http_get(u).json()
            c = js["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            c = [x for x in c if x]
            return c[-1], c[-2]
        w, w_prev = _fetch("CL=F")
        d, d_prev = _fetch("^DXY")
        out["WTI"] = round(w, 2)
        out["DXY"] = round(d, 2)
        out["WTI_change"] = round((w - w_prev) / w_prev * 100, 2)
        out["DXY_change"] = round((d - d_prev) / d_prev * 100, 2)
    except Exception as e:
        out["error"] = f"prices: {e}"
        if cached: return cached
    set_cache("prices", out, 600)
    return out
    # ============================
#   AI, FORMATTING & ROUTES
# ============================

# ====== GPT (4o-mini) ======
def gpt_analyze(payload, source=None):
    """
    GPT-анализатор (v2) — с торговыми рекомендациями.
    Форматирует текст в Markdown для Telegram.
    """
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        system_prompt = (
            "Ты опытный аналитик нефтяного рынка. "
            "Проанализируй предоставленные данные из JSON (EIA, Baker Hughes, CFTC, FRED, Yahoo). "
            "Выяви ключевые бычьи и медвежьи факторы, затем сформулируй торговую рекомендацию: "
            "BUY (лонг), SELL (шорт) или NEUTRAL. "
            "Укажи текущую цену WTI, цель и стоп, если данные позволяют.\n\n"
            "Используй структуру Markdown для Telegram с эмодзи и блоками:\n"
            "🔴 **EIA Oil Report Analysis**\n"
            "🎯 BUY / SELL / NEUTRAL\n"
            "💰 Цена WTI: $...\n"
            "🎯 Цель: ...\n"
            "⛔ Стоп: ...\n\n"
            "📊 Факторы:\n"
            "- 🔴 Медвежий фактор\n"
            "- 🟢 Бычий фактор\n\n"
            "📈 Итог: короткое резюме (2–3 предложения)\n"
            "⏰ Вход: диапазон времени (по UTC или CT)\n\n"
            "Добавь внизу подпись: 🤖 *EIA Oil Analyzer*"
        )

        # --- Выбор режима ---
        if source:
            user_prompt = f"Анализируй только источник: {source}\n\n" + json.dumps(payload.get(source, {}), ensure_ascii=False, indent=2)
        else:
            user_prompt = "Проанализируй все источники и сделай полный торговый отчёт:\n\n" + json.dumps(payload, ensure_ascii=False, indent=2)

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.35,
        )

        text = resp.choices[0].message.content.strip()

        # --- Добавим дату и сигнатуру ---
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        footer = f"\n\n🕒 *Generated automatically at {timestamp}*\n🤖 *EIA Oil Analyzer*"

        return text + footer

    except Exception as e:
        return f"GPT error: {e}"

# ====== formatting helpers ======
def _fmt_num(x, nd=2):
    try:
        return f"{float(x):,.{nd}f}"
    except Exception:
        return "N/A"

def _fmt_pct(x, nd=2):
    try:
        return f"{float(x):+.{nd}f}%"
    except Exception:
        return f"{0:+.{nd}f}%"

# ====== per-block mini-cards ======
def block_eia(e):
    if not e or "error" in e:
        return "📦 <b>EIA:</b> данные недоступны"
    return (
        "📦 <b>EIA</b>\n"
        f"• Period: <b>{e.get('period','N/A')}</b>\n"
        f"• Region: <b>{e.get('area','N/A')}</b>\n"
        f"• Product: <b>{e.get('product','N/A')}</b>\n"
        f"• Value: <b>{_fmt_num(e.get('value'))} {e.get('units','')}</b>"
    )

def block_baker(b):
    if not b or "error" in b:
        return "🏗️ <b>Baker Hughes:</b> нет данных"
    sn = b.get("snippet","")[:280]
    return f"🏗️ <b>Baker Hughes</b>\n<code>{sn}</code>"

def block_cftc(c):
    if not c or "error" in c:
        return "📊 <b>CFTC:</b> нет данных"
    sn = c.get("snippet","")[:280]
    return f"📊 <b>CFTC</b>\n<code>{sn}</code>"

def block_macro(f):
    if not f or "error" in f:
        return "🏦 <b>Macro:</b> нет данных"
    return (
        "🏦 <b>Macro (FRED)</b>\n"
        f"• CPI: <b>{_fmt_num(f.get('CPI'))}</b>\n"
        f"• Fed Funds: <b>{_fmt_num(f.get('FedRate'))}%</b>"
    )

def block_prices(p):
    p = p or {}
    wti = _fmt_num(p.get("WTI"))
    dxy = _fmt_num(p.get("DXY"))
    wchg = _fmt_pct(p.get("WTI_change"))
    dchg = _fmt_pct(p.get("DXY_change"))
    return (
        "💹 <b>Market</b>\n"
        f"🛢 WTI: <b>${wti}</b> (24h {wchg})\n"
        f"💵 DXY: <b>{dxy}</b> (24h {dchg})"
    )

# ====== final summary message ======
def format_summary_msg(payload: dict, analysis: str | None = None) -> str:
    e = payload.get("eia")
    b = payload.get("baker")
    c = payload.get("cot")
    f = payload.get("fred")
    p = payload.get("prices")

    parts = [
        "🧾 <b>Oil Report: SUMMARY</b>",
        f"🕒 {utc_now()}",
        block_eia(e),
        block_baker(b),
        block_cftc(c),
        block_macro(f),
        block_prices(p),
    ]
    if analysis:
        parts.append("🧠 <b>AI Analysis</b>\n" + analysis)
    return "\n\n".join([x for x in parts if x])

def format_prices_only(p: dict) -> str:
    return "💹 <b>DXY & WTI update</b>\n" + f"🕒 {utc_now()}\n\n" + block_prices(p)

# ====== DATA COLLECTION (parallel) ======
def collect(mode: str) -> dict:
    mode = (mode or "summary").lower()
    data = {"mode": mode, "timestamp": utc_now()}

    def _prices(): return ("prices", get_prices())
    def _eia():    return ("eia", get_eia())
    def _baker():  return ("baker", get_baker())
    def _cot():    return ("cot", get_cftc())
    def _fred():   return ("fred", get_fred())

    tasks = []
    if mode in ("summary", "prices"): tasks.append(_prices)
    if mode in ("summary", "eia"):    tasks.append(_eia)
    if mode in ("summary", "baker"):  tasks.append(_baker)
    if mode in ("summary", "cot"):    tasks.append(_cot)
    if mode in ("summary", "macro"):  tasks.append(_fred)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futs = [ex.submit(fn) for fn in tasks]
        for f in concurrent.futures.as_completed(futs):
            k, v = f.result()
            data[k] = v
    return data

def run_once(mode: str, chat_id: str | None = None) -> dict:
    payload = collect(mode)
    analysis = None
    if mode == "summary":
        analysis = gpt_analyze(payload)
        msg = format_summary_msg(payload, analysis)
    elif mode == "prices":
        msg = format_prices_only(payload.get("prices", {}))
    elif mode == "eia":
        msg = block_eia(payload.get("eia"))
    elif mode == "baker":
        msg = block_baker(payload.get("baker"))
    elif mode == "cot":
        msg = block_cftc(payload.get("cot"))
    elif mode == "macro":
        msg = block_macro(payload.get("fred"))
    else:
        msg = format_summary_msg(payload, None)

    sent = send_telegram(msg, chat_id=chat_id)
    return {"ok": True, "sent": sent, "payload": payload, "analysis": analysis}

# ====== ROUTES ======
@app.route("/")
def index():
    return jsonify({"ok": True, "message": "Oil Analyzer Bot is running", "time": utc_now()})

@app.route("/health")
def health():
    return jsonify({"ok": True, "time": utc_now()})

@app.route("/data")
def data_endpoint():
    mode = request.args.get("mode", "summary")
    return jsonify(collect(mode))

@app.route("/analyze")
def analyze_endpoint():
    mode = request.args.get("mode", "summary")
    return jsonify(run_once(mode))

# ежедневный крон (под Render Scheduler)
@app.route("/cron/daily")
def cron_daily():
    res = run_once("summary", chat_id=TELEGRAM_CHAT_ID)
    return jsonify({"ok": True, "result": res})

# ====== Telegram webhook ======
@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    try:
        upd = request.get_json(force=True, silent=True) or {}
        msg = upd.get("message") or upd.get("edited_message") or {}
        chat_id = str(msg.get("chat", {}).get("id", "")) or TELEGRAM_CHAT_ID
        text = (msg.get("text") or "").strip().lower()

        if text in ("/start", "/help"):
            help_txt = (
                "🛢 <b>Oil Analyzer Bot</b>\n\n"
                "Команды:\n"
                "• /prices — WTI & DXY\n"
                "• /eia — последний Weekly EIA\n"
                "• /baker — Baker Hughes rig count\n"
                "• /cot — CFTC Disaggregated\n"
                "• /macro — CPI & Fed Funds\n"
                "• /summary — всё вместе + AI\n"
                "Автодайджест: ежедневно 14:00 UTC"
            )
            send_telegram(help_txt, chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/prices"):
            d = collect("prices")
            send_telegram(format_prices_only(d.get("prices", {})), chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/eia"):
            d = collect("eia")
            send_telegram(block_eia(d.get("eia")), chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/baker"):
            d = collect("baker")
            send_telegram(block_baker(d.get("baker")), chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/cot"):
            d = collect("cot")
            send_telegram(block_cftc(d.get("cot")), chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/macro"):
            d = collect("macro")
            send_telegram(block_macro(d.get("fred")), chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/summary"):
            res = run_once("summary", chat_id=chat_id)
            return jsonify({"ok": True, "result": res})

        send_telegram("Неизвестная команда. /help", chat_id=chat_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

# ====== DEV/CLI ======
if __name__ == "__main__":
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        print(json.dumps(run_once(mode), ensure_ascii=False, indent=2))
    else:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
