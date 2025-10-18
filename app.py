import os
import sys
import json
from datetime import datetime, timezone, timedelta
import requests
import yfinance as yf
from flask import Flask, jsonify, request
from openai import OpenAI
from bs4 import BeautifulSoup
import concurrent.futures

# ====== ENV ======
EIA_API_KEY        = os.getenv("EIA_API_KEY", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")  # канал/чат для авто-отчётов

REQUEST_TIMEOUT = 20

# ====== APP ======
app = Flask(__name__)

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def http_get(url):
    return requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "oil-analyzer/1.0"})

def send_telegram(text, chat_id=None):
    """Отправка в Telegram (HTML)"""
    if not TELEGRAM_BOT_TOKEN:
        return False
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.ok
    except Exception:
        return False

# ====== КЭШ (простая память в процессе) ======
# key -> {"ts": datetime, "ttl": seconds, "data": any}
CACHE = {}

def get_cache(key):
    obj = CACHE.get(key)
    if not obj:
        return None
    if datetime.now(timezone.utc) > obj["ts"] + timedelta(seconds=obj["ttl"]):
        return None
    return obj["data"]

def set_cache(key, data, ttl_sec):
    CACHE[key] = {"ts": datetime.now(timezone.utc), "ttl": ttl_sec, "data": data}

# ====== Источники ======
def get_prices():
    """WTI & DXY с кэшем 10 минут"""
    cached = get_cache("prices")
    if cached: 
        return cached
    out = {"WTI": None, "WTI_change": None, "DXY": None, "DXY_change": None, "source": "Yahoo Finance"}
    try:
        # WTI
        w = yf.download("CL=F", period="2d", interval="1h", progress=False)
        if len(w) > 0:
            wti = float(w["Close"].dropna().iloc[-1])
            w_prev_close = float(yf.Ticker("CL=F").history(period="2d")["Close"].dropna().iloc[-2])
            out["WTI"] = round(wti, 2)
            out["WTI_change"] = round((wti - w_prev_close) / w_prev_close * 100, 2)
        # DXY
        d = yf.download("DX-Y.NYB", period="2d", interval="1h", progress=False)
        if len(d) > 0:
            dxy = float(d["Close"].dropna().iloc[-1])
            d_prev_close = float(yf.Ticker("DX-Y.NYB").history(period="2d")["Close"].dropna().iloc[-2])
            out["DXY"] = round(dxy, 2)
            out["DXY_change"] = round((dxy - d_prev_close) / d_prev_close * 100, 2)
    except Exception as e:
        out["error"] = f"prices: {e}"
    set_cache("prices", out, ttl_sec=600)  # 10 минут
    return out

def get_eia_weekly():
    """EIA weekly — кэш 6 часов (в неделю достаточно)"""
    if not EIA_API_KEY:
        return {"error": "EIA_API_KEY missing"}
    cached = get_cache("eia_weekly")
    if cached:
        return cached
    try:
        url = (
            "https://api.eia.gov/v2/petroleum/sum/sndw/data/"
            f"?api_key={EIA_API_KEY}&frequency=weekly&data[0]=value"
            "&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1"
        )
        js = http_get(url).json()
        rec = js["response"]["data"][0]
        data = {"raw": rec, "period": rec.get("period")}
        set_cache("eia_weekly", data, ttl_sec=21600)  # 6h
        return data
    except Exception as e:
        return {"error": f"eia: {e}"}

def get_cftc_disagg_snippet():
    """CFTC: Disaggregated Futures-only CRUDE OIL (парсим страницу и берём короткий сниппет).
       Кэш 24 часа (обновляется раз в неделю)."""
    cached = get_cache("cftc_disagg")
    if cached:
        return cached
    try:
        # главная страница COT c интерлинками на long/short format
        index_url = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm"
        html = http_get(index_url).text
        # В кач-ве простого фоллбэка — ищем слово Petroleum на странице и рядом ссылки
        # (CFTC часто меняет URL, поэтому тут «мягкий» парсер)
        soup = BeautifulSoup(html, "html.parser")
        txt = soup.get_text(" ", strip=True)
        snippet = "CFTC: see Disaggregated Petroleum table on site."
        if "Petroleum" in txt:
            i = txt.find("Petroleum")
            snippet = txt[max(0, i - 160): i + 200]
        data = {"snippet": snippet, "source": "CFTC (disaggregated, page snippet)"}
    except Exception as e:
        data = {"error": f"cftc: {e}"}
    set_cache("cftc_disagg", data, ttl_sec=86400)  # 24h
    return data

def get_baker_hughes():
    """Baker Hughes rig count (страница). Кэш 24 часа."""
    cached = get_cache("bhi")
    if cached:
        return cached
    try:
        url = "https://rigcount.bakerhughes.com/"
        html = http_get(url).text
        soup = BeautifulSoup(html, "html.parser")
        # Ищем блок с таблицей «U.S. / Canada / International ...»
        table_text = soup.get_text(" ", strip=True)
        # вырезаем короткий аккуратный фрагмент для телеграм
        key_words = ["U.S.", "Canada", "International"]
        frag = None
        for kw in key_words:
            if kw in table_text:
                i = table_text.find(kw)
                frag = table_text[max(0, i-60): i+220]
                break
        snippet = (frag or table_text[:280]).strip()
        data = {"snippet": snippet, "source": "Baker Hughes (page snippet)"}
    except Exception as e:
        data = {"error": f"rigs: {e}"}
    set_cache("bhi", data, ttl_sec=86400)
    return data

# ====== GPT ======
def gpt_analyze(payload):
    if not OPENAI_API_KEY:
        return "GPT disabled: OPENAI_API_KEY not set."
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = f"""Ты опытный аналитик нефтяного рынка.
Суммируй ключевые факторы (бычьи/медвежьи), дай краткую рекомендацию (BUY/SELL/NEUTRAL),
укажи целевой диапазон (ближайшие 24–72ч) и стоп. Формат — коротко и по делу.

Данные:
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты лаконичный и прагматичный рыночный аналитик."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.25,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"GPT error: {e}"

# ====== Форматирование сообщений ======
def format_prices_msg(p):
    w = p.get("WTI"); wchg = p.get("WTI_change")
    d = p.get("DXY"); dchg = p.get("DXY_change")
    parts = ["💹 <b>DXY & WTI update</b>", f"🕒 {utc_now()}"]
    if w is not None: parts.append(f"🛢 WTI: <b>${w}</b>  (24h {wchg:+}%)")
    if d is not None: parts.append(f"💵 DXY: <b>{d}</b>  (24h {dchg:+}%)")
    return "\n".join(parts)

def format_summary_msg(payload, analysis=None):
    lines = [f"🧾 <b>Oil Report: SUMMARY</b>", f"🕒 {utc_now()}"]

    # EIA
    e = payload.get("eia", {}).get("raw", {})
    if e:
        lines.append(
            f"📅 Period: <b>{e.get('period','N/A')}</b>\n"
            f"📍 Region: <b>{e.get('area-name','N/A')}</b>\n"
            f"🛢 Product: <b>{e.get('product-name','N/A')}</b>\n"
            f"📦 Value: <b>{e.get('value','N/A')} {e.get('units','')}</b>"
        )

    # Baker Hughes
    r = payload.get("rigs", {})
    if r.get("snippet"):
        lines.append(f"\n🏗️ <b>Baker Hughes:</b>\n{r['snippet']}")

    # CFTC
    c = payload.get("cot", {})
    if c.get("snippet"):
        lines.append(f"\n📊 <b>CFTC:</b> {c['snippet']}")

    # Prices
    p = payload.get("prices", {})
    lines.append(
        f"\n💹 <b>Market:</b>\n"
        f"🛢 WTI: <b>${p.get('WTI','N/A')}</b> ({p.get('WTI_change',0):+}%)\n"
        f"💵 DXY: <b>{p.get('DXY','N/A')}</b> ({p.get('DXY_change',0):+}%)"
    )

    if analysis:
        lines.append("\n🧠 <b>AI Analysis</b>\n" + analysis)

    return "\n".join(lines)

# ====== Сбор данных ======
def collect(mode: str):
    mode = (mode or "summary").lower()
    data = {"timestamp": utc_now(), "mode": mode}

    def _prices(): return ("prices", get_prices())
    def _eia():    return ("eia", get_eia_weekly())
    def _cot():    return ("cot", get_cftc_disagg_snippet())
    def _rigs():   return ("rigs", get_baker_hughes())

    tasks = []
    if mode in ("prices", "summary"): tasks.append(_prices)
    if mode in ("eia", "summary"):    tasks.append(_eia)
    if mode in ("cot", "summary"):    tasks.append(_cot)
    if mode in ("rigs", "summary"):   tasks.append(_rigs)

    # параллельно
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(fn) for fn in tasks]
        for f in concurrent.futures.as_completed(futs):
            k, v = f.result()
            data[k] = v

    return data

def run_once(mode: str, chat_id: str | None = None):
    payload = collect(mode)
    analysis = None
    if mode in ("summary", "eia", "cot", "rigs"):
        analysis = gpt_analyze(payload)
    if mode == "prices":
        msg = format_prices_msg(payload.get("prices", {}))
    else:
        msg = format_summary_msg(payload, analysis)
    sent = send_telegram(msg, chat_id=chat_id)
    return {"ok": True, "sent": sent, "payload": payload, "analysis": analysis}

# ====== HTTP ======
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

# ====== CRON: ежедневный дайджест 14:00 UTC ======
@app.route("/cron/daily")
def cron_daily():
    # просто запускаем SUMMARY и отправляем в TELEGRAM_CHAT_ID
    res = run_once("summary", chat_id=TELEGRAM_CHAT_ID)
    return jsonify({"ok": True, "result": res})

# ====== Telegram webhook (команды) ======
@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    try:
        update = request.get_json(force=True, silent=True) or {}
        msg = update.get("message") or update.get("edited_message") or {}
        chat_id = str(msg.get("chat", {}).get("id", "")) or TELEGRAM_CHAT_ID
        text = (msg.get("text") or "").strip()

        if not text:
            return jsonify({"ok": True})

        if text.lower().startswith("/start") or text.lower().startswith("/help"):
            help_txt = (
                "🛢 <b>Oil Analyzer Bot</b>\n\n"
                "Доступные команды:\n"
                "• <b>/prices</b> — WTI & DXY (последние цены)\n"
                "• <b>/summary</b> — cводка (EIA, Baker Hughes, CFTC, цены, AI-анализ)\n"
                "• <b>/help</b> — помощь\n\n"
                "Авто-дайджест приходит ежедневно в 14:00 UTC."
            )
            send_telegram(help_txt, chat_id=chat_id)
            return jsonify({"ok": True})

        if text.lower().startswith("/prices"):
            p = collect("prices")
            msg_txt = format_prices_msg(p.get("prices", {}))
            send_telegram(msg_txt, chat_id=chat_id)
            return jsonify({"ok": True})

        if text.lower().startswith("/summary"):
            res = run_once("summary", chat_id=chat_id)
            return jsonify({"ok": True, "result": res})

        # Всё остальное — просто подсказка
        send_telegram("Не знаю такую команду. Напишите /help", chat_id=chat_id)
        return jsonify({"ok": True})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

# ====== DEV/CLI ======
if __name__ == "__main__":
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        result = run_once(mode)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
