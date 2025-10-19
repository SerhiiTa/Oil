# ============================
#  OIL ANALYZER v7.2 (final)
#  Источники: EIA, Baker Hughes, CFTC, FRED, Yahoo
#  Команды: /summary /prices /eia /baker /cot /macro /help
# ============================

import os, sys, json, requests, yfinance as yf
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request
from openai import OpenAI
import concurrent.futures

# -------- ENV --------
EIA_API_KEY        = os.getenv("EIA_API_KEY", "")
FRED_API_KEY       = os.getenv("FRED_API_KEY", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

REQUEST_TIMEOUT = 20
CACHE = {}

app = Flask(__name__)

# -------- HELPERS --------
def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def http_get(url):
    return requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "oil-analyzer/7.2"})

def get_cache(key):
    item = CACHE.get(key)
    if not item:
        return None
    if datetime.now(timezone.utc) > item["ts"] + timedelta(seconds=item["ttl"]):
        return None
    return item["data"]

def set_cache(key, data, ttl):
    CACHE[key] = {"ts": datetime.now(timezone.utc), "ttl": ttl, "data": data}

def send_telegram(text, chat_id=None, parse_mode="HTML"):
    if not TELEGRAM_BOT_TOKEN:
        return False
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.ok
    except Exception:
        return False

# -------- DATA SOURCES --------
def get_prices():
    """
    Быстрые WTI/DXY с кэшем 10 минут.
    1) Yahoo quote JSON (moment snapshot)
    2) fallback: Yahoo chart JSON (2d/1h)
    3) fallback: yfinance
    """
    cached = get_cache("prices")
    out = {"WTI": None, "WTI_change": None, "DXY": None, "DXY_change": None, "source": "Yahoo"}

    def _yahoo_quote(symbols):
        try:
            url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ",".join(symbols)
            js = http_get(url).json()
            quotes = js["quoteResponse"]["result"]
            m = {q["symbol"]: q for q in quotes}
            return m
        except Exception:
            return {}

    def _chart_last_prev(sym):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=2d&interval=1h"
            js = http_get(url).json()
            res = js.get("chart", {}).get("result", [])
            if not res:
                return None, None
            closes = res[0]["indicators"]["quote"][0]["close"]
            closes = [x for x in closes if x is not None]
            if len(closes) >= 2:
                return float(closes[-1]), float(closes[-2])
            if len(closes) == 1:
                return float(closes[-1]), float(closes[-1])
        except Exception:
            pass
        return None, None

    def _yf_last_prev(sym):
        try:
            t = yf.Ticker(sym)
            h = t.history(period="2d", interval="1h").dropna()
            if len(h) >= 2:
                return float(h["Close"].iloc[-1]), float(h["Close"].iloc[-2])
            if len(h) == 1:
                v = float(h["Close"].iloc[-1]); return v, v
        except Exception:
            pass
        return None, None

    # --- try yahoo quote first ---
    q = _yahoo_quote(["CL=F", "^DXY"])
    try:
        if "CL=F" in q:
            last = q["CL=F"].get("regularMarketPrice")
            prev = q["CL=F"].get("regularMarketPreviousClose")
            if last is not None and prev is not None:
                out["WTI"] = round(float(last), 2)
                out["WTI_change"] = round((float(last) - float(prev)) / float(prev) * 100, 2)
    except Exception:
        pass
    try:
        if "^DXY" in q:
            last = q["^DXY"].get("regularMarketPrice")
            prev = q["^DXY"].get("regularMarketPreviousClose")
            if last is not None and prev is not None:
                out["DXY"] = round(float(last), 2)
                out["DXY_change"] = round((float(last) - float(prev)) / float(prev) * 100, 2)
    except Exception:
        pass

    # --- fallbacks if needed ---
    if out["WTI"] is None:
        w_last, w_prev = _chart_last_prev("CL=F")
        if w_last is None:
            w_last, w_prev = _yf_last_prev("CL=F")
        if w_last is not None and w_prev is not None:
            out["WTI"] = round(w_last, 2)
            out["WTI_change"] = round((w_last - w_prev) / w_prev * 100, 2) if w_prev else 0.0

    if out["DXY"] is None:
        d_last, d_prev = _chart_last_prev("^DXY")
        if d_last is None:
            d_last, d_prev = _yf_last_prev("^DXY")
        if d_last is not None and d_prev is not None:
            out["DXY"] = round(d_last, 2)
            out["DXY_change"] = round((d_last - d_prev) / d_prev * 100, 2) if d_prev else 0.0

    # если совсем пусто — вернём кэш
    if (out["WTI"] is None) and (out["DXY"] is None) and cached:
        return cached

    set_cache("prices", out, 600)  # 10 мин
    return out

def get_eia():
    """EIA Weekly (кэш 6ч)."""
    if not EIA_API_KEY:
        return {"error": "EIA_API_KEY missing"}
    cached = get_cache("eia")
    if cached: return cached
    try:
        url = (
            "https://api.eia.gov/v2/petroleum/sum/sndw/data/"
            f"?api_key={EIA_API_KEY}&frequency=weekly&data[0]=value"
            "&sort[0][column]=period&sort[0][direction]=desc&length=1"
        )
        rec = http_get(url).json()["response"]["data"][0]
        data = {
            "period": rec.get("period"),
            "area": rec.get("area-name"),
            "product": rec.get("product-name"),
            "value": float(rec.get("value")) if rec.get("value") not in (None, "") else None,
            "units": rec.get("units", "")
        }
        set_cache("eia", data, 21600)
        return data
    except Exception as e:
        return {"error": f"eia: {e}"}

def get_baker():
    """Rig count (кэш 24ч) — парсим главную страницу."""
    cached = get_cache("baker")
    if cached: return cached
    try:
        html = http_get("https://rigcount.bakerhughes.com/").text
        soup = BeautifulSoup(html, "html.parser")
        txt = soup.get_text(" ", strip=True)
        # выхватываем фрагмент с "U.S." или "Canada" — как показывали твои скрины
        keys = ["U.S.", "Canada", "International"]
        frag = None
        for kw in keys:
            if kw in txt:
                i = txt.find(kw)
                frag = txt[max(0, i - 60): i + 220]
                break
        snippet = (frag or txt[:300]).strip()
        data = {"snippet": snippet}
    except Exception as e:
        data = {"error": f"baker: {e}"}
    set_cache("baker", data, 86400)
    return data

def get_cftc():
    """
    Disaggregated Futures (long format) по нефти — берём релевантный фрагмент.
    Реальные секции из твоих фото:
    WTI - PHYSICAL / WTI FINANCIAL / BRENT LAST DAY / WTI-BRENT CALENDAR / CRUDE DIFF / CONDENSATE / HSFO / MARINE .5% / FUEL OIL
    """
    cached = get_cache("cftc")
    if cached: return cached
    try:
        url = "https://www.cftc.gov/dea/futures/petroleum_lf.htm"
        html = http_get(url).text
        keys = [
            "WTI - PHYSICAL",
            "WTI FINANCIAL",
            "WTI MIDLAND",
            "WTI HOUSTON",
            "BRENT LAST DAY",
            "WTI-BRENT CALENDAR",
            "CRUDE DIFF",
            "CONDENSATE",
            "USGC HSFO",
            "MARINE .5%",
            "FUEL OIL"
        ]
        hit = None
        for kw in keys:
            if kw in html:
                i = html.find(kw)
                hit = html[max(0, i - 300): i + 1000]
                break
        if not hit:
            # мягкий фоллбэк на слово Petroleum
            if "Petroleum" in html:
                i = html.find("Petroleum")
                hit = html[max(0, i - 300): i + 1000]
        soup = BeautifulSoup(hit or "", "html.parser")
        text = soup.get_text(" ", strip=True)[:1000] if hit else "CFTC: crude-related section not found"
        data = {"snippet": text, "url": url}
    except Exception as e:
        data = {"error": f"cftc: {e}"}
    set_cache("cftc", data, 86400)
    return data

def get_fred():
    """CPI и FedFunds (кэш 12ч)."""
    if not FRED_API_KEY:
        return {"error": "FRED_API_KEY missing"}
    cached = get_cache("fred")
    if cached: return cached
    try:
        url_cpi  = f"https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&api_key={FRED_API_KEY}&file_type=json"
        url_rate = f"https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key={FRED_API_KEY}&file_type=json"
        cpi  = http_get(url_cpi).json()["observations"][-1]
        rate = http_get(url_rate).json()["observations"][-1]
        data = {"CPI": float(cpi["value"]), "FedRate": float(rate["value"]), "CPI_date": cpi["date"], "Rate_date": rate["date"]}
        set_cache("fred", data, 43200)
        return data
    except Exception as e:
        return {"error": f"fred: {e}"}
        # ============================
#  AI ANALYZER + ROUTES
# ============================

def gpt_analyze_full_html(payload: dict) -> str:
    """
    Генерирует полный отчёт (РОВНО как в макете пользователя):
    - торговый блок с BUY/SELL/NEUTRAL, динамическими целью/стопом
    - факторы
    - секции EIA/Baker/CFTC/Macro/Market
    - общий вердикт и подпись
    Формат: HTML (parse_mode='HTML')
    Правила динамики цели/стопа (относительно цены WTI P):
      BUY:  target = P * (1 + 0.015), stop = P * (1 - 0.010)
      SELL: target = P * (1 - 0.015), stop = P * (1 + 0.010)
      NEUTRAL: target/stop оставить пустыми или нейтральными
    """
    if not OPENAI_API_KEY:
        return "GPT disabled: OPENAI_API_KEY not set."
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)

        # подготовим некоторые удобные числа для модели
        prices = payload.get("prices", {}) or {}
        wti = prices.get("WTI")
        dxy = prices.get("DXY")
        wti_chg = prices.get("WTI_change")
        dxy_chg = prices.get("DXY_change")

        # ссылка CFTC (для контекста модели)
        cftc_url = (payload.get("cot") or {}).get("url", "https://www.cftc.gov/dea/futures/petroleum_lf.htm")

        system = (
            "Ты опытный аналитик нефтяного рынка. "
            "Собери ЕДИНЫЙ отчёт для Telegram в формате HTML (не Markdown), строго по шаблону ниже. "
            "Обязательно включи торговую рекомендацию (BUY/SELL/NEUTRAL), динамическую цель и стоп по правилам, "
            "факторы (бычьи/медвежьи), секции EIA / Baker / CFTC / Macro / Market, общий вердикт. "
            "Все числа округляй разумно (2 знака). Не добавляй лишних пояснений вне шаблона. "
            "Если каких-то данных нет — аккуратно напиши, что данных нет."
        )

        # Шаблон/инструкции для модели
        user = f"""
Данные (JSON):
{json.dumps(payload, ensure_ascii=False, indent=2)}

Ссылка CFTC: {cftc_url}

Собери ОДНО сообщение в HTML РОВНО по шаблону (на русском):

🔴 <b>EIA Oil Report Analysis</b>

🎯 BUY/SELL/NEUTRAL (укажи на русском в скобках: Лонг/Шорт)
💰 Цена WTI: ${'{:.2f}'.format(wti) if isinstance(wti,(int,float)) else 'Данные недоступны'}

<b>Торговый план:</b>
🎯 Цель: $<авто-расчёт от цены WTI по правилам выше>
⛔ Стоп: $<авто-расчёт от цены WTI по правилам выше>

<b>Факторы:</b>
- 🔴/🟢 Короткие пункты (2–5 шт)

<b>📈 Итог:</b>
Короткое резюме (1–2 предложения) с объяснением, почему такая рекомендация.

⏰ Вход: 09:45–10:00 CT

---
<h3>🛢 EIA</h3>
Коротко об изменениях/ключах из EIA (если известны: период, продукт/запасы/ввод и т.п.)

<h3>🏗 Baker Hughes</h3>
Коротко: количество буровых/тенденция. Если данных нет — укажи.

<h3>📊 CFTC</h3>
Коротко по позициям (Managed Money, net long/short). Если данных мало — укажи.

<h3>🏦 Macro (FRED)</h3>
CPI и ставка ФРС (и толкование). Если данных нет — укажи.

<h3>💹 Market (WTI/DXY)</h3>
WTI ${'{:.2f}'.format(wti) if isinstance(wti,(int,float)) else 'N/A'} ({wti_chg:+.2f}% если есть), DXY {dxy if dxy is not None else 'N/A'} ({dxy_chg:+.2f}% если есть).

---
<b>📊 Общий вердикт:</b> BUY/SELL/NEUTRAL (жирным, на одной строке)

🕒 Generated at {utc_now()}
🤖 <i>EIA Oil Analyzer</i>

ВАЖНО:
• Форматируй ТОЛЬКО HTML-тегами (<b>, <i>, <h3>, <br/> не нужно — переносы по строке).
• Числа целей/стопов рассчитай от текущей цены WTI P так:
  — BUY:  target = P * 1.015, stop = P * 0.990
  — SELL: target = P * 0.985, stop = P * 1.010
  — NEUTRAL: допускается указать цель/стоп коротко как «—».
• Если цена WTI недоступна, напиши «Данные недоступны» и не выдумывай число.
"""

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.25,
        )
        return resp.choices[0].message.content.strip()

    except Exception as e:
        return f"GPT error: {e}"

# ---- собрать данные параллельно ----
def collect(mode: str) -> dict:
    mode = (mode or "summary").lower()
    data = {"mode": mode, "timestamp": utc_now()}

    def _prices(): return ("prices", get_prices())
    def _eia():    return ("eia", get_eia())
    def _baker():  return ("baker", get_baker())
    def _cftc():   return ("cot", get_cftc())
    def _fred():   return ("fred", get_fred())

    tasks = []
    if mode in ("summary", "prices"): tasks.append(_prices)
    if mode in ("summary", "eia"):    tasks.append(_eia)
    if mode in ("summary", "baker"):  tasks.append(_baker)
    if mode in ("summary", "cot"):    tasks.append(_cftc)
    if mode in ("summary", "macro"):  tasks.append(_fred)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futs = [ex.submit(fn) for fn in tasks]
        for f in concurrent.futures.as_completed(futs):
            k, v = f.result()
            data[k] = v
    return data

def run_summary(chat_id=None):
    payload = collect("summary")
    report = gpt_analyze_full_html(payload)
    sent = send_telegram(report, chat_id=chat_id, parse_mode="HTML")
    return {"ok": True, "sent": sent, "payload": payload, "report_len": len(report)}

# ---- ROUTES ----
@app.route("/")
def index():
    return jsonify({"ok": True, "message": "Oil Analyzer v7.2 running", "time": utc_now()})

@app.route("/health")
def health():
    return jsonify({"ok": True, "time": utc_now()})

@app.route("/data")
def data_endpoint():
    mode = request.args.get("mode", "summary")
    return jsonify(collect(mode))

@app.route("/analyze")
def analyze_endpoint():
    res = run_summary(chat_id=TELEGRAM_CHAT_ID)
    return jsonify(res)

# ---- CRON ----
@app.route("/cron/hourly")
def cron_hourly():
    """
    Дёргай Render Scheduler ЕЖЕЧАСНО.
    Отправим отчёт ТОЛЬКО если сейчас 08:00 по America/Chicago.
    Это надёжно перекроет смену DST (летнее/зимнее время).
    """
    now_cst = datetime.now(ZoneInfo("America/Chicago"))
    if now_cst.hour == 8:
        res = run_summary(chat_id=TELEGRAM_CHAT_ID)
        return jsonify({"ok": True, "ran": True, "local_time": now_cst.isoformat(), "result": res})
    else:
        return jsonify({"ok": True, "ran": False, "local_time": now_cst.isoformat()})

# ---- Telegram webhook ----
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
                "• /summary — полный отчёт + торговый план (AI)\n"
                "• /prices — только WTI & DXY (быстро)\n"
                "• /eia — последний Weekly EIA (сырой блок)\n"
                "• /baker — Baker Hughes (сырой блок)\n"
                "• /cot — CFTC snippet (сырой блок)\n"
                "• /macro — FRED (CPI/FedFunds)\n\n"
                "Автоотчёт ежедневно в 08:00 по America/Chicago."
            )
            send_telegram(help_txt, chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/prices"):
            p = get_prices()
            msg_txt = (
                f"💹 <b>DXY & WTI update</b>\n"
                f"🕒 {utc_now()}\n\n"
                f"🛢 WTI: <b>${(f'{p['WTI']:.2f}' if isinstance(p.get('WTI'),(int,float)) else 'N/A')}</b> "
                f"(24h {(f'{p['WTI_change']:+.2f}%' if isinstance(p.get('WTI_change'),(int,float)) else '+0.00%')})\n"
                f"💵 DXY: <b>{(f'{p['DXY']:.2f}' if isinstance(p.get('DXY'),(int,float)) else 'N/A')}</b> "
                f"(24h {(f'{p['DXY_change']:+.2f}%' if isinstance(p.get('DXY_change'),(int,float)) else '+0.00%')})"
            )
            send_telegram(msg_txt, chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/summary"):
            res = run_summary(chat_id=chat_id)
            return jsonify({"ok": True, "result": res})

        if text.startswith("/eia"):
            send_telegram(f"<b>EIA raw:</b>\n<code>{json.dumps(get_eia(), ensure_ascii=False)[:1000]}</code>", chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/baker"):
            send_telegram(f"<b>Baker raw:</b>\n<code>{json.dumps(get_baker(), ensure_ascii=False)[:1000]}</code>", chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/cot"):
            send_telegram(f"<b>CFTC raw:</b>\n<code>{json.dumps(get_cftc(), ensure_ascii=False)[:1000]}</code>", chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/macro"):
            send_telegram(f"<b>FRED raw:</b>\n<code>{json.dumps(get_fred(), ensure_ascii=False)[:1000]}</code>", chat_id=chat_id)
            return jsonify({"ok": True})

        send_telegram("Неизвестная команда. Напишите /help", chat_id=chat_id)
        return jsonify({"ok": True})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

# ---- DEV / LOCAL ----
if __name__ == "__main__":
    if len(sys.argv) > 1:
        # запуск в режиме CLI: python app.py summary
        mode = sys.argv[1].lower()
        if mode == "summary":
            print(json.dumps(run_summary(), ensure_ascii=False, indent=2))
        else:
            print(json.dumps(collect(mode), ensure_ascii=False, indent=2))
    else:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
