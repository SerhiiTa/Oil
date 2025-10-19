# ============================
#  OIL ANALYZER v6 (GPT-4o)
# ============================
#  Sources: EIA, Baker Hughes, CFTC, Yahoo Finance, FRED, Alpha Vantage
#  Features:
#   • AI-анализ по каждому блоку
#   • Ежедневный авто-дайджест + /summary, /prices, /help
#   • Telegram webhook и CRON endpoint
#   • Кэширование и параллельные запросы
# ============================

import os
import sys
import json
import requests
import yfinance as yf
import concurrent.futures
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request
from openai import OpenAI

# ====== ENV ======
EIA_API_KEY        = os.getenv("EIA_API_KEY", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
FRED_API_KEY       = os.getenv("FRED_API_KEY", "")
ALPHA_VANTAGE_KEY  = os.getenv("ALPHA_VANTAGE_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

REQUEST_TIMEOUT = 20
CACHE = {}

app = Flask(__name__)

# ====== HELPERS ======
def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def http_get(url):
    return requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "oil-analyzer/2.0"})

# ====== CACHE ======
def get_cache(key):
    entry = CACHE.get(key)
    if not entry:
        return None
    if datetime.now(timezone.utc) > entry["ts"] + timedelta(seconds=entry["ttl"]):
        return None
    return entry["data"]

def set_cache(key, data, ttl_sec):
    CACHE[key] = {"ts": datetime.now(timezone.utc), "ttl": ttl_sec, "data": data}

# ====== TELEGRAM ======
def send_telegram(text, chat_id=None):
    if not TELEGRAM_BOT_TOKEN:
        return False
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.ok
    except Exception:
        return False


# ============================
#   DATA SOURCES
# ============================

# ====== EIA ======
def get_eia_weekly():
    """EIA Weekly Petroleum Status (cache 6h)."""
    if not EIA_API_KEY:
        return {"error": "EIA_API_KEY missing"}
    cached = get_cache("eia")
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
        set_cache("eia", data, ttl_sec=21600)
        return data
    except Exception as e:
        return {"error": f"eia: {e}"}


# ====== Baker Hughes ======
def get_baker_hughes():
    """Парсинг rigcount.bakerhughes.com (краткий сниппет)."""
    cached = get_cache("baker")
    if cached:
        return cached
    try:
        html = http_get("https://rigcount.bakerhughes.com/").text
        soup = BeautifulSoup(html, "html.parser")
        txt = soup.get_text(" ", strip=True)
        key_words = ["U.S.", "Canada", "International"]
        frag = None
        for kw in key_words:
            if kw in txt:
                i = txt.find(kw)
                frag = txt[max(0, i - 60): i + 220]
                break
        snippet = (frag or txt[:300]).strip()
        data = {"snippet": snippet, "source": "Baker Hughes (Rig Count)"}
    except Exception as e:
        data = {"error": f"baker: {e}"}
    set_cache("baker", data, ttl_sec=86400)
    return data


# ====== CFTC (Disaggregated Petroleum) ======
def get_cftc():
    """
    Disaggregated reports:
      • Futures Only:        https://www.cftc.gov/dea/futures/petroleum_lf.htm
      • Futures + Options:   https://www.cftc.gov/dea/options/petroleum_lof.htm
    Возвращает короткий текст вокруг "Crude Oil".
    """
    cached = get_cache("cftc")
    if cached:
        return cached

    urls = [
        "https://www.cftc.gov/dea/futures/petroleum_lf.htm",
        "https://www.cftc.gov/dea/options/petroleum_lof.htm",
    ]

    texts = []
    try:
        for u in urls:
            html = http_get(u).text
            soup = BeautifulSoup(html, "html.parser")
            txt = soup.get_text(" ", strip=True)
            key = "Crude Oil"
            if key in txt:
                i = txt.find(key)
                snippet = txt[max(0, i - 160): i + 320].strip()
                texts.append(snippet)
            else:
                texts.append(txt[:280])
        final = "\n\n".join(texts) or "CFTC: no petroleum section found."
        data = {"snippet": final, "source": "CFTC (Disaggregated Petroleum)"}
    except Exception as e:
        data = {"error": f"cftc: {e}"}

    set_cache("cftc", data, ttl_sec=86400)
    return data
    # ====== Yahoo Finance (WTI & DXY) ======
def get_prices():
    """WTI & DXY (cache 10m) с безопасным fallback."""
    cached = get_cache("prices")
    out = {"WTI": None, "WTI_change": None, "DXY": None, "DXY_change": None, "source": "Yahoo Finance"}

    def _last_close(ticker, period="2d", interval="1h"):
        t = yf.Ticker(ticker)
        try:
            h = t.history(period=period, interval=interval)
            if h is None or len(h.dropna()) == 0:
                h = yf.download(ticker, period=period, interval=interval, progress=False)
            h = h.dropna()
            if len(h) >= 2:
                return float(h["Close"].iloc[-1]), float(h["Close"].iloc[-2])
            elif len(h) == 1:
                last = float(h["Close"].iloc[-1])
                return last, last
        except Exception:
            return None, None
        return None, None

    w_last, w_prev = _last_close("CL=F")
    d_last, d_prev = _last_close("DX-Y.NYB")

    if (w_last is None or w_prev is None) and (d_last is None or d_prev is None):
        return cached or out

    try:
        if w_last and w_prev:
            out["WTI"] = round(w_last, 2)
            out["WTI_change"] = round((w_last - w_prev) / w_prev * 100, 2)
        if d_last and d_prev:
            out["DXY"] = round(d_last, 2)
            out["DXY_change"] = round((d_last - d_prev) / d_prev * 100, 2)
    except Exception as e:
        out["error"] = f"prices: {e}"

    if cached:
        for k in ["WTI", "WTI_change", "DXY", "DXY_change"]:
            if out.get(k) is None:
                out[k] = cached.get(k)

    set_cache("prices", out, ttl_sec=600)
    return out


# ====== FRED ======
def get_fred_data():
    if not FRED_API_KEY:
        return {"error": "FRED_API_KEY missing"}
    cached = get_cache("fred")
    if cached:
        return cached
    try:
        url_cpi  = f"https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&api_key={FRED_API_KEY}&file_type=json"
        url_rate = f"https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key={FRED_API_KEY}&file_type=json"
        cpi  = http_get(url_cpi).json()["observations"][-1]
        rate = http_get(url_rate).json()["observations"][-1]
        data = {"CPI": float(cpi["value"]), "CPI_date": cpi["date"],
                "FedRate": float(rate["value"]), "FedRate_date": rate["date"]}
        set_cache("fred", data, ttl_sec=43200)
        return data
    except Exception as e:
        return {"error": f"fred: {e}"}


# ====== Alpha Vantage (через Yahoo) ======
def get_alpha_vantage():
    cached = get_cache("alpha")
    if cached:
        return cached
    try:
        br = yf.download("BZ=F", period="2d", interval="1h", progress=False)
        brent = round(float(br["Close"].dropna().iloc[-1]), 2)
        sp = yf.download("^GSPC", period="2d", interval="1h", progress=False)
        sp500 = round(float(sp["Close"].dropna().iloc[-1]), 2)
        data = {"Brent": brent, "SP500": sp500}
        set_cache("alpha", data, ttl_sec=21600)
        return data
    except Exception as e:
        return {"error": f"alpha: {e}"}


# ====== GPT ANALYSIS ======
def gpt_analyze(payload):
    if not OPENAI_API_KEY:
        return "GPT disabled: OPENAI_API_KEY not set."
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Ты аналитик нефтяного рынка."},
                {"role": "user", "content": "Проанализируй данные:\n" + json.dumps(payload, ensure_ascii=False, indent=2)},
            ],
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"GPT error: {e}"
        # ====== FORMATTING ======
def _fmt_num(x, nd=2):
    try:
        return f"{float(x):,.{nd}f}"
    except (TypeError, ValueError):
        return "N/A"

def _fmt_pct(x, nd=2):
    try:
        return f"{float(x):+.{nd}f}%"
    except (TypeError, ValueError):
        return f"{0:+.{nd}f}%"

def format_summary_msg(payload, analysis=None):
    payload = payload or {}
    lines = [f"🧾 <b>Oil Report: SUMMARY</b>", f"🕒 {utc_now()}"]

    # --- EIA ---
    e = (payload.get("eia") or {}).get("raw") or {}
    if e:
        lines.append(
            f"📅 Period: <b>{e.get('period','N/A')}</b>\n"
            f"📍 Region: <b>{e.get('area-name','N/A')}</b>\n"
            f"🛢 Product: <b>{e.get('product-name','N/A')}</b>\n"
            f"📦 Value: <b>{e.get('value','N/A')} {e.get('units','')}</b>"
        )

    # --- Market prices ---
    p = payload.get("prices") or {}
    lines.append(
        f"\n💹 <b>Market:</b>\n"
        f"🛢 WTI: <b>${_fmt_num(p.get('WTI'))}</b> ({_fmt_pct(p.get('WTI_change'))})\n"
        f"💵 DXY: <b>{_fmt_num(p.get('DXY'))}</b> ({_fmt_pct(p.get('DXY_change'))})"
    )

    # --- AI analysis ---
    if analysis:
        lines.append("\n🧠 <b>AI Analysis</b>\n" + analysis)

    return "\n".join(lines)


# ====== COLLECT ALL DATA ======
def collect(mode: str):
    mode = (mode or "summary").lower()
    data = {"timestamp": utc_now(), "mode": mode}

    def _prices(): return ("prices", get_prices())
    def _eia(): return ("eia", get_eia_weekly())
    def _cot(): return ("cftc", get_cftc())
    def _rigs(): return ("baker", get_baker_hughes())
    def _fred(): return ("fred", get_fred_data())
    def _alpha(): return ("alpha", get_alpha_vantage())

    tasks = []
    if mode in ("summary", "prices"): tasks.append(_prices)
    if mode in ("summary", "eia"): tasks.append(_eia)
    if mode in ("summary", "cot"): tasks.append(_cot)
    if mode in ("summary", "rigs"): tasks.append(_rigs)
    if mode in ("summary", "fred"): tasks.append(_fred)
    if mode in ("summary", "alpha"): tasks.append(_alpha)

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(fn) for fn in tasks]
        for f in concurrent.futures.as_completed(futs):
            k, v = f.result()
            data[k] = v
    return data


# ====== EXECUTION ======
def run_once(mode: str, chat_id: str | None = None):
    payload = collect(mode)
    analysis = None
    if mode == "summary":
        analysis = gpt_analyze(payload)
    msg = format_summary_msg(payload, analysis)
    send_telegram(msg, chat_id=chat_id)
    return {"ok": True, "payload": payload, "analysis": analysis}


# ====== ROUTES ======
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


@app.route("/cron/daily")
def cron_daily():
    res = run_once("summary", chat_id=TELEGRAM_CHAT_ID)
    return jsonify({"ok": True, "result": res})


@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    try:
        update = request.get_json(force=True, silent=True) or {}
        msg = update.get("message") or update.get("edited_message") or {}
        chat_id = str(msg.get("chat", {}).get("id", "")) or TELEGRAM_CHAT_ID
        text = (msg.get("text") or "").strip().lower()

        if text in ("/start", "/help"):
            help_txt = (
                "🛢 <b>Oil Analyzer Bot</b>\n\n"
                "Команды:\n"
                "• /prices — последние цены WTI и DXY\n"
                "• /summary — полный отчёт (EIA, Baker, CFTC, FRED, Alpha, AI)\n"
                "• /help — помощь\n\n"
                "📆 Автоотчёт ежедневно в 14:00 UTC."
            )
            send_telegram(help_txt, chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/prices"):
            data = collect("prices")
            msg_txt = format_summary_msg(data)
            send_telegram(msg_txt, chat_id=chat_id)
            return jsonify({"ok": True})

        if text.startswith("/summary"):
            res = run_once("summary", chat_id=chat_id)
            return jsonify({"ok": True, "result": res})

        send_telegram("Неизвестная команда. Введите /help", chat_id=chat_id)
        return jsonify({"ok": True})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200


# ====== ROOT & RUN ======
@app.route("/")
def index():
    return jsonify({
        "ok": True,
        "message": "Oil Analyzer Bot is running",
        "time": utc_now()
    })


if __name__ == "__main__":
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        result = run_once(mode)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
