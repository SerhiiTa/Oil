from flask import Flask, jsonify
import requests, yfinance as yf
from bs4 import BeautifulSoup
from datetime import datetime

app = Flask(__name__)

@app.route("/data.json")
def get_data():
    data = {"timestamp": datetime.utcnow().isoformat()}

    # --- WTI & DXY ---
    try:
        wti = yf.Ticker("CL=F").info.get("regularMarketPrice")
        dxy = yf.Ticker("DX-Y.NYB").info.get("regularMarketPrice")
        data["WTI"] = wti
        data["DXY"] = dxy
    except Exception as e:
        data["WTI"], data["DXY"] = None, None
        data["error_yahoo"] = str(e)

    # --- Baker Hughes ---
    try:
        r = requests.get("https://rigcount.bakerhughes.com/")
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        data["BakerHughes"] = table.text.strip()[:120] if table else "Not found"
    except Exception as e:
        data["BakerHughes"] = None
        data["error_rig"] = str(e)

    # --- CFTC COT ---
    try:
        url = "https://www.cftc.gov/dea/futures/deacmelf.htm"
        r = requests.get(url)
        if r.status_code == 200:
            data["COT"] = "OK"
        else:
            data["COT"] = f"Error {r.status_code}"
    except Exception as e:
        data["COT"] = None
        data["error_cot"] = str(e)

    return jsonify(data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
