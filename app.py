import os
import logging
import sys
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- 1. CLOUD LOGGING CONFIGURATION ---
# This ensures logs appear in Render when running via python app.py
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))

# Attach to both Root and Flask loggers
logging.getLogger().addHandler(stream_handler)
app.logger.addHandler(stream_handler)
app.logger.setLevel(logging.INFO)

app.logger.info(">>> ML FUSION BRIDGE: LOGGER INITIALIZED <<<")

# --- CONFIGURATION ---
TELEGRAM_TOKEN = "8272512971:AAHCVmQj_0Q30b0PgfSwP43WpMSMb-NJuDo"
TELEGRAM_CHAT_ID = "1420064473" 
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"
DHAN_SECRET = "OvWi0"
EXPIRY_DATE = "2025-12-30" 

# --- GLOBAL TRACKERS ---
last_signal = "None"
last_strike = "None"
last_status = "Waiting for first signal..."
trade_count = 0

def get_itm_strike(price, ticker, opt_type):
    try:
        price = float(price)
        step = 100 if "BANK" in ticker.upper() else 50
        atm_strike = int(round(price / step) * step)
        return (atm_strike - step) if opt_type == "CE" else (atm_strike + step)
    except Exception as e:
        app.logger.error(f"Strike Calculation Error: {e}")
        return 0

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        app.logger.error(f"Telegram Error: {e}")

@app.route('/', methods=['GET'])
def health_check():
    return f"""
    <html>
        <body style="font-family: sans-serif; text-align: center; padding-top: 50px; background-color: #f4f4f9;">
            <div style="display: inline-block; padding: 20px; border: 1px solid #ccc; border-radius: 10px; background: white;">
                <h1 style="color: #2c3e50;">🚀 ML Fusion Bridge is LIVE</h1>
                <p><strong>Last Signal:</strong> {last_signal}</p>
                <p><strong>Last ITM Strike:</strong> {last_strike}</p>
                <p><strong>Total Flips Today:</strong> {trade_count}</p>
                <p><strong>Dhan Status:</strong> {last_status}</p>
                <hr>
                <p style="color: gray; font-size: 0.8em;">Expiry Config: {EXPIRY_DATE}</p>
            </div>
        </body>
    </html>
    """, 200

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    global last_signal, last_strike, last_status, trade_count
    
    data = request.get_json()
    app.logger.info(f"--- WEBHOOK RECEIVED: {data} ---")
    
    if not data:
        app.logger.error("No JSON data received")
        return jsonify({"status": "error"}), 400

    signal = data.get("message", "").upper() 
    ticker = data.get("ticker", "BANKNIFTY")
    price = data.get("price", 0)

    new_opt_type = "CE" if "BUY" in signal else "PE"
    itm_strike = get_itm_strike(price, ticker, new_opt_type)

    last_signal = f"{signal} on {ticker}"
    last_strike = f"{itm_strike} ({new_opt_type})"
    trade_count += 1

    dhan_payload = {
        "secret": DHAN_SECRET,
        "alertType": "multi_leg_order",
        "order_legs": [
            {
                "transactionType": "S", "orderType": "MKT", "quantity": "0",
                "exchange": "NSE", "symbol": ticker, "instrument": "OPT",
                "productType": "M", "sort_order": "1", "manage_position": "EXIT_ALL" 
            },
            {
                "transactionType": "B", "orderType": "MKT", "quantity": "1",
                "exchange": "NSE", "symbol": ticker, "instrument": "OPT",
                "productType": "M", "sort_order": "2", "option_type": new_opt_type,
                "strike_price": str(float(itm_strike)), "expiry_date": EXPIRY_DATE
            }
        ]
    }

    try:
        response = requests.post(DHAN_WEBHOOK_URL, json=dhan_payload, timeout=15)
        last_status = f"Success ({response.status_code})"
        app.logger.info(f"Dhan Action: {signal} | Strike: {itm_strike} | Resp: {response.status_code}")
        return jsonify({"status": "SUCCESS"}), 200
    except Exception as e:
        last_status = f"Error: {str(e)}"
        app.logger.error(f"Post Error: {e}")
        return jsonify({"status": "FAILED"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
