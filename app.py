import os
import logging
import sys
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# 1. Create a specialized handler for Render's console
class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()  # Force the line out of the buffer immediately

# 2. Configure the logger using this new handler
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

handler = FlushHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

logger.info(">>> ML FUSION BRIDGE: INSTANT LOGGING ACTIVE <<<")

# --- CONFIGURATION ---
TELEGRAM_TOKEN = "8272512971:AAHCVmQj_0Q30b0PgfSwP43WpMSMb-NJuDo"
TELEGRAM_CHAT_ID = "1420064473" 
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"
DHAN_SECRET = "OvWi0"
EXPIRY_DATE = "2025-12-30" 

# --- GLOBAL TRACKERS ---
last_signal = "None"
last_strike = "None"
trade_count = 0
last_status = "Waiting for signal..."

def get_itm_strike(price, ticker, opt_type):
    if ticker == "NIFTY":
        base = 50
    elif ticker == "BANKNIFTY":
        base = 100
    else:
        base = 100
    
    # Keeping your original logic: math on price works if price is numeric-string
    atm_strike = round(float(price) / base) * base
    return (atm_strike - base) if opt_type == "CE" else (atm_strike + base)

# --- ADDED: WAKEUP & SUMMARY ROUTE ---
@app.route('/')
def home():
    global last_signal, last_strike, trade_count, last_status
    return f"STATUS: {last_status} | LAST SIGNAL: {last_signal} | TRADES: {trade_count}"

# --- WEBHOOK ENDPOINT ---
@app.route('/mlfusion', methods=['POST'])
def webhook():
    global last_signal, last_strike, trade_count, last_status
    
    data = request.get_json()
    app.logger.info(f"--- WEBHOOK RECEIVED: {data} ---")

    if not data:
        return jsonify({"error": "No data received"}), 400

    signal = data.get("message", "").upper() 
    ticker = data.get("ticker", "BANKNIFTY")
    price = data.get("price", "0") # Kept as string

    if signal not in ["BUY", "SELL"]:
        return jsonify({"status": "Ignored"}), 200

    new_opt_type = "CE" if signal == "BUY" else "PE"
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
        return jsonify({"status": "SUCCESS"}), 200
    except Exception as e:
        last_status = f"Error: {str(e)}"
        return jsonify({"status": "FAILED"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
