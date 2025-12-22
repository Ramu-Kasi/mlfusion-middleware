import os
import logging
import sys
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- INSTANT LOGGING SETUP ---
class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers(): logger.handlers.clear()
handler = FlushHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
logger.addHandler(handler)

# --- CONFIGURATION ---
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"
DHAN_SECRET = "OvWi0"
EXPIRY_DATE = "2025-12-30" 

last_status = "Waiting..."

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    global last_status
    try:
        data = request.get_json()
        logger.info(f">>> Alert Received: {data}")
        
        if not data:
            return jsonify({"status": "error", "reason": "No JSON payload"}), 400

        # Extract data from TradingView Alert
        signal = str(data.get("message", "")).upper()
        ticker = data.get("ticker", "BANKNIFTY")
        price = float(data.get("price", 0))
        
        # Logic for strike
        new_opt_type = "CE" if "BUY" in signal else "PE"
        step = 100 if "BANK" in ticker.upper() else 50
        atm = int(round(price / step) * step)
        final_strike = (atm - step) if new_opt_type == "CE" else (atm + step)

        # BUILD DHAN PAYLOAD
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
                    "strike_price": str(int(final_strike)), # Changed to clean Integer string
                    "expiry_date": EXPIRY_DATE
                }
            ]
        }

        # SEND TO DHAN
        resp = requests.post(DHAN_WEBHOOK_URL, json=dhan_payload, timeout=15)
        
        # LOG THE REJECTION REASON
        logger.info(f">>> Dhan Response: {resp.text} (Status: {resp.status_code})")
        last_status = f"Dhan Response: {resp.text}"
        
        return jsonify({"status": "processed", "dhan_msg": resp.text}), 200

    except Exception as e:
        logger.error(f"!!! BRIDGE CRASHED: {str(e)}")
        last_status = f"Bridge Error: {str(e)}"
        return jsonify({"error": str(e)}), 500

@app.route('/')
def health():
    return f"<h1>Last Bridge Activity:</h1><p>{last_status}</p>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
