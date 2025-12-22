import os
import logging
import sys
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- INSTANT LOGGING ---
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

# --- CONFIG --- (Double check these!)
TELEGRAM_TOKEN = "8272512971:AAHCVmQj_0Q30b0PgfSwP43WpMSMb-NJuDo"
TELEGRAM_CHAT_ID = "1420064473" 
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"
DHAN_SECRET = "OvWi0"
EXPIRY_DATE = "2025-12-30" 

last_status = "Waiting..."
trade_count = 0

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    global last_status, trade_count
    try:
        data = request.get_json()
        logger.info(f"RECEIVED FROM TV: {data}")
        
        signal = str(data.get("message", "")).upper()
        ticker = data.get("ticker", "BANKNIFTY")
        price = data.get("price", 0)
        
        # Logic to determine type
        new_opt_type = "CE" if "BUY" in signal else "PE"
        
        # Simple Strike Calc
        price = float(price)
        step = 100 if "BANK" in ticker.upper() else 50
        itm_strike = int(round(price / step) * step)
        final_strike = (itm_strike - step) if new_opt_type == "CE" else (itm_strike + step)

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
                    "strike_price": str(float(final_strike)), "expiry_date": EXPIRY_DATE
                }
            ]
        }

        # SEND TO DHAN
        resp = requests.post(DHAN_WEBHOOK_URL, json=dhan_payload, timeout=15)
        
        # THIS IS THE IMPORTANT PART: Capture what Dhan actually said
        dhan_msg = resp.text 
        last_status = f"Dhan Said: {dhan_msg} (Code: {resp.status_code})"
        logger.info(f"DHAN RESPONSE: {dhan_msg}")
        
        trade_count += 1
        return jsonify({"status": "processed", "dhan_response": dhan_msg}), 200

    except Exception as e:
        logger.error(f"BRIDGE CRASHED: {str(e)}")
        last_status = f"Crash: {str(e)}"
        return jsonify({"error": str(e)}), 500

@app.route('/')
def status():
    return f"<h1>Status: {last_status}</h1><p>Trades: {trade_count}</p>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
