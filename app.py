import os
import sys
import json
import requests
import logging
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- CONFIGURATION ---
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"
DHAN_SECRET = "OvWi0"
EXPIRY_DATE = "2025-12-30" 

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    try:
        data = request.get_json()
        # Force immediate print of incoming data
        print(f"\n--- [STEP 1] INCOMING ALERT ---\n{data}", flush=True)
        
        signal = str(data.get("message", "")).upper()
        ticker = data.get("ticker", "BANKNIFTY")
        price = float(data.get("price", 0))
        
        # Strike Selection Logic
        new_opt_type = "CE" if "BUY" in signal else "PE"
        step = 100 if "BANK" in ticker.upper() else 50
        atm = int(round(price / step) * step)
        itm_strike = (atm - step) if new_opt_type == "CE" else (atm + step)

        # BUILD DHAN PAYLOAD
        dhan_payload = {
            "secret": DHAN_SECRET,
            "alertType": "multi_leg_order",
            "order_legs": [
                {
                    "transactionType": "S", 
                    "orderType": "MKT", 
                    "quantity": "0",
                    "exchange": "NSE", 
                    "symbol": ticker, 
                    "instrument": "OPT",
                    "productType": "M", 
                    "sort_order": "1", 
                    "manage_position": "EXIT_ALL"
                },
                {
                    "transactionType": "B", 
                    "orderType": "MKT", 
                    "quantity": "1",
                    "exchange": "NSE", 
                    "symbol": ticker, 
                    "instrument": "OPT",
                    "productType": "M", 
                    "sort_order": "2", 
                    "option_type": new_opt_type,
                    "strike_price": f"{float(itm_strike):.1f}", # Decimal + String format
                    "expiry_date": EXPIRY_DATE
                }
            ]
        }

        # --- [STEP 2] PRINT FULL JSON BEFORE SENDING ---
        print("\n--- [STEP 2] PAYLOAD BEING SENT TO DHAN ---", flush=True)
        print(json.dumps(dhan_payload, indent=4), flush=True)
        print("-------------------------------------------\n", flush=True)

        # SEND TO DHAN
        resp = requests.post(DHAN_WEBHOOK_URL, json=dhan_payload, timeout=15)
        
        print(f"--- [STEP 3] DHAN RESPONSE ---\n{resp.text}\n(HTTP {resp.status_code})", flush=True)
        
        return jsonify({"bridge_status": "success", "dhan_raw": resp.text}), 200

    except Exception as e:
        print(f"!!! BRIDGE CRASHED: {str(e)}", flush=True)
        return jsonify({"error": str(e)}), 500

@app.route('/')
def status():
    return "<h1>Bridge is Online</h1>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
