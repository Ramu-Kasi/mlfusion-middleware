import os
import sys
import json
import requests
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
        print(f"\n--- [STEP 1] INCOMING ALERT ---\n{data}", flush=True)
        
        signal = str(data.get("message", "")).upper()
        ticker = data.get("ticker", "BANKNIFTY")
        price_val = float(data.get("price", 0))
        
        # Strike Selection Logic
        new_opt_type = "CE" if "BUY" in signal else "PE"
        step = 100
        atm = int(round(price_val / step) * step)
        itm_strike = (atm - step) if new_opt_type == "CE" else (atm + step)

        # BUILD DHAN PAYLOAD (Added "price": "0")
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
                    "price": "0", # <--- Added missing field
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
                    "price": "0", # <--- Added missing field
                    "option_type": new_opt_type,
                    "strike_price": f"{float(itm_strike):.1f}", 
                    "expiry_date": EXPIRY_DATE
                }
            ]
        }

        # Print the final payload for verification
        print("\n--- [STEP 2] PAYLOAD BEING SENT ---", flush=True)
        print(json.dumps(dhan_payload, indent=4), flush=True)

        # HIT WEBHOOK
        resp = requests.post(DHAN_WEBHOOK_URL, json=dhan_payload, timeout=15)
        
        print(f"--- [STEP 3] DHAN RESPONSE ---\n{resp.text}", flush=True)
        
        return jsonify({"bridge_status": "success", "dhan_raw": resp.text}), 200

    except Exception as e:
        print(f"!!! BRIDGE CRASHED: {str(e)}", flush=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
