import os
import sys
import json
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- CONFIGURATION ---
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    try:
        data = request.get_json()
        print(f"\n--- [STEP 1] INCOMING ALERT FROM TV ---\n{data}", flush=True)

        # EXACT PAYLOAD AS PER YOUR DHAN JSON
        dhan_payload = {
            "secret": "OvWi0",
            "alertType": "multi_leg_order",
            "order_legs": [
                {
                    "transactionType": "B",
                    "orderType": "MKT",
                    "quantity": "1",
                    "exchange": "NSE",
                    "symbol": "BANKNIFTY",
                    "instrument": "OPT",
                    "productType": "M",
                    "sort_order": "1",
                    "price": "0",
                    "option_type": "CE",
                    "strike_price": "59500.0",
                    "expiry_date": "2025-12-30"
                }
            ]
        }

        # MIMIC TRADINGVIEW HEADERS
        # This is critical because Dhan's /tv/alert/ path often rejects 
        # requests that don't look like they came from TradingView.
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "TradingView-Webhook/1.0"
        }

        print("\n--- [STEP 2] PAYLOAD BEING SENT TO DHAN ---", flush=True)
        print(json.dumps(dhan_payload, indent=4), flush=True)

        # HIT DHAN WEBHOOK
        resp = requests.post(
            DHAN_WEBHOOK_URL, 
            json=dhan_payload, 
            headers=headers, 
            timeout=15
        )
        
        print(f"\n--- [STEP 3] DHAN RESPONSE ---\n{resp.text} (Code: {resp.status_code})", flush=True)
        
        return jsonify({"bridge_status": "executed", "dhan_raw": resp.text}), 200

    except Exception as e:
        print(f"!!! BRIDGE CRASHED: {str(e)}", flush=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
