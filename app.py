import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# CONFIGURATION
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"
DHAN_SECRET = "OvWi0"
EXPIRY_DATE = "2025-12-30"

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    try:
        data = request.get_json()
        print(f"DEBUG INCOMING: {data}", flush=True)

        # 1. BARE MINIMUM PAYLOAD (Single Order)
        # Note: alertType is changed to 'single_order' or removed for standard orders
        dhan_payload = {
            "secret": DHAN_SECRET,
            "transactionType": "B",
            "orderType": "MKT",
            "quantity": "1",     # Standard lot for Bank Nifty
            "exchange": "NSE",
            "symbol": "BANKNIFTY",
            "instrument": "OPT",
            "productType": "M",
            "price": "0",
            "option_type": "CE",
            "strike_price": "59200.0",
            "expiry_date": EXPIRY_DATE
        }

        print(f"DEBUG OUTGOING: {dhan_payload}", flush=True)

        # 2. HIT DHAN
        resp = requests.post(DHAN_WEBHOOK_URL, json=dhan_payload, timeout=15)
        
        print(f"DEBUG RESPONSE: {resp.text} (Code: {resp.status_code})", flush=True)
        
        return jsonify({"dhan_response": resp.text}), 200

    except Exception as e:
        print(f"DEBUG ERROR: {e}", flush=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
