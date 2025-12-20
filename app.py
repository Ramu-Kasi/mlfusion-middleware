import os
import logging
import sys
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# --- GLOBAL VARIABLES FOR THE STATUS PAGE ---
last_signal = "None"
last_strike = "None"
last_status = "Waiting for first signal..."

# --- CONFIGURATION ---
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"
DHAN_SECRET = "OvWi0"
EXPIRY_DATE = "2025-12-30" 

def get_itm_strike(price, ticker, opt_type):
    price = float(price)
    step = 100 if "BANK" in ticker.upper() else 50
    atm_strike = int(round(price / step) * step)
    return (atm_strike - step) if opt_type == "CE" else (atm_strike + step)

# --- NEW: STATUS PAGE ENDPOINT ---
@app.route('/', methods=['GET'])
def health_check():
    return f"""
    <html>
        <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
            <h1>🚀 ML Fusion Bridge is LIVE</h1>
            <p><strong>Last Signal:</strong> {last_signal}</p>
            <p><strong>Last ITM Strike:</strong> {last_strike}</p>
            <p><strong>Dhan Response:</strong> {last_status}</p>
            <hr width="50%">
            <p style="color: gray;">Refreshed at: {EXPIRY_DATE} Expiry Setup</p>
        </body>
    </html>
    """, 200

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    global last_signal, last_strike, last_status
    data = request.get_json()
    
    signal = data.get("message", "").upper() 
    ticker = data.get("ticker", "BANKNIFTY")
    price = data.get("price", 0)

    new_opt_type = "CE" if "BUY" in signal else "PE"
    itm_strike = get_itm_strike(price, ticker, new_opt_type)

    # Update status for the homepage
    last_signal = f"{signal} on {ticker}"
    last_strike = f"{itm_strike} ({new_opt_type})"

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
                "strike_price": str(float(itm_strike)),
                "expiry_date": EXPIRY_DATE
            }
        ]
    }

    try:
        response = requests.post(DHAN_WEBHOOK_URL, json=dhan_payload, timeout=15)
        last_status = f"Success (Code {response.status_code})"
        return jsonify({"status": "SUCCESS"}), 200
    except Exception as e:
        last_status = f"Error: {str(e)}"
        return jsonify({"status": "FAILED"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
