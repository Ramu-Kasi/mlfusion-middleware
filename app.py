import os
import logging
import sys
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# Basic logging setup to see activity in Render logs
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# --- CONFIGURATION ---
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"
DHAN_SECRET = "OvWi0"
EXPIRY_DATE = "2025-12-30"  # IMPORTANT: Update this weekly for the current contract

def get_itm_strike(price, ticker, opt_type):
    """Calculates the 1-Step In-The-Money strike price."""
    price = float(price)
    # Step size: BankNifty = 100, Nifty = 50
    step = 100 if "BANK" in ticker.upper() else 50
    atm_strike = int(round(price / step) * step)
    
    if opt_type == "CE":
        return atm_strike - step  # Call ITM is below current price
    else:
        return atm_strike + step  # Put ITM is above current price

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json()
    if not data:
        return jsonify({"status": "ERROR", "reason": "No data received"}), 400

    # Extract info from TradingView Alert
    signal = data.get("message", "").upper() 
    ticker = data.get("ticker", "BANKNIFTY")
    price = data.get("price", 0)

    # Determine what we want to hold now
    new_opt_type = "CE" if "BUY" in signal else "PE"
    itm_strike = get_itm_strike(price, ticker, new_opt_type)

    # --- DHAN MULTI-LEG PAYLOAD ---
    # Leg 1: Closes any existing open position for THIS ticker only.
    # Leg 2: Opens the new 1-ITM position.
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
                "manage_position": "EXIT_ALL" # Target ONLY the ticker in this leg
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

    # Console output for Render Logs
    print(f">>> RECEIVED: {signal} for {ticker} @ {price}")
    print(f">>> REVERSAL: Exiting old {ticker} positions -> Buying 1-ITM {new_opt_type} at {itm_strike}")

    try:
        response = requests.post(DHAN_WEBHOOK_URL, json=dhan_payload, timeout=15)
        logging.info(f"Dhan Status: {response.status_code} | Response: {response.text}")
        return jsonify({"status": "SUCCESS", "dhan_response": response.text}), 200
    except Exception as e:
        logging.error(f"CRITICAL ERROR: {str(e)}")
        return jsonify({"status": "FAILED", "error": str(e)}), 500

if __name__ == '__main__':
    # Use 'use_reloader=False' to prevent the double-logging issue
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
