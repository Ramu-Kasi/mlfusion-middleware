import os
import logging
import sys
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# YOUR UNIQUE DHAN WEBHOOK URL
DHAN_WEBHOOK_URL = "https://tv-webhook.dhan.co/tv/alert/5fa02e0ded734d27888fbef62ee1cbc2/FOOE21573Z"

def get_1_itm_ce(price, ticker):
    price = float(price)
    step = 100 if "BANK" in ticker.upper() else 50
    # Round to nearest ATM then subtract 1 step for ITM
    atm_strike = int(round(price / step) * step)
    return atm_strike - step 

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json()
    if not data:
        return jsonify({"status": "ERROR", "reason": "No data"}), 400

    signal = data.get("message", "").upper() 
    ticker = data.get("ticker", "BANKNIFTY")
    price = data.get("price", 0)

    # Logic: BUY/STRONG_BUY -> Buy (B) | SELL/STRONG_SELL -> Sell (S)
    trans_type = "B" if "BUY" in signal else "S"
    qty = "15" if "BANK" in ticker.upper() else "25"
    itm_strike = get_1_itm_ce(price, ticker)

    # Prepare the JSON specifically for your unique Dhan link
    dhan_order = {
        "secret": "OvWi0",
        "alertType": "multi_leg_order",
        "order_legs": [{
            "transactionType": trans_type,
            "orderType": "MKT",          
            "quantity": qty,             
            "exchange": "NSE",           
            "symbol": ticker,
            "instrument": "OPT",
            "productType": "M",          
            "sort_order": "1",
            "price": "0",                
            "option_type": "CE",         
            "strike_price": str(float(itm_strike)),
            "expiry_date": "2025-12-30"  
        }]
    }

    # SENDING TO YOUR UNIQUE DHAN LINK
    try:
        logging.info(f"Sending {signal} to Dhan: {itm_strike} CE")
        response = requests.post(DHAN_WEBHOOK_URL, json=dhan_order, timeout=10)
        
        # Log Dhan's actual response so we can troubleshoot
        logging.info(f"DHAN STATUS: {response.status_code} | RESPONSE: {response.text}")
        
        return jsonify({
            "status": "PROCESSED",
            "dhan_response": response.text
        }), 200

    except Exception as e:
        logging.error(f"CRITICAL ERROR: {str(e)}")
        return jsonify({"status": "SERVER_ERROR", "error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
