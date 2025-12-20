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
    # BankNifty step = 100, Nifty step = 50
    step = 100 if "BANK" in ticker.upper() else 50
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

    # Signal logic: BUY/STRONG_BUY -> Buy (B) | SELL/STRONG_SELL -> Sell (S)
    trans_type = "B" if "BUY" in signal else "S"
    
    # FORCED QUANTITY TO 1
    qty = "1" 
    
    itm_strike = get_1_itm_ce(price, ticker)

    dhan_order = {
        "secret": "OvWi0",
        "alertType": "multi_leg_order",
        "order_legs": [{
            "transactionType": trans_type,
            "orderType": "MKT",          
            "quantity": qty,             # Always 1
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

    # Deep Logging to see the details in Render
    print(f">>> SIGNAL: {signal} | TICKER: {ticker} | QTY: {qty}")
    print(f">>> ITM STRIKE: {itm_strike}")

    try:
        response = requests.post(DHAN_WEBHOOK_URL, json=dhan_order, timeout=10)
        logging.info(f"DHAN RESPONSE: {response.status_code} | {response.text}")
        
        return jsonify({
            "status": "PROCESSED",
            "dhan_msg": response.text
        }), 200

    except Exception as e:
        logging.error(f"CONNECTION ERROR: {str(e)}")
        return jsonify({"status": "FAILED", "error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
