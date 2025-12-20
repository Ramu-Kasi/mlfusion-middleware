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
    atm_strike = int(round(price / step) * step)
    return atm_strike - step 

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json()
    if not data:
        return jsonify({"status": "ERROR", "reason": "No data"}), 400

    # For testing BTCUSD, ticker will be "BTCUSD"
    signal = data.get("message", "").upper() 
    ticker = data.get("ticker", "BANKNIFTY")
    price = data.get("price", 0)

    trans_type = "B" if "BUY" in signal else "S"
    qty = "15" if "BANK" in ticker.upper() else "25"
    itm_strike = get_1_itm_ce(price, ticker)

    dhan_order = {
        "secret": "OvWi0",
        "alertType": "multi_leg_order",
        "order_legs": [{
            "transactionType": trans_type,
            "orderType": "MKT",          
            "quantity": qty,             
            "exchange": "NSE",           
            "symbol": ticker, # This will be BTCUSD during your test
            "instrument": "OPT",
            "productType": "M",          
            "sort_order": "1",
            "price": "0",                
            "option_type": "CE",         
            "strike_price": str(float(itm_strike)),
            "expiry_date": "2025-12-30"  
        }]
    }

    try:
        # LOGGING: See exactly what we are sending
        logging.info(f"TESTING WITH {ticker}: Sending payload to Dhan...")
        
        response = requests.post(DHAN_WEBHOOK_URL, json=dhan_order, timeout=10)
        
        # LOGGING: See Dhan's rejection message
        logging.info(f"DHAN RESPONSE CODE: {response.status_code}")
        logging.info(f"DHAN RESPONSE TEXT: {response.text}")
        
        return jsonify({
            "status": "TEST_COMPLETE",
            "dhan_received": response.text
        }), 200

    except Exception as e:
        logging.error(f"DEPLOYMENT ERROR: {str(e)}")
        return jsonify({"status": "FAILED", "error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
