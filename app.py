import os
import logging
import sys
from flask import Flask, request, jsonify

app = Flask(__name__)

# Standard logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def get_atm_strike(price, step=100):
    """Rounds market price to nearest strike (100 for BankNifty, 50 for Nifty)"""
    return int(round(float(price) / step) * step)

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json()
    
    # Get values from TradingView JSON
    message = data.get("message", "").upper()
    raw_price = data.get("price", 0) # This picks up {{close}}
    ticker = data.get("ticker", "BANKNIFTY")

    # Determine strike step (BankNifty=100, Nifty=50)
    step = 50 if "NIFTY" in ticker and "BANK" not in ticker else 100
    
    # Calculate the ATM Strike
    atm_strike = get_atm_strike(raw_price, step)
    
    logging.info(f"Signal: {message} | Price: {raw_price} | ATM Strike: {atm_strike}")

    # Return the clean strike to confirm it worked
    return jsonify({
        "status": "SUCCESS",
        "signal": message,
        "atm_strike": atm_strike
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
