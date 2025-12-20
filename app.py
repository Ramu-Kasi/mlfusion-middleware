import os
import logging
import sys
from flask import Flask, request, jsonify

# 1. Initialize the app FIRST to avoid NameError
app = Flask(__name__)

# Configure logging for Render logs
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def get_atm_strike(price, ticker):
    """Calculates ATM strike based on ticker type"""
    price = float(price)
    if "BANKNIFTY" in ticker:
        step = 100
    elif "NIFTY" in ticker:
        step = 50
    elif "BTC" in ticker:
        step = 100  # Example step for BTC options
    else:
        step = 10   # Default for other stocks
    return int(round(price / step) * step)

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json()
    if not data:
        return jsonify({"status": "ERROR", "reason": "No data"}), 400

    # Get dynamic data from TradingView alert message
    message = data.get("message", "").upper()
    price = data.get("price", 0)
    ticker = data.get("ticker", "BTCUSD")

    # Calculate the ATM strike using the rounding function
    atm_strike = get_atm_strike(price, ticker)
    
    logging.info(f"RECEIVED: {ticker} {message} at {price} | ATM STRIKE: {atm_strike}")
    
    return jsonify({
        "status": "SUCCESS",
        "signal": message,
        "atm_strike": atm_strike
    }), 200

if __name__ == '__main__':
    # Use 'PORT' from Render environment or default to 5000
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
