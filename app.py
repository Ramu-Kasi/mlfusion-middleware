import os
import logging
import sys
from flask import Flask, request, jsonify

app = Flask(__name__)

# Configure logging for Render
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def get_itm_strike(price, ticker, signal):
    """Calculates 1-Strike ITM based on signal and ticker."""
    price = float(price)
    signal = signal.upper()
    
    # Define strike intervals (steps)
    if "BANKNIFTY" in ticker:
        step = 100
    elif "NIFTY" in ticker:
        step = 50
    elif "BTC" in ticker:
        step = 100
    else:
        step = 10

    # 1. Find the ATM strike first
    atm_strike = int(round(price / step) * step)

    # 2. Shift for 1 ITM
    # If BUY (Call), ITM is one step LOWER
    if "BUY" in signal:
        itm_strike = atm_strike - step
    # If SELL (Put), ITM is one step HIGHER
    elif "SELL" in signal:
        itm_strike = atm_strike + step
    else:
        itm_strike = atm_strike
        
    return itm_strike

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json()
    if not data:
        return jsonify({"status": "ERROR", "reason": "No data"}), 400

    message = data.get("message", "").upper()
    price = data.get("price", 0)
    ticker = data.get("ticker", "UNKNOWN")

    # Calculate 1 ITM Strike
    target_strike = get_itm_strike(price, ticker, message)
    
    logging.info(f"SIGNAL: {message} | PRICE: {price} | 1-ITM STRIKE: {target_strike}")
    
    return jsonify({
        "status": "SUCCESS",
        "signal": message,
        "itm_strike": target_strike
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
