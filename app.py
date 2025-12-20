import os
import logging
import sys
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def get_1_itm_ce(price, ticker):
    """Always calculates 1-ITM CE (Call Option)."""
    price = float(price)
    # BankNifty step = 100, Nifty step = 50
    step = 100 if "BANK" in ticker.upper() else 50
    atm_strike = int(round(price / step) * step)
    return atm_strike - step  # 1-ITM CE is one step below ATM

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json()
    if not data:
        return jsonify({"status": "ERROR", "reason": "No data"}), 400

    # These values come from your TradingView alert message
    signal = data.get("message", "").upper() 
    ticker = data.get("ticker", "BANKNIFTY")
    price = data.get("price", 0)

    # LOGIC: If the message contains "BUY", we go Long (B)
    # If it contains "SELL", we Exit (S)
    trans_type = "B" if "BUY" in signal else "S"
    
    # 1 Lot Quantity (BankNifty=15, Nifty=25)
    qty = "15" if "BANK" in ticker.upper() else "25"
    itm_strike = get_1_itm_ce(price, ticker)

    # FINAL DHAN JSON with all required parameters
    dhan_order = {
        "secret": "OvWi0",
        "alertType": "multi_leg_order",
        "order_legs": [{
            "transactionType": trans_type,
            "orderType": "MKT",          # Market Order
            "quantity": qty,             # 1 Lot
            "exchange": "NSE",           # NSE F&O
            "symbol": ticker,
            "instrument": "OPT",
            "productType": "M",          # Margin
            "sort_order": "1",
            "price": "0",                
            "option_type": "CE",         # Always trading the CE
            "strike_price": str(float(itm_strike)),
            "expiry_date": "2025-12-30"  # Update this to current expiry
        }]
    }

    logging.info(f"SIGNAL: {signal} | ACTION: {trans_type} | STRIKE: {itm_strike} CE")
    
    # Ready to send to Dhan
    return jsonify({"status": "SUCCESS", "dhan_json": dhan_order}), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
