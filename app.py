import os
from flask import Flask, request, jsonify
from dhanhq import dhanhq

app = Flask(__name__)

# --- CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

def surgical_reversal(signal_type):
    """
    Closes only the OPPOSITE position. 
    If signal is BUY, close open PE. If signal is SELL, close open CE.
    """
    try:
        positions_resp = dhan.get_positions()
        if positions_resp.get('status') == 'success':
            for pos in positions_resp.get('data', []):
                symbol = pos.get('tradingSymbol', '').upper()
                net_qty = int(pos.get('netQty', 0))
                
                if "BANKNIFTY" in symbol and net_qty != 0:
                    # Logic: If signal is BUY, we want to clear any existing SELL (PE) positions
                    # In our bot, BUY signal = BUY CALL. SELL signal = BUY PUT.
                    
                    is_call = "CE" in symbol
                    is_put = "PE" in symbol
                    
                    # CASE 1: We get a BUY signal but we are holding a PUT
                    if signal_type == "BUY" and is_put:
                        print(f"!!! [REVERSAL]: New BUY signal. Closing existing PUT: {symbol}")
                        exit_position(pos)
                        
                    # CASE 2: We get a SELL signal but we are holding a CALL
                    elif signal_type == "SELL" and is_call:
                        print(f"!!! [REVERSAL]: New SELL signal. Closing existing CALL: {symbol}")
                        exit_position(pos)
                        
                    # CASE 3: Signal matches what we hold (e.g., BUY signal + holding CE)
                    else:
                        print(f"!!! [HOLD]: Signal matches current position {symbol}. No action.")
                        
        return True
    except Exception as e:
        print(f"!!! [REVERSAL ERROR]: {e}")
        return False

def exit_position(pos):
    """Helper to execute the market exit order"""
    exit_side = dhan.SELL if int(pos['netQty']) > 0 else dhan.BUY
    dhan.place_order(
        security_id=pos['securityId'],
        exchange_segment=pos['exchangeSegment'],
        transaction_type=exit_side,
        quantity=abs(int(pos['netQty'])),
        order_type=dhan.MARKET,
        product_type=pos['productType']
    )

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    signal = data.get('signal', '').upper() # Expecting "BUY" or "SELL"
    
    # 1. Run the surgical check
    surgical_reversal(signal)
    
    # 2. Proceed to place the new signal's order
    # (Your existing logic for Finding Strike & Placing Order goes here)
    
    return jsonify({"status": "processed"}), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
