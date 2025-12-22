import os
import sys
from flask import Flask, request, jsonify
from dhanhq import dhanhq

# 1. INITIALIZE APP
app = Flask(__name__)

# 2. CONFIGURATION
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# 3. STATIC DATA (Valid for Current Expiry)
# Since you only trade 2-3 times, we use the IDs for the active Bank Nifty week.
# To find these, look at your Dhan terminal or the last successful CSV log.
SCRIP_LOOKUP = {
    "52400_CE": "86432", # Replace with actual IDs from your terminal
    "52400_PE": "86433",
    "52500_CE": "86434",
    "52500_PE": "86435",
    "52600_CE": "86436",
    "52600_PE": "86437"
}

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def get_atm_id(price, signal):
    """Instant lookup without any CSV download headache"""
    try:
        if not price or str(price).lower() == "none":
            return None, None
            
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        key = f"{strike}_{opt_type}"
        
        log_now(f"STATIC LOOKUP: Searching for {key}...")
        
        # Pull ID from our hardcoded list
        sec_id = SCRIP_LOOKUP.get(key)
        
        if sec_id:
            log_now(f"SUCCESS: Found Static ID {sec_id}")
            return sec_id, strike
            
        log_now(f"MISSING: Strike {strike} not in static list.")
        return None, strike
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_now(f"SIGNAL: {request.get_data(as_text=True)}")
    try:
        data = request.get_json(force=True, silent=True)
        if not data: return jsonify({"status": "error"}), 400

        # Instant lookup - No more 'FETCH ERROR'
        sec_id, strike = get_atm_id(data.get("price"), data.get("message", ""))
        
        if not sec_id:
            return jsonify({"status": "strike_not_in_list"}), 404

        order = dhan.place_order(
            security_id=sec_id, exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, quantity=30, # Hardcoded lot for BN
            order_type=dhan.MARKET, product_type=dhan.MARGIN,
            price=0, validity='DAY'
        )
        log_now(f"ORDER PLACED: {order}")
        return jsonify(order), 200
    except Exception as e:
        log_now(f"FATAL: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/')
def health(): return "BRIDGE_ACTIVE", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
