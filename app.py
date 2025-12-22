import os
import sys
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq

# 1. INITIALIZE APP
app = Flask(__name__)

# 2. CONFIGURATION
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# Dhan Scrip Master URL
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# GLOBAL RAM CACHE
SCRIP_MASTER_DATA = None

def log_now(msg):
    print(f"!!! [ALGO_ENGINE]: {msg}", file=sys.stderr, flush=True)

def load_scrip_master():
    """Robust CSV loader for Bank Nifty"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Fetching Scrip Master...")
    try:
        # Load full CSV
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Filter for Index Options (OPTIDX) where Underlying is Bank Nifty (25)
        SCRIP_MASTER_DATA = df[
            (df['SEM_INSTRUMENT_NAME'] == 'OPTIDX') & 
            (df['SEM_UNDERLYING_SECURITY_ID'] == 25)
        ].copy()
        
        log_now(f"BOOT: Success! {len(SCRIP_MASTER_DATA)} Bank Nifty contracts cached.")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

# Load data on start
load_scrip_master()

def get_atm_id(price, signal):
    """Finds nearest ATM strike ID"""
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty:
            return None, None
            
        # Round price to nearest 100
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Filter for Strike + Type
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA['SEM_STRIKE_PRICE'] == strike) & 
            (SCRIP_MASTER_DATA['SEM_OPTION_TYPE'] == opt_type)
        ]
        
        if not match.empty:
            # Sort by expiry to get nearest contract
            match = match.sort_values(by='SEM_EXPIRY_DATE')
            return str(int(match.iloc[0]['SEM_SMST_SECURITY_ID'])), strike
            
        return None, strike
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    # Log incoming request for safety
    raw_body = request.get_data(as_text=True)
    log_now(f"SIGNAL: {raw_body}")

    try:
        # FIXED: Balanced parentheses below
        data = request.get_json(force=True, silent=True)
        
        if not data:
            log_now("ERROR: Invalid JSON structure")
            return jsonify({"status": "error", "message": "Invalid JSON"}), 400

        tv_price = data.get("price")
        signal = data.get("message", "")

        # 1. Lookup ATM Security ID
        sec_id, strike = get_atm_id(tv_price, signal)
        
        if not sec_id:
            log_now(f"NOT FOUND: Strike {strike}")
            return jsonify({"status": "not_found"}), 404

        log_now(f"EXECUTE: {signal} ATM {strike} (ID: {sec_id})")

        # 2. PLACE ORDER (QTY 35)
        order = dhan.place_order(
            security_id=sec_id,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, 
            quantity=35, 
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0,
            validity='DAY'
        )

        log_now(f"RESPONSE: {order}")
        return jsonify(order), 200

    except Exception as e:
        log_now(f"CRITICAL: {str(e)}")
        return jsonify({"status": "error", "reason": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
