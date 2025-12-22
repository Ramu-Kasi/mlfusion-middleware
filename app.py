import os
import sys
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq

# 1. INITIALIZE APP FIRST (Fixes NameError)
app = Flask(__name__)

# 2. CONFIGURATION
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# URL for Dhan's Scrip Master
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# GLOBAL RAM CACHE
SCRIP_MASTER_DATA = None

def log_now(msg):
    """Force logs to show up on Render immediately"""
    print(f"!!! [ALGO_ENGINE]: {msg}", file=sys.stderr, flush=True)

def load_scrip_master():
    """Download and cache the CSV in memory on startup"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Downloading Scrip Master into RAM...")
    try:
        # Load only necessary columns to keep memory low
        SCRIP_MASTER_DATA = pd.read_csv(SCRIP_URL, usecols=[
            'SEM_UNDERLYING_SECURITY_ID', 
            'SEM_STRIKE_PRICE', 
            'SEM_OPTION_TYPE', 
            'SEM_INSTRUMENT_NAME', 
            'SEM_SMST_SECURITY_ID'
        ])
        log_now(f"BOOT: Cache Ready with {len(SCRIP_MASTER_DATA)} instruments.")
    except Exception as e:
        log_now(f"BOOT ERROR: Failed to load CSV: {e}")

# LOAD DATA AT STARTUP
load_scrip_master()

def get_atm_id(price, signal):
    """Finds the nearest ATM strike ID in milliseconds"""
    # Math: Round to nearest 100 for Bank Nifty
    strike = round(float(price) / 100) * 100
    opt_type = "CE" if "BUY" in signal.upper() else "PE"
    
    # Filter the cached DataFrame
    match = SCRIP_MASTER_DATA[
        (SCRIP_MASTER_DATA['SEM_UNDERLYING_SECURITY_ID'] == 25) & 
        (SCRIP_MASTER_DATA['SEM_STRIKE_PRICE'] == strike) & 
        (SCRIP_MASTER_DATA['SEM_OPTION_TYPE'] == opt_type) &
        (SCRIP_MASTER_DATA['SEM_INSTRUMENT_NAME'] == 'OPTIDX')
    ]
    
    if not match.empty:
        # Returns the first available expiry ID (usually the current week)
        return str(int(match.iloc[0]['SEM_SMST_SECURITY_ID'])), strike
    return None, strike

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    try:
        # Capture signal from TradingView
        data = request.get_json(force=True, silent=True)
        if not data:
            log_now("ERROR: Invalid JSON received")
            return jsonify({"error": "Invalid JSON"}), 400

        tv_price = data.get("price")
        signal = data.get("message", "")

        # 1. Lookup ATM Security ID
        sec_id, strike = get_atm_id(tv_price, signal)
        
        if not sec_id:
            log_now(f"NOT FOUND: No ID for {strike} {signal}")
            return jsonify({"error": "Strike not found"}), 404

        log_now(f"EXECUTE: {signal} @ ATM {strike} (ID: {sec_id})")

        # 2. Place Market Order (35 units = 1 Lot for BN)
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

        log_now(f"DHAN RESPONSE: {order}")
        return jsonify(order), 200

    except Exception as e:
        log_now(f"CRITICAL ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
