import os
import sys
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq

app = Flask(__name__)

# --- GLOBAL DATA CACHE ---
# This variable lives in the server's RAM
SCRIP_MASTER_DATA = None

# Dhan Config
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

def log_now(msg):
    print(f"!!! [ALGO_CACHE]: {msg}", file=sys.stderr, flush=True)

def load_scrip_master():
    """Downloads the CSV once on server startup"""
    global SCRIP_MASTER_DATA
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    log_now("BOOT: Downloading Dhan Scrip Master into RAM...")
    
    # We only load specific columns to save memory on Render's free tier
    try:
        SCRIP_MASTER_DATA = pd.read_csv(url, usecols=[
            'SEM_UNDERLYING_SECURITY_ID', 
            'SEM_STRIKE_PRICE', 
            'SEM_OPTION_TYPE', 
            'SEM_INSTRUMENT_NAME', 
            'SEM_SMST_SECURITY_ID',
            'SEM_EXPIRY_DATE'
        ])
        log_now(f"BOOT: Success! Cached {len(SCRIP_MASTER_DATA)} instruments.")
    except Exception as e:
        log_now(f"BOOT ERROR: Failed to load CSV: {e}")

# TRIGGER THE CACHE AT STARTUP
load_scrip_master()

def get_atm_id(price, signal):
    """Instant lookup from RAM"""
    strike = round(float(price) / 100) * 100
    opt_type = "CE" if "BUY" in signal.upper() else "PE"
    
    # Filter the cached dataframe (takes < 0.01 seconds)
    match = SCRIP_MASTER_DATA[
        (SCRIP_MASTER_DATA['SEM_UNDERLYING_SECURITY_ID'] == 25) & 
        (SCRIP_MASTER_DATA['SEM_STRIKE_PRICE'] == strike) & 
        (SCRIP_MASTER_DATA['SEM_OPTION_TYPE'] == opt_type) &
        (SCRIP_MASTER_DATA['SEM_INSTRUMENT_NAME'] == 'OPTIDX')
    ]
    
    if not match.empty:
        # Get the first result (usually the current week's expiry)
        return str(int(match.iloc[0]['SEM_SMST_SECURITY_ID'])), strike
    return None, strike

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    try:
        data = request.get_json(force=True)
        tv_price = data.get("price")
        signal = data.get("message")
        
        # 1. Map Price to Security ID via Cache
        sec_id, strike = get_atm_id(tv_price, signal)
        
        if not sec_id:
            log_now(f"Lookup Failed for {strike} {signal}")
            return jsonify({"error": "Instrument not found in cache"}), 404

        log_now(f"EXECUTE: {signal} | Strike: {strike} | ID: {sec_id}")

        # 2. Fire the Order
        order = dhan.place_order(
            security_id=sec_id,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, # Buying the Option
            quantity=35,
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0,
            validity='DAY'
        )

        return jsonify(order), 200

    except Exception as e:
        log_now(f"CRITICAL ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
