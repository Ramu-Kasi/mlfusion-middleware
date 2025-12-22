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
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

def log_now(msg):
    """Force immediate logs for Render visibility"""
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def get_atm_id(price, signal):
    """Live Pull Logic: Downloads and finds the strike in real-time"""
    try:
        if not price or str(price).lower() == "none":
            log_now("ERROR: No price received in signal.")
            return None, None, None

        log_now(f"LIVE FETCH: Downloading Scrip Master for Price {price}...")
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Determine Strike and Type
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Flexible Column Detection to prevent KeyErrors
        cols = df.columns
        inst_c = next((c for c in cols if 'INSTRUMENT_NAME' in c.upper()), None)
        und_c = next((c for c in cols if 'UNDERLYING_SECURITY_ID' in c.upper()), None)
        s_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        t_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        i_col = next((c for c in cols if 'SECURITY_ID' in c.upper()), None)
        e_col = next((c for c in cols if 'EXPIRY_DATE' in c.upper()), None)
        q_col = next((c for c in cols if 'LOT' in c.upper()), None)

        # Filter: Bank Nifty (25) + Instrument + Strike + Type
        match = df[
            (df[inst_c].str.contains('OPTIDX', na=False)) & 
            (df[und_c] == 25) &
            (df[s_col] == strike) & 
            (df[t_col] == opt_type)
        ].copy()
        
        if not match.empty:
            # Sort by Expiry Date to get the current contract
            if e_col:
                match[e_col] = pd.to_datetime(match[e_col], errors='coerce')
                match = match.dropna(subset=[e_col]).sort_values(by=e_col)
            
            row = match.iloc[0]
            final_id = str(int(row[i_col]))
            qty = int(row[q_col]) if q_col else 35 # Default to 35 if lot size missing
            
            log_now(f"MATCH FOUND: {row.get('SEM_TRADING_SYMBOL', 'Contract')} -> ID {final_id}")
            return final_id, strike, qty
            
        return None, strike, 35
    except Exception as e:
        log_now(f"FETCH ERROR: {e}")
        return None, None, 35

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    # Capture raw data immediately
    raw_payload = request.get_data(as_text=True)
    log_now(f"SIGNAL RECEIVED: {raw_payload}")
    
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"status": "error", "message": "Invalid JSON"}), 400

        # Run the Live Pull
        sec_id, strike, qty = get_atm_id(data.get("price"), data.get("message", ""))
        
        if not sec_id:
            log_now(f"FAILED: Strike {strike} not found in live data.")
            return jsonify({"status": "not_found"}), 404

        log_now(f"EXECUTE: Sending Order for SecurityId {sec_id} with Qty {qty}")

        order = dhan.place_order(
            security_id=sec_id,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, 
            quantity=qty, 
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0,
            validity='DAY'
        )

        log_now(f"DHAN RESPONSE: {order}")
        return jsonify(order), 200

    except Exception as e:
        log_now(f"RUNTIME ERROR: {str(e)}")
        return jsonify({"status": "error", "reason": str(e)}), 500

@app.route('/')
def health():
    return "BRIDGE_ACTIVE", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
