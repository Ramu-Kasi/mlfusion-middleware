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
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def get_atm_id(price, signal):
    """Memory-safe chunked reader to prevent 'FETCH ERROR: None'"""
    try:
        if not price or str(price).lower() == "none":
            return None, None, None

        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        log_now(f"START CHUNKED FETCH: Seeking {strike} {opt_type}...")

        # We read the file in small chunks of 50,000 rows to save RAM
        chunks = pd.read_csv(SCRIP_URL, low_memory=False, chunksize=50000)
        
        matches = []
        for df_chunk in chunks:
            # Flexible column detection for each chunk
            cols = df_chunk.columns
            inst_c = next((c for c in cols if 'INSTRUMENT_NAME' in c.upper()), None)
            und_c = next((c for c in cols if 'UNDERLYING_SECURITY_ID' in c.upper()), None)
            s_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
            t_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)

            # Filter current chunk
            temp_match = df_chunk[
                (df_chunk[inst_c].str.contains('OPTIDX', na=False)) & 
                (df_chunk[und_c] == 25) &
                (df_chunk[s_col] == strike) & 
                (df_chunk[t_col] == opt_type)
            ].copy()
            
            if not temp_match.empty:
                matches.append(temp_match)
        
        if matches:
            final_df = pd.concat(matches)
            # Find ID and Expiry columns
            id_col = next((c for c in final_df.columns if 'SECURITY_ID' in c.upper()), None)
            exp_col = next((c for c in final_df.columns if 'EXPIRY_DATE' in c.upper()), None)
            lot_col = next((c for c in final_df.columns if 'LOT' in c.upper()), None)

            if exp_col:
                final_df[exp_col] = pd.to_datetime(final_df[exp_col], errors='coerce')
                final_df = final_df.dropna(subset=[exp_col]).sort_values(by=exp_col)
            
            row = final_df.iloc[0]
            final_id = str(int(row[id_col]))
            qty = int(row[lot_col]) if lot_col else 35
            
            log_now(f"MATCH FOUND: {row.get('SEM_TRADING_SYMBOL', 'Contract')} -> ID {final_id}")
            return final_id, strike, qty
            
        return None, strike, 35
    except Exception as e:
        log_now(f"FETCH ERROR: {e}")
        return None, None, 35

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_now(f"SIGNAL: {request.get_data(as_text=True)}")
    try:
        data = request.get_json(force=True, silent=True)
        if not data: return jsonify({"status": "error"}), 400

        sec_id, strike, qty = get_atm_id(data.get("price"), data.get("message", ""))
        
        if not sec_id:
            log_now(f"FAILED: Strike {strike} not found.")
            return jsonify({"status": "not_found"}), 404

        order = dhan.place_order(
            security_id=sec_id, exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, quantity=qty, 
            order_type=dhan.MARKET, product_type=dhan.MARGIN,
            price=0, validity='DAY'
        )
        log_now(f"DHAN: {order}")
        return jsonify(order), 200
    except Exception as e:
        log_now(f"ERROR: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/')
def health(): return "ACTIVE", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
