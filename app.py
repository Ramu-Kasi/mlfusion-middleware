import os
import sys
import json
from flask import Flask, request, jsonify
from dhanhq import dhanhq

app = Flask(__name__)

# --- SECURE CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')

# Initialize Dhan
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# --- LOGGING HELPER ---
def log_debug(message):
    """Force print to Render's stderr logs immediately"""
    print(f"!!! DEBUG: {message}", file=sys.stderr, flush=True)

# --- ROUTES ---

@app.route('/', methods=['GET', 'HEAD'])
def home():
    log_debug("Home route hit (Root)")
    return "Dhan Bridge is Online", 200

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_debug("--- NEW SIGNAL DETECTED ---")
    
    try:
        # 1. Capture Raw Data first to see exactly what TV is sending
        raw_data = request.get_data(as_text=True)
        log_debug(f"Raw Body Received: {raw_data}")

        # 2. Parse JSON (force=True ignores the 'Content-Type' header)
        data = request.get_json(force=True, silent=True)
        
        if not data:
            log_debug("FAILED: Request body is not valid JSON.")
            return jsonify({"error": "Invalid JSON"}), 400

        log_debug(f"Parsed JSON Data: {json.dumps(data)}")

        # 3. Extract logic
        signal = data.get("message", "").upper()
        ticker = data.get("ticker", "BANKNIFTY")
        
        log_debug(f"Processing Signal: {signal} for {ticker}")

        # 4. Connection Test (Baby Step)
        funds = dhan.get_fund_limits()
        if funds.get('status') == 'success':
            bal = funds['data']['availabelBalance']
            log_debug(f"Dhan Connection Verified. Balance: {bal}")
        else:
            log_debug(f"Dhan API Error: {funds}")

        return jsonify({"status": "success", "received": data}), 200

    except Exception as e:
        log_debug(f"CRITICAL ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Render binding
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
