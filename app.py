import os
import sys
from flask import Flask, request, jsonify
from dhanhq import dhanhq

app = Flask(__name__)

# --- DHAN SETUP ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

def log_now(msg):
    print(f"!!! [TEST_ID_MODE]: {msg}", file=sys.stderr, flush=True)

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_now("Testing with Security ID: 51417")
    
    try:
        data = request.get_json(force=True, silent=True)
        signal = data.get("message", "").upper()
        
        # --- THE BABY STEP ACTION ---
        # Instead of placing a real order, we call the 'get_security_details'
        # or 'get_fund_limits' to prove we can 'talk' to Dhan about this ID.
        
        # For this test, we will just LOG exactly what a BUY order WOULD look like.
        target_id = "51416"
        
        log_now(f"SIGNAL RECEIVED: {signal}")
        log_now(f"ACTION: I would now place a MARKET {signal} for Security ID {target_id}")
        
        # PROOF OF CONNECTION: Just fetch your margin to ensure the API is 'live'
        funds = dhan.get_fund_limits()
        balance = funds['data']['availabelBalance'] if funds.get('status') == 'success' else "Error"
        
        return jsonify({
            "test_status": "Success",
            "simulated_id": target_id,
            "current_balance_verified": balance,
            "message": f"Ready to trade ID {target_id}"
        }), 200

    except Exception as e:
        log_now(f"ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
