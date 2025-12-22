import os
import sys
from flask import Flask, request, jsonify
from dhanhq import dhanhq

app = Flask(__name__)

# --- DHAN CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

def log_now(msg):
    print(f"!!! [ORDER_MODE]: {msg}", file=sys.stderr, flush=True)

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    log_now("Signal Received. Buying 1 Lot (35 units) of ID 51439...")
    
    try:
        # 1. PLACE THE ORDER
        order = dhan.place_order(
            security_id='51439',       
            exchange_segment=dhan.NSE_FNO, 
            transaction_type=dhan.BUY, 
            quantity=35,               # CORRECTED: 35 units = 1 Lot
            order_type=dhan.MARKET,    
            product_type=dhan.MARGIN,  
            price=0,                   
            validity='DAY'             
        )

        log_now(f"Order Response: {order}")
        return jsonify(order), 200

    except Exception as e:
        log_now(f"CRITICAL ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
