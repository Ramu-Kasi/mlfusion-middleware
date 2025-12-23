import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime

# 1. INITIALIZE APP
app = Flask(__name__)

# 2. CONFIGURATION & STATE
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
TRADE_HISTORY = [] 

def log_now(msg):
    """Force logs to show in Render immediately"""
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    """OPTIMIZED BOOT: Downloads and prunes data once to ensure fast startup"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading Master CSV and filtering Bank Nifty...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Identify columns dynamically
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
        strike_col = next((c for c in df.columns if 'STRIKE' in c.upper()), None)
        id_col = next((c for c in df.columns if 'SECURITY_ID' in c.upper() or 'SMST' in c.upper()), None)
        type_col = next((c for c in df.columns if 'OPTION_TYPE' in c.upper()), None)

        # STRICT FILTER: Bank Nifty NSE Index Options
        mask = (
            (df[inst_col].str.contains('OPTIDX', na=False)) & 
            (df[sym_col].str.contains('BANKNIFTY', case=False, na=False)) &
            (~df[sym_col].str.contains('BANKEX', case=False, na=False))
        )
        if exch_col:
            mask = mask & (df[exch_col].str.contains('NSE', case=False, na=False))

        # Keep ONLY the columns we need
        needed_cols = [id_col, strike_col, type_col, exp_col]
        SCRIP_MASTER_DATA = df[mask][needed_cols].copy()
        
        # Convert dates
        SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
        SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=[exp_col])
        
        log_now(f"BOOT: Success! {len(SCRIP_MASTER_DATA)} Bank Nifty contracts loaded.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

# Run once at startup
load_scrip_master()

def close_opposite_position(type_to_close):
    """Execution with 100ms Reversal"""
    try:
        positions = dhan.get_positions()
        if positions.get('status') == 'success' and positions.get('data'):
            for pos in positions['data']:
                symbol = pos.get('tradingSymbol', '')
                qty = int(pos.get('netQty', 0))
                
                if "BANKNIFTY" in symbol and symbol.endswith(type_to_close) and qty != 0:
                    log_now(f"REVERSAL: Closing {symbol}")
                    dhan.place_order(
                        tag='MLFusion_Exit',
                        transaction_type=dhan.SELL if qty > 0 else dhan.BUY,
                        exchange_segment=dhan.NSE_FNO,
                        product_type=dhan.INTRA,
                        order_type=dhan.MARKET,
                        validity=dhan.DAY,
                        security_id=pos['securityId'],
                        quantity=abs(qty),
                        price=0
                    )
                    time.sleep(0.1) 
        return True
    except Exception as e:
        log_now(f"REVERSAL ERROR: {e}")
        return False

def get_itm_id(price, signal):
    """Memory-lookup for 1-Step ITM"""
    try:
        if SCRIP_MASTER_DATA is None: 
            return None, None
            
        atm_strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        strike = (atm_strike - 100) if opt_type == "CE" else (atm_strike + 100)
        
        id_col, strike_col, type_col, exp_col = SCRIP_MASTER_DATA.columns
        
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ].copy()
        
        if not match.empty:
            today = pd.Timestamp(datetime.now().date())
            match = match[match[exp_col] >= today].sort_values(by=exp_col)
            if not match.empty:
                row = match.iloc[0]
                return str(int(row[id_col])), strike
        return None, strike
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None

@app.route('/')
def dashboard():
    """Live Trading Summary Page"""
    html = """
    <html>
        <head>
            <title>MLFusion Live Summary</title>
            <meta http-equiv="refresh" content="30">
            <style>
                body { font-family: sans-serif; margin: 40px; background: #f4f4f9; }
                table { width: 100%; border-collapse: collapse; background: white; }
                th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
                th { background: #333; color: white; }
                .CE { color: green; font-weight: bold; }
                .PE { color: red; font-weight: bold; }
            </style>
        </head>
        <body>
            <h2>Bank Nifty Live Summary</h2>
            <p><b>Total Signal Count:</b> {{ count }}</p>
            <table>
                <tr><th>Time</th><th>Price</th><th>Strike</th><th>Type</th><th>Dhan Status</th><th>Remarks</th></tr>
                {% for t in trades %}
                <tr>
                    <td>{{ t.time }}</td>
                    <td>{{ t.price }}</td>
                    <td>{{ t.strike }}</td>
                    <td class="{{ t.type }}">{{ t.type }}</td>
                    <td>{{ t.status }}</td>
                    <td>{{ t.remarks }}</td>
                </tr>
                {% endfor %}
            </table>
        </body>
    </html>
    """
    return render_template_string(html, trades=reversed(TRADE_HISTORY), count=len(TRADE_HISTORY))

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    """Main signal handler"""
    log_now(f"SIGNAL RECEIVED: {request.get_data(as_text=True)}")
    trade_info = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "price": "N/A", "strike": "N/A", "type": "N/A", "status": "Pending", "remarks": ""
    }
    try:
        data = request.get_json(force=True, silent=True)
        if not data: 
            return jsonify({"status": "error", "message": "No JSON"}), 400

        signal = data.get("message", "").upper()
        current_price = data.get("price")
        trade_info["price"] = current_price
        
        current_type, opposite_type = ("CE", "PE") if "BUY" in signal else ("PE", "CE")
        trade_info["type"] = current_type

        # 1. Close opposite leg
        close_opposite_position(opposite_type)

        # 2. Get ITM ID
        sec_id, strike = get_itm_id(current_price, signal)
        trade_info["strike"] = strike
        
        if not sec_id:
            trade_info["status"] = "Failure"
            trade_info["remarks"] = "Strike lookup failed"
            TRADE_HISTORY.append(trade_info)
            return jsonify({"status": "not_found"}), 404

        # 3. Buy 1 lot (35 Qty)
        order = dhan.place_order(
            tag='MLFusion_BN',
            transaction_type=dhan.BUY, 
            exchange_segment=dhan.NSE_FNO,
            product_type=dhan.INTRA,
            order_type=dhan.MARKET,
            validity=dhan.DAY,
            security_id=sec_id,
            quantity=35, 
            price=0
        )

        # Update History
        trade_info["status"] = order.get('status', 'failure')
        if trade_info["status"] == "success":
            trade_info["remarks"] = f"OrderID: {order.get('data', {}).get('orderId', 'N/A')}"
        else:
            trade_info["remarks"] = order.get('remarks', 'API Error/Market Closed')

        TRADE_HISTORY.append(trade_info)
        log_now(f"DHAN RESPONSE: {order}")
        return jsonify({"status": trade_info["status"], "order_data": order})

    except Exception as e:
        trade_info["status"] = "Error"
        trade_info["remarks"] = str(e)
        TRADE_HISTORY.append(trade_info)
        log_now(f"HANDLER ERROR: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
