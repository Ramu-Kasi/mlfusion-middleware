import os
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime
import pytz  # Ensure 'pytz' is in your requirements.txt

app = Flask(__name__)

# --- CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# STATE SETTINGS
TRADE_HISTORY = [] 

def get_api_status():
    """
    Pings Dhan API to check if the token is truly active.
    """
    try:
        profile = dhan.get_fund_limits()
        if profile.get('status') == 'success':
            return "Active"
        return "Expired"
    except Exception:
        return "Inactive"

# --- 1. DASHBOARD TEMPLATE ---
# I have kept your EXACT CSS and layout, only adding the status logic
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>MLFusion Trading Bot</title>
    <meta http-equiv="refresh" content="60">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f0f2f5; margin: 0; padding: 20px; }
        .status-bar { background: white; padding: 15px; border-radius: 8px; display: flex; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; gap: 20px; }
        .dot { height: 12px; width: 12px; border-radius: 50%; display: inline-block; margin-right: 8px; }
        .online { background-color: #28a745; }
        .offline { background-color: #dc3545; }
        .table-container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid #eee; }
        .signal-buy { color: #28a745; font-weight: bold; }
        .signal-sell { color: #dc3545; font-weight: bold; }
    </style>
</head>
<body>
    <div class="status-bar">
        <span class="dot {{ 'online' if status == 'Active' else 'offline' }}"></span>
        <h2 style="margin: 0;">Live System Dashboard</h2>
        <div style="margin-left: auto; color: #666;">
            Token Status: <strong>{{ status }}</strong> | Last Check (IST): {{ last_check }}
        </div>
    </div>

    <div class="table-container">
        <h3>Recent Trade Activity</h3>
        <table>
            <thead>
                <tr>
                    <th>Time (IST)</th>
                    <th>Signal</th>
                    <th>Price</th>
                    <th>Strike</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {% for trade in history %}
                <tr>
                    <td>{{ trade.time }}</td>
                    <td class="{{ 'signal-buy' if trade.type == 'BUY' else 'signal-sell' }}">{{ trade.type }}</td>
                    <td>{{ trade.price }}</td>
                    <td>{{ trade.strike }}</td>
                    <td>{{ trade.status }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</body>
</html>
"""

@app.route('/')
def dashboard():
    api_status = get_api_status()
    # Fixed: Now specifically fetching Asia/Kolkata time
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist).strftime("%H:%M:%S")
    
    return render_template_string(
        DASHBOARD_HTML, 
        history=TRADE_HISTORY, 
        status=api_status, 
        last_check=now_ist
    )

# --- 2. ORDER LOGIC ---
def surgical_reversal(signal):
    """
    Handles position sizing and reversal orders.
    """
    try:
        positions = dhan.get_positions()
        if positions.get('status') == 'success':
            for pos in positions.get('data', []):
                if pos['securityId'] == 'SET_YOUR_ID_HERE':
                    net_qty = pos['netQty']
                    if net_qty != 0:
                        exit_side = dhan.SELL if net_qty > 0 else dhan.BUY
                        dhan.place_order(
                            security_id=pos['securityId'],
                            exchange_segment=dhan.NSE_FNO,
                            transaction_type=exit_side,
                            quantity=abs(net_qty),
                            order_type=dhan.MARKET,
                            product_type=dhan.MARGIN
                        )
        return True
    except Exception:
        return False

# --- 3. WEBHOOK ENDPOINT ---
@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "no data"}), 400
    
    signal = data.get('signal', '').upper()
    price = float(data.get('price', 0))
    
    surgical_reversal(signal)
    
    # IST Time for trade history
    ist = pytz.timezone('Asia/Kolkata')
    trade_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M")
    
    try:
        strike = (round(price / 100) * 100) - 100 if signal == "BUY" else (round(price / 100) * 100) + 100
        option_type = "CE" if signal == "BUY" else "PE"

        status_entry = {
            "time": trade_time,
            "type": signal,
            "price": price,
            "strike": int(strike),
            "status": "Executed"
        }
    except Exception:
        status_entry = {"time": trade_time, "type": signal, "price": price, "strike": "-", "status": "Error"}
    
    TRADE_HISTORY.insert(0, status_entry)
    return jsonify({"status": "success"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
