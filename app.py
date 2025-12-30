import os
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime

app = Flask(__name__)

# --- CONFIGURATION (Render Environment Variables) ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# STATE SETTINGS
TRADE_HISTORY = [] 

def get_api_status():
    """
    Pings Dhan API to check if the token is truly active.
    This runs every time the dashboard is refreshed.
    """
    try:
        # Pinging fund limits is the fastest way to verify a token
        profile = dhan.get_fund_limits()
        if profile.get('status') == 'success':
            return "Active"
        return "Expired"
    except Exception:
        return "Inactive / Connection Error"

# --- 1. DASHBOARD TEMPLATE ---
# Added a dynamic status badge for the Token
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>MLFusion Trading Bot - Bank Nifty Trial</title>
    <meta http-equiv="refresh" content="60">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f0f2f5; margin: 0; padding: 20px; }
        .status-bar { background: white; padding: 15px; border-radius: 8px; display: flex; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; gap: 20px; }
        .status-badge { padding: 5px 12px; border-radius: 20px; font-weight: bold; font-size: 0.9em; }
        .active { background-color: #e6f4ea; color: #1e8e3e; }
        .expired { background-color: #fce8e6; color: #d93025; }
        .table-container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="status-bar">
        <h2>System Control Panel</h2>
        <div>
            Token Status: 
            <span class="status-badge {{ 'active' if status == 'Active' else 'expired' }}">
                {{ status }}
            </span>
        </div>
        <div style="color: #666;">Last Check: {{ last_check }}</div>
    </div>

    <div class="table-container">
        <h3>Live BN Trade Logs (Last 30 Days Trial)</h3>
        <table>
            <thead>
                <tr>
                    <th>Time</th>
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
                    <td>{{ trade.type }}</td>
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
    now = datetime.now().strftime("%H:%M:%S")
    return render_template_string(
        DASHBOARD_HTML, 
        history=TRADE_HISTORY, 
        status=api_status, 
        last_check=now
    )

# --- 2. ORDER LOGIC ---
def surgical_reversal(signal):
    """Placeholder for your reversal logic discussed earlier"""
    print(f"Executing {signal} for Bank Nifty...")
    pass

# --- 3. WEBHOOK ENDPOINT ---
@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "no data"}), 400
    
    signal = data.get('signal', '').upper()
    price = float(data.get('price', 0))
    
    # Run the trade
    surgical_reversal(signal)
    
    # Log the result for the dashboard
    TRADE_HISTORY.insert(0, {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "type": signal,
        "price": price,
        "strike": round(price/100)*100,
        "status": "Executed"
    })
    
    return jsonify({"status": "success"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
