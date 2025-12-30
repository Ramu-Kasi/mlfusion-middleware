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

# --- 1. DASHBOARD TEMPLATE (EXACT ORIGINAL LAYOUT) ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>MLFusion Trading Bot</title>
    <meta http-equiv="refresh" content="60">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f0f2f5; margin: 0; padding: 20px; }
        .status-bar { background: white; padding: 15px; border-radius: 8px; display: flex; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; gap: 20px; }
        .dot { height: 12px; width: 12px; border-radius: 50%; display: inline-block; margin-right: 5px; }
        .status-active { background-color: #28a745; color: #28a745; }
        .status-expired { background-color: #dc3545; color: #dc3545; }
        .status-text { font-weight: bold; }
        .refresh-text { color: #666; font-size: 0.9em; margin-left: auto; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        th { background: #333; color: white; padding: 12px; text-align: left; font-size: 0.9em; }
        td { padding: 12px; border-bottom: 1px solid #eee; font-size: 0.9em; }
        .type-ce { color: #28a745; font-weight: bold; }
        .type-pe { color: #dc3545; font-weight: bold; }
    </style>
</head>
<body>
    <div class="status-bar">
        <b>Dhan API Status:</b>
        {% set api_state = get_status() %}
        <div>
            <span class="dot {{ 'status-active' if api_state == 'Active' else 'status-expired' }}"></span> 
            <span class="status-text {{ 'status-active' if api_state == 'Active' else 'status-expired' }}">{{ api_state }}</span>
        </div>
        <span class="refresh-text">Last Checked: {{ last_run }} (IST)</span>
    </div>

    <h3>Trade History</h3>
    <table>
        <thead>
            <tr>
                <th>Time (IST)</th>
                <th>Price</th>
                <th>Strike</th>
                <th>Type</th>
                <th>Expiry</th>
                <th>Status</th>
                <th>Remarks</th>
            </tr>
        </thead>
        <tbody>
            {% for trade in history %}
            <tr>
                <td>{{ trade.time }}</td>
                <td>{{ trade.price }}</td>
                <td>{{ trade.strike }}</td>
                <td class="type-{{ trade.type|lower }}">{{ trade.type }}</td>
                <td>{{ trade.expiry }}</td>
                <td>{{ trade.status }}</td>
                <td>{{ trade.remarks }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""

@app.route('/')
def dashboard():
    # Corrected IST time for Render server
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist).strftime("%H:%M:%S")
    return render_template_string(
        DASHBOARD_HTML, 
        history=TRADE_HISTORY, 
        get_status=get_api_status,
        last_run=now_ist
    )

# --- 2. SURGICAL REVERSAL (RESTORED EXACTLY) ---
def surgical_reversal(signal_type):
    """
    Handles position sizing and reversal orders as per original logic.
