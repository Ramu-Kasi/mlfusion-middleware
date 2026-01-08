import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime, date
import pytz
import threading

app = Flask(__name__)

# ---------------- CONFIG ----------------
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

IST = pytz.timezone("Asia/Kolkata")
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# ---------------- GLOBALS ----------------
SCRIP_MASTER_DATA = None
BN_EXPIRIES = []
TRADE_HISTORY = []
OPEN_TRADE_REF = None

DHAN_API_STATUS = {
    "state": "UNKNOWN",
    "message": "Checking..."
}

# ---------------- LOG ----------------
def log_now(msg):
    sys.stderr.write(f"[ALGO_ENGINE] {msg}\n")
    sys.stderr.flush()

# ---------------- SCRIP MASTER ----------------
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)

        inst = next(c for c in df.columns if "INSTRUMENT" in c.upper())
        sym  = next(c for c in df.columns if "SYMBOL" in c.upper())
        exp  = next(c for c in df.columns if "EXPIRY_DATE" in c.upper())

        mask = (
            df[inst].str.contains("OPTIDX", na=False) &
            df[sym].str.contains("BANKNIFTY", case=False, na=False) &
            ~df[sym].str.contains("BANKEX", case=False, na=False)
        )

        SCRIP_MASTER_DATA = df[mask].copy()
        SCRIP_MASTER_DATA[exp] = pd.to_datetime(SCRIP_MASTER_DATA[exp], errors="coerce")
        SCRIP_MASTER_DATA.dropna(subset=[exp], inplace=True)

        refresh_bn_expiries()
        log_now("Scrip master loaded")

    except Exception as e:
        log_now(f"Scrip load error: {e}")

def periodic_scrip_refresh():
    while True:
        time.sleep(24 * 60 * 60)
        load_scrip_master()

threading.Thread(target=load_scrip_master, daemon=True).start()
threading.Thread(target=periodic_scrip_refresh, daemon=True).start()

# ---------------- EXPIRY ----------------
def refresh_bn_expiries():
    global BN_EXPIRIES
    exp = next(c for c in SCRIP_MASTER_DATA.columns if "EXPIRY_DATE" in c.upper())
    BN_EXPIRIES = sorted(SCRIP_MASTER_DATA[exp].unique())

def get_current_and_next_expiry():
    today = date.today()
    future = [e for e in BN_EXPIRIES if e.date() >= today]
    if len(future) >= 2:
        return future[0], future[1]
    if len(future) == 1:
        return future[0], future[0]
    return None, None

def get_active_expiry_details():
    curr, nxt = get_current_and_next_expiry()
    if not curr:
        return "—", None
    dte = (curr.date() - date.today()).days
    active = nxt if dte <= 5 else curr
    return active.strftime("%d-%b-%Y"), dte

# ---------------- ATM ----------------
def get_atm_id(price, signal):
    try:
        base = round(price / 100) * 100
        strike, opt = (base - 100, "CE") if "BUY" in signal else (base + 100, "PE")

        cols = SCRIP_MASTER_DATA.columns
        sc = next(c for c in cols if "STRIKE" in c.upper())
        tc = next(c for c in cols if "OPTION_TYPE" in c.upper())
        ec = next(c for c in cols if "EXPIRY_DATE" in c.upper())
        ic = next(c for c in cols if "SECURITY" in c.upper() or "TOKEN" in c.upper())

        curr, nxt = get_current_and_next_expiry()
        dte = (curr.date() - date.today()).days if curr else 99
        expiry = nxt if dte <= 5 else curr

        row = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[sc] == strike) &
            (SCRIP_MASTER_DATA[tc] == opt) &
            (SCRIP_MASTER_DATA[ec] == expiry)
        ]

        if row.empty:
            return None, strike, 30, "—"

        return str(int(row.iloc[0][ic])), strike, 30, expiry.strftime("%d-%b-%Y")

    except Exception as e:
        log_now(f"ATM error: {e}")
        return None, None, 30, "—"

# ---------------- PRICE ----------------
def fetch_price(sec_id=None):
    try:
        tb = dhan.get_trade_book()
        if tb.get("status") != "success":
            return None
        for t in reversed(tb["data"]):
            if sec_id is None or str(t["securityId"]) == str(sec_id):
                return float(t["tradedPrice"])
    except Exception:
        pass
    return None

# ---------------- API HEALTH ----------------
def check_dhan_api_status():
    try:
        resp = dhan.get_positions()
        if resp.get("status") == "success":
            DHAN_API_STATUS["state"] = "ACTIVE"
            DHAN_API_STATUS["message"] = "Active"
        else:
            DHAN_API_STATUS["state"] = "ERROR"
            DHAN_API_STATUS["message"] = "API Error"
    except Exception:
        DHAN_API_STATUS["state"] = "ERROR"
        DHAN_API_STATUS["message"] = "API Error"

# ---------------- VERIFY POSITION CLOSED ----------------
def verify_position_closed(security_id, max_retries=2):
    """
    Fast position verification - max 2 attempts with minimal delay.
    Returns True if position is confirmed closed or not found.
    """
    for attempt in range(max_retries):
        try:
            time.sleep(0.15)  # Ultra-fast 150ms delay
            resp = dhan.get_positions()
            if resp.get("status") != "success":
                log_now(f"Check {attempt+1}: API error")
                continue
            
            # Check if security_id exists in open positions
            positions = resp.get("data", [])
            for pos in positions:
                if str(pos.get("securityId")) == str(security_id):
                    net_qty = pos.get("netQty", 0)
                    if net_qty != 0:
                        log_now(f"Check {attempt+1}: Still open (qty={net_qty})")
                        return False
            
            log_now(f"✓ Verified closed (attempt {attempt+1})")
            return True
            
        except Exception as e:
            log_now(f"Verify error {attempt+1}: {e}")
    
    # If we can't verify after 2 attempts, assume it failed
    log_now("⚠ Could not verify closure")
    return False

# ---------------- CHECK ACTUAL BN POSITIONS ----------------
def get_actual_bn_positions():
    """
    Query Dhan API for actual open BankNifty option positions.
    Returns list of positions with type (CE/PE), security_id, and quantity.
    """
    try:
        resp = dhan.get_positions()
        if resp.get("status") != "success":
            log_now("⚠ Could not fetch positions from Dhan API")
            return []
        
        bn_positions = []
        positions = resp.get("data", [])
        
        for pos in positions:
            net_qty = pos.get("netQty", 0)
            if net_qty == 0:
                continue  # Skip closed positions
            
            # Check if it's a BankNifty option
            trading_symbol = pos.get("tradingSymbol", "")
            if "BANKNIFTY" not in trading_symbol.upper():
                continue
            if "BANKEX" in trading_symbol.upper():
                continue  # Skip BankEx
            
            # Determine CE or PE
            option_type = None
            if "CE" in trading_symbol:
                option_type = "CE"
            elif "PE" in trading_symbol:
                option_type = "PE"
            
            if option_type:
                bn_positions.append({
                    "type": option_type,
                    "security_id": str(pos.get("securityId")),
                    "quantity": abs(net_qty),
                    "symbol": trading_symbol
                })
        
        return bn_positions
        
    except Exception as e:
        log_now(f"⚠ Error fetching BN positions: {e}")
        return []

# ---------------- EXIT OPPOSITE (FIXED ATOMIC SWITCH) ----------------
def exit_opposite(expected_type):
    """
    CRITICAL FIX: Exit opposite BankNifty trade type before entering new trade.
    - Checks ACTUAL Dhan positions (not just OPEN_TRADE_REF)
    - Handles manual trades and force-closed positions
    - If expected_type is CE, close any open PE trade
    - If expected_type is PE, close any open CE trade
    - Returns True only when safe to proceed with new trade
    """
    global OPEN_TRADE_REF

    # Step 1: Check actual BN positions from Dhan API
    log_now("→ Checking actual BankNifty positions...")
    actual_positions = get_actual_bn_positions()
    
    if not actual_positions:
        log_now("✓ No open BN positions, safe to proceed")
        OPEN_TRADE_REF = None  # Sync state
        return True
    
    # Step 2: Find opposite type position
    opposite_type = "PE" if expected_type == "CE" else "CE"
    opposite_position = None
    
    for pos in actual_positions:
        if pos["type"] == opposite_type:
            opposite_position = pos
            break
    
    # Step 3: Check if same type already open (no action needed)
    same_type_open = any(pos["type"] == expected_type for pos in actual_positions)
    
    if not opposite_position:
        if same_type_open:
            log_now(f"✓ {expected_type} already open, no opposite found")
        else:
            log_now(f"✓ No opposite {opposite_type} position found")
        return True
    
    # Step 4: OPPOSITE DETECTED - MUST CLOSE IT
    log_now(f"⚠ REVERSAL: Open {opposite_type} detected, new signal is {expected_type}")
    log_now(f"→ ATOMIC SWITCH: Closing {opposite_type} ({opposite_position['symbol']})...")

    security_id = opposite_position["security_id"]
    lot_size = opposite_position["quantity"]

    try:
        # Place exit order
        exit_resp = dhan.place_order(
            security_id=security_id,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.SELL,
            quantity=lot_size,
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0
        )

        log_now(f"Exit order response: {exit_resp}")

        # Check if order was placed successfully
        if exit_resp.get("status") != "success":
            log_now(f"✗ EXIT FAILED: {exit_resp.get('remarks', 'Unknown error')}")
            return False

        # Verify position is actually closed
        if not verify_position_closed(security_id):
            log_now("✗ EXIT FAILED: Position still open after exit order")
            return False

        # Update trade record if it was tracked
        if OPEN_TRADE_REF and OPEN_TRADE_REF.get("security_id") == security_id:
            exit_price = fetch_price(security_id)
            OPEN_TRADE_REF["exit_price"] = exit_price if exit_price else "—"
            OPEN_TRADE_REF["status"] = "CLOSED"
            OPEN_TRADE_REF["remarks"] = "EXIT ON REVERSAL"
            log_now(f"✓ {opposite_type} closed @ {exit_price}")
        else:
            log_now(f"✓ {opposite_type} (manual) closed successfully")
        
        # Clear the reference
        OPEN_TRADE_REF = None
        
        # Minimal delay for state propagation
        time.sleep(0.1)
        
        return True

    except Exception as e:
        log_now(f"✗ EXIT EXCEPTION: {e}")
        return False

# ---------------- TRADE ACTIVE DAYS ----------------
def trade_active_days(trade):
    try:
        if trade.get("status") == "REJECTED" or trade.get("entry_price") in ["—", None]:
            return "—"
        start = datetime.strptime(trade["date"], "%d-%b-%Y").date()
        return (date.today() - start).days + 1
    except Exception:
        return "—"

# ---------------- ROUTES ----------------
@app.route("/")
def dashboard():
    check_dhan_api_status()
    expiry, dte = get_active_expiry_details()

    return render_template_string(
        DASHBOARD_HTML,
        history=TRADE_HISTORY,
        trade_active_days=trade_active_days,
        api_state=DHAN_API_STATUS["state"],
        api_message=DHAN_API_STATUS["message"],
        active_expiry=expiry,
        expiry_danger=(dte is not None and dte <= 5),
        last_run=datetime.now(IST).strftime("%H:%M:%S")
    )

@app.route("/mlfusion", methods=["POST"])
def mlfusion():
    global OPEN_TRADE_REF

    data = request.get_json(force=True)
    msg = data.get("message", "").upper()
    price = float(data.get("price", 0))

    log_now(f"═══ MLFUSION SIGNAL ═══")
    log_now(f"Signal: {msg} @ ₹{price}")

    expected_type = "CE" if "BUY" in msg else "PE"
    log_now(f"Expected trade type: {expected_type}")

    # CRITICAL: Exit opposite trade FIRST (atomic switch)
    if not exit_opposite(expected_type):
        log_now("✗ TRADE REJECTED: Failed to exit opposite position")
        return jsonify({"error": "Failed to exit opposite position - trade aborted"}), 400

    # Get ATM details for new trade
    sec, strike, qty, expiry_used = get_atm_id(price, msg)
    if not sec:
        log_now("✗ TRADE REJECTED: ATM strike not found")
        return jsonify({"error": "ATM not found"}), 400

    log_now(f"→ Entering {expected_type} @ Strike {strike}, Qty {qty}")

    # Place new order
    resp = dhan.place_order(
        security_id=sec,
        exchange_segment=dhan.NSE_FNO,
        transaction_type=dhan.BUY,
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.MARGIN,
        price=0
    )

    success = resp.get("status") == "success"
    
    if success:
        log_now(f"✓ {expected_type} order placed successfully")
    else:
        log_now(f"✗ {expected_type} order FAILED: {resp}")

    # Create trade record
    trade = {
        "date": datetime.now(IST).strftime("%d-%b-%Y"),
        "time": datetime.now(IST).strftime("%H:%M:%S"),
        "price": price,
        "strike": strike,
        "type": expected_type,
        "expiry_used": expiry_used,
        "lot_size": qty,
        "premium_paid": "—",
        "entry_price": "—",
        "exit_price": "—",
        "status": "OPEN" if success else "REJECTED",
        "remarks": str(resp),
        "security_id": sec
    }

    if success:
        # Minimal delay for execution confirmation
        time.sleep(0.3)
        entry = fetch_price(sec)
        trade["entry_price"] = entry
        trade["premium_paid"] = round(entry * qty, 2) if entry else "—"
        OPEN_TRADE_REF = trade
        log_now(f"Entry: ₹{entry}, Premium: ₹{trade['premium_paid']}")

    TRADE_HISTORY.insert(0, trade)
    log_now("═══════════════════════")
    
    return jsonify(trade), 200

# ---------------- UI ----------------
DASHBOARD_HTML = '''
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="refresh" content="60">
<style>
body{font-family:sans-serif;background:#f0f2f5;padding:20px}
.status-bar{background:#fff;padding:15px;border-radius:8px;display:flex;gap:20px;align-items:center}
.status-active{background:#28a745;color:#fff;padding:2px 10px;border-radius:10px}
.status-expired{background:#d9534f;color:#fff;padding:2px 10px;border-radius:10px}
.expiry-danger{color:#d9534f;font-weight:bold}
.journal-title{font-family:Georgia,serif;font-size:21px;color:#b08d57}
table{width:100%;border-collapse:collapse;background:#fff;margin-top:20px}
th{background:#333;color:#fff;padding:10px}
td{padding:10px;border-bottom:1px solid #eee}
</style>
</head>
<body>

<div class="status-bar">
<b>Dhan API:</b>
<span class="{% if api_state=='ACTIVE' %}status-active{% else %}status-expired{% endif %}">
{{ api_message }}
</span>

&nbsp;&nbsp;<b>Active BN Expiry:</b>
<span class="{{ 'expiry-danger' if expiry_danger else '' }}">{{ active_expiry }}</span>

<div class="journal-title" style="margin:0 auto;">Ramu's Magic Journal</div>
<div>Last Check: {{ last_run }} IST</div>
</div>

<table>
<tr>
<th>Date</th><th>Time</th><th>Price</th><th>Strike</th><th>Type</th><th>Expiry Used</th>
<th>Lot</th><th>Premium</th><th>Entry</th><th>Exit</th>
<th>Points</th><th>PnL</th><th>Days</th><th>Status</th><th>Remarks</th>
</tr>

{% for t in history %}
<tr>
<td>{{t.date}}</td><td>{{t.time}}</td><td>{{t.price}}</td><td>{{t.strike}}</td><td>{{t.type}}</td>
<td>{{t.expiry_used}}</td><td>{{t.lot_size}}</td><td>{{t.premium_paid}}</td>
<td>{{t.entry_price}}</td><td>{{t.exit_price}}</td>

{% if t.entry_price != '—' and t.exit_price != '—' %}
{% set pts = t.exit_price - t.entry_price %}
<td>{{ pts }}</td>
<td style="background-color:
{{ 'lightgreen' if pts * t.lot_size > 0 else 'lightcoral' if pts * t.lot_size < 0 else 'lightyellow' }}">
₹{{ pts * t.lot_size }}
</td>
{% else %}
<td>—</td><td style="background-color:lightyellow">—</td>
{% endif %}

<td>{{ trade_active_days(t) }}</td>
<td>{{t.status}}</td><td>{{t.remarks}}</td>
</tr>
{% endfor %}
</table>
</body>
</html>
'''

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
