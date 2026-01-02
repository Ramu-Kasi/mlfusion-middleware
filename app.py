import os
import sys
import time
import pandas as pd
from flask import Flask, request, jsonify, render_template_string
from dhanhq import dhanhq
from datetime import datetime
import pytz
import threading

app = Flask(__name__)

# --- 1. CONFIGURATION ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# BN lot size is 30 as of Jan 2026
BN_LOT_SIZE = 30 
TARGET_LOTS = 1
FIXED_QTY = TARGET_LOTS * BN_LOT_SIZE

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
TRADE_HISTORY = []

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

# --- 2. DYNAMIC SCRIP MASTER ---
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        log_now("ASYNC: Loading Scrip Master...")
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
        
        if inst_col and sym_col:
            mask = (
                (df[inst_col].str.contains('OPTIDX', na=False)) & 
                (df[sym_col].str.contains('BANKNIFTY', case=False, na=False)) &
                (~df[sym_col].str.contains('BANKEX', case=False, na=False))
            )
            if exch_col:
                mask = mask & (df[exch_col].str.contains('NSE', case=False, na=False))

            SCRIP_MASTER_DATA = df[mask].copy()
            if exp_col:
                SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
                SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=[exp_col])
        log_now("BOOT: Scrip Master Ready (BN Lot: 30).")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

threading.Thread(target=load_scrip_master, daemon=True).start()

def get_atm_id(price, signal):
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty: 
            return None, None, FIXED_QTY
        
        base_strike = round(float(price) / 100) * 100
        if "BUY" in signal.upper():
            strike, opt_type = base_strike - 100, "CE"
        else:
            strike, opt_type = base_strike + 100, "PE"
            
        cols = SCRIP_MASTER_DATA.columns
        strike_col = next((c for c in cols if 'STRIKE' in c.upper()), None)
        type_col = next((c for c in cols if 'OPTION_TYPE' in c.upper()), None)
        exp_col = next((c for c in cols if 'EXPIRY_DATE' in c.upper()), None)
        id_col = next((c for c in cols if 'SMST_SECURITY_ID' in c.upper()), 
                     next((c for c in cols if 'TOKEN' in c.upper()), None))

        match = SCRIP_MASTER_DATA[(SCRIP_MASTER_DATA[strike_col] == strike) & (SCRIP_MASTER_DATA[type_col] == opt_type)].copy()
        if not match.empty:
            today = pd.Timestamp(datetime.now().date())
            match = match[match[exp_col] >= today].sort_values(by=exp_col, ascending=True)
            if not match.empty:
                return str(int(match.iloc[0][id_col])), strike, FIXED_QTY
        return None, strike, FIXED_QTY
    except Exception: 
        return None, None, FIXED_QTY

# --- 3. DASHBOARD UI ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>MLFusion - BN 30 Lot Size</title>
    <style>
        body { font-family: sans-serif; background-color: #f0f2f5; padding: 20px; }
        .status-bar { background: white; padding: 15px; border-radius: 8px; display: flex; align-items: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; }
        th { background: #333; color: white; padding: 12px; text-align: left; }
        td { padding: 12px; border-bottom: 1px solid #eee; }
        .ce-text { color: #28a745; font-weight: bold; }
        .pe-text { color: #d9534f; font-weight: bold; }
    </style>
</head>
<body>
    <div class="status-bar">
        <b>Dhan API:</b> &nbsp; <span style="color:green">Connected</span>
        <span style="
