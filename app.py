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
        log_now("ASYNC: Loading Scrip Master in background...")
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
        log_now("BOOT: Jan1-2026 Scrip Master Ready.")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

threading.Thread(target=load_scrip_master, daemon=True).start()

def get_atm_id(price, signal):
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty: return None, None, 15
        base_strike = round(float(price) / 100) * 100
        if "BUY" in signal.upper():
            strike, opt_type = base_strike - 100, "CE"
        else:
            strike, opt_type = base_strike + 100, "PE"
            
        cols = SCRIP
