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
        
        # Identify columns dynamically to handle any CSV header changes
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

        # Keep ONLY the columns we need for trading to save RAM
        needed_cols = [id_col, strike_col, type_col, exp_col]
        SCRIP_MASTER_DATA = df[mask][needed_cols].copy()
        
        # Convert dates once during boot
        SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
        SCRIP_MASTER_DATA = SCRIP_MASTER_DATA.dropna(subset=[exp_col])
        
        log_now(f"BOOT: Success! {len(SCRIP_MASTER_DATA)} Bank Nifty contracts loaded to memory.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

# Run once at startup
load_scrip_master()

def close_opposite_position(type_to_close):
    """Execution: 100ms Reversal"""
    try:
        positions = dhan.get_positions()
        if positions.get('status') == 'success' and positions.get('data'):
            for pos in positions['data']:
                symbol = pos.get('tradingSymbol', '')
                qty = int(pos.get('netQty', 0))
                
                # Check for opposite leg to square off
                if "BANKNIFTY" in symbol and symbol.endswith(type_to_close) and qty != 0:
                    log_now(f"RE
