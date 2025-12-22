import os
import sys
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq

# 1. INITIALIZE APP FIRST
app = Flask(__name__)

# 2. CONFIGURATION
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# Dhan Scrip Master URL
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# GLOBAL RAM CACHE
SCRIP_MASTER_DATA = None

def log_now(msg):
    """Immediate logging for Render"""
    print(f"!!! [ALGO_ENGINE]: {msg}", file=sys.stderr, flush=True)

def load_scrip_master():
    """Robust CSV loader that dynamically finds Bank Nifty"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Fetching Scrip Master into RAM...")
    try:
        # Load the CSV without forcing columns to prevent 'Usecols' errors
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Filter for Index Options (OPTIDX) + Bank Nifty (Underlying 25)
        # This reduces the 50MB file to just a few KB in RAM
        SCRIP_MASTER_DATA = df[
            (df['SEM_INSTRUMENT_NAME'] == 'OPTIDX') & 
            (df['SEM_UNDERLYING_SECURITY_ID'] == 25)
        ].copy()
        
        log_now(f"BOOT: Success! Cached {len(SCRIP_MASTER_DATA)} Bank Nifty contracts.")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

# Load the cache on start
load_scrip_master()

def get_atm_id(price, signal):
    """Finds nearest ATM strike ID in milliseconds"""
    try:
        if SCRIP_MASTER_DATA is None or SCRIP_MASTER_DATA.empty:
            return None, None
            
        # Round price to nearest 100
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Filter for Strike + Type
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA['SEM_STRIKE_PRICE'] == strike) & 
            (SCRIP_MASTER_DATA['SEM_OPTION_TYPE'] == opt_type)
        ]
        
        if not match.empty:
            # Sort by expiry to get the nearest (weekly) contract
            match = match.sort_values(by='SEM_EXPIRY_DATE')
            return str(int(match.iloc[0]['SEM_SMST_SECURITY_ID'])), strike
            
        return None, strike
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None

@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    # Debug: See exactly what TradingView is sending
    raw_body = request.get_data(as_text=True)
    log_now(f"SIGNAL RECEIVED: {raw_body}")

    try:
        data = request.get_json(force=True
