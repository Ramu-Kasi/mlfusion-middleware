import os
import sys
import pandas as pd
from flask import Flask, request, jsonify
from dhanhq import dhanhq

# 1. INITIALIZE APP
app = Flask(__name__)

# 2. CONFIGURATION
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None

def log_now(msg):
    """Force logs to show in Render immediately"""
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

def load_scrip_master():
    """Robust CSV loader that strictly filters for Bank Nifty Index Options"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading CSV...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Identify columns using keywords
        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        und_col = next((c for c in df.columns if 'UNDERLYING_SECURITY_ID' in c.upper()), None)
        sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        
        if inst_col and und_col:
            # FIX: Mandatory filter for 'BANKNIFTY' in symbol to stop PAGEIND
            # Also filtering for Index Options (OPTIDX) and Underlying 25 (Bank Nifty)
            SCRIP_MASTER_DATA = df[
                (df[inst_col].str.contains('OPTIDX', na=False)) & 
                (df[und_col] == 25) &
                (df[sym_col].str.contains('BANKNIFTY', na=False))
            ].copy()
            
            # DYNAMIC DATE CONVERSION
            exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
            if exp_col:
                SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
            
            log_now(f"BOOT: Filtered {len(SCRIP_MASTER_DATA)} Bank Nifty contracts.")
        else:
            log_now("BOOT WARNING: Filter columns not found. Using full list.")
            SCRIP_MASTER_DATA = df
            
        log_now("BOOT: CSV Loaded successfully.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")

# Load the cache on start
load_scrip_master()

def get_atm_id(price, signal):
    """Finds nearest ATM strike ID with 35 qty default [cite: 1.1, 4.1
