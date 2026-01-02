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

# --- CONFIG ---
CLIENT_ID = os.environ.get('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.environ.get('DHAN_ACCESS_TOKEN')
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
SCRIP_MASTER_DATA = None
BN_EXPIRIES = []

TRADE_HISTORY = []
OPEN_TRADE_REF = None

def log_now(msg):
    sys.stderr.write(f"!!! [ALGO_ENGINE]: {msg}\n")
    sys.stderr.flush()

# --- LOAD SCRIP MASTER ---
def load_scrip_master():
    global SCRIP_MASTER_DATA
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)

        inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
        sym_col  = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)
        exch_col = next((c for c in df.columns if 'EXCHANGE' in c.upper()), None)
        exp_col  = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)

        mask = (
            df[inst_col].str.contains('OPTIDX', na=False) &
            df[sym_col].str.contains('BANKNIFTY', case=False, na=False) &
            ~df[sym_col].str.contains('BANKEX', case=False, na=False)
        )

        if exch_col:
            mask &= df[exch_col].str.contains('NSE', case=False, na=False)

        SCRIP_MASTER_DATA = df[mask].copy()
        SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
        SCRIP_MASTER_DATA.dropna(subset=[exp_col], inplace=True)

        refresh_bn_expiries()

    except Exception as e:
        log_now(f"SCRIP LOAD ERROR: {e}")

threading.Thread(target=load_scrip_master, daemon=True).start()

# --- EXPIRY UTILS ---
def refresh_bn_expiries():
    global BN_EXPIRIES
    exp_col = next(c for c in SCRIP_MASTER_DATA.columns if 'EXPIRY_DATE' in c.upper())
    BN_EXPIRIES = sorted(SCRIP_MASTER_DATA[exp_col].unique())

def get_current_and_next_expiry():
    today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
    future = [e for e in BN_EXPIRIES if e.date() >= today]
    return (future + future[:1])[:2]

# --- ATM SELECTION ---
def get_atm_id(price, signal):
    base = round(price / 100) * 100
    strike, opt_type = (base - 100, "CE") if "BUY" in signal else (base + 100, "PE")

    cols = SCRIP_MASTER_DATA.columns
    strike_col = next(c for c in cols if 'STRIKE' in c.upper())
    type_col   = next(c for c in cols if 'OPTION_TYPE' in c.upper())
    exp_col    = next(c for c in cols if 'EXPIRY_DATE' in c.upper())
    id_col     = next(c for c in cols if 'SECURITY' in c.upper() or 'TOKEN' in c.upper())

    curr_exp, next_exp = get_current_and_next_expiry()
    today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
    selected_exp = next_exp if (curr_exp and (curr_exp.date() - today).days <= 5) else curr_exp

    match = SCRIP_MASTER_DATA[
        (SCRIP_MASTER_DATA[strike_col] == strike) &
        (SCRIP_MASTER_DATA[type_col] == opt_type) &
        (SCRIP_MASTER_DATA[exp_col] == selected_exp)
    ]

    if not match.empty:
        return str(int
