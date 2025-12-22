def load_scrip_master():
    global SCRIP_MASTER_DATA
    log_now("BOOT: Fetching Scrip Master...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Dynamic search for the correct columns
        inst_col = [c for c in df.columns if 'INSTRUMENT_NAME' in c][0]
        
        # Filter for Index Options (OPTIDX)
        # We search broadly for 'BANKNIFTY' in any symbol column to be safe
        sym_col = [c for c in df.columns if 'SYMBOL_NAME' in c or 'UNDERLYING' in c][0]
        
        SCRIP_MASTER_DATA = df[
            (df[inst_col] == 'OPTIDX') & 
            (df[sym_col].astype(str).str.contains('BANKNIFTY|25'))
        ].copy()
        
        log_now(f"BOOT: Success! Cached {len(SCRIP_MASTER_DATA)} Bank Nifty contracts.")
    except Exception as e:
        log_now(f"BOOT ERROR: {e}")

def get_atm_id(price, signal):
    try:
        strike = round(float(price) / 100) * 100
        opt_type = "CE" if "BUY" in signal.upper() else "PE"
        
        # Finding columns by partial names to prevent Header Errors
        strike_col = [c for c in SCRIP_MASTER_DATA.columns if 'STRIKE' in c][0]
        type_col = [c for c in SCRIP_MASTER_DATA.columns if 'OPTION_TYPE' in c][0]
        id_col = [c for c in SCRIP_MASTER_DATA.columns if 'SECURITY_ID' in c][0]
        
        match = SCRIP_MASTER_DATA[
            (SCRIP_MASTER_DATA[strike_col] == strike) & 
            (SCRIP_MASTER_DATA[type_col] == opt_type)
        ]
        
        if not match.empty:
            # Sort by Expiry (usually index 8) to get the current contract
            exp_col = [c for c in SCRIP_MASTER_DATA.columns if 'EXPIRY_DATE' in c][0]
            match = match.sort_values(by=exp_col)
            return str(int(match.iloc[0][id_col])), strike
            
        return None, strike
    except Exception as e:
        log_now(f"LOOKUP ERROR: {e}")
        return None, None
