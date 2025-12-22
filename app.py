def load_scrip_master():
    """Robust CSV loader that strictly filters for Bank Nifty Index Options"""
    global SCRIP_MASTER_DATA
    log_now("BOOT: Loading CSV...")
    try:
        df = pd.read_csv(SCRIP_URL, low_memory=False)
        
        # Use exact column names from Dhan documentation to avoid "keyword" errors
        inst_col = 'SEM_INSTRUMENT_NAME'
        und_col = 'SEM_UNDERLYING_SECURITY_ID'
        sym_col = 'SEM_SYMBOL_NAME' # Use SEM_SYMBOL_NAME for strict matching
        
        # If columns aren't found by exact name, fallback to keyword search
        if inst_col not in df.columns:
            inst_col = next((c for c in df.columns if 'INSTRUMENT' in c.upper()), None)
            und_col = next((c for c in df.columns if 'UNDERLYING_SECURITY_ID' in c.upper()), None)
            sym_col = next((c for c in df.columns if 'SYMBOL' in c.upper()), None)

        if inst_col and und_col:
            # IMPROVED FILTER: 
            # 1. Must be OPTIDX (Index Options)
            # 2. Underlying ID for Bank Nifty is usually 25, but we add a strict Symbol check
            # 3. Specifically exclude 'BANKEX' from the symbol column
            SCRIP_MASTER_DATA = df[
                (df[inst_col].str.contains('OPTIDX', na=False)) & 
                (df[sym_col].str.contains('BANKNIFTY', case=False, na=False)) &
                (~df[sym_col].str.contains('BANKEX', case=False, na=False))
            ].copy()
            
            exp_col = next((c for c in df.columns if 'EXPIRY_DATE' in c.upper()), None)
            if exp_col:
                SCRIP_MASTER_DATA[exp_col] = pd.to_datetime(SCRIP_MASTER_DATA[exp_col], errors='coerce')
            
            log_now(f"BOOT: Filtered {len(SCRIP_MASTER_DATA)} Bank Nifty contracts.")
        else:
            log_now("BOOT WARNING: Filter columns not found.")
            SCRIP_MASTER_DATA = df
            
        log_now("BOOT: CSV Loaded successfully.")
    except Exception as e:
        log_now(f"CRITICAL BOOT ERROR: {e}")
