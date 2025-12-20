@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json()
    if not data:
        logging.error("No JSON received from TradingView")
        return jsonify({"status": "ERROR"}), 400

    signal = data.get("message", "").upper() 
    ticker = data.get("ticker", "BANKNIFTY")
    price = data.get("price", 0)

    trans_type = "B" if "BUY" in signal else "S"
    qty = "15" if "BANK" in ticker.upper() else "25"
    itm_strike = get_1_itm_ce(price, ticker)

    dhan_order = {
        "secret": "OvWi0",
        "alertType": "multi_leg_order",
        "order_legs": [{
            "transactionType": trans_type,
            "orderType": "MKT",          
            "quantity": qty,             
            "exchange": "NSE",           
            "symbol": ticker,
            "instrument": "OPT",
            "productType": "M",          
            "sort_order": "1",
            "price": "0",                
            "option_type": "CE",         
            "strike_price": str(float(itm_strike)),
            "expiry_date": "2025-12-30"  
        }]
    }

    # --- CRITICAL CHANGE: FORCE PRINT TO LOGS ---
    # We use 'print' alongside logging because Render always captures 'print'
    print(f">>> SENDING TO DHAN: {signal} on {ticker}")
    print(f">>> CALCULATED STRIKE: {itm_strike}")
    print(f">>> FULL JSON: {dhan_order}")

    try:
        response = requests.post(DHAN_WEBHOOK_URL, json=dhan_order, timeout=10)
        logging.info(f"Dhan Status: {response.status_code}")
        return jsonify({"status": "PROCESSED", "dhan_msg": response.text}), 200
    except Exception as e:
        logging.error(f"Post failed: {str(e)}")
        return jsonify({"status": "FAIL"}), 500
