# --- 4. SURGICAL REVERSAL (Updated to track action) ---
def surgical_reversal(signal_type):
    action_taken = False
    try:
        positions_resp = dhan.get_positions()
        if positions_resp.get('status') == 'success':
            for pos in positions_resp.get('data', []):
                symbol = pos.get('tradingSymbol', '').upper()
                net_qty = int(pos.get('netQty', 0))
                if "BANKNIFTY" in symbol and net_qty != 0:
                    is_call, is_put = "CE" in symbol, "PE" in symbol
                    if (signal_type == "BUY" and is_put) or (signal_type == "SELL" and is_call):
                        dhan.place_order(
                            security_id=pos['securityId'], 
                            exchange_segment=pos['exchangeSegment'], 
                            transaction_type=dhan.SELL if net_qty > 0 else dhan.BUY, 
                            quantity=abs(net_qty), 
                            order_type=dhan.MARKET, 
                            product_type=dhan.MARGIN, 
                            price=0
                        )
                        action_taken = True # We actually closed a position
        return action_taken
    except Exception: return False

# --- 5. ROUTES ---
@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    data = request.get_json(force=True, silent=True)
    if not data: return jsonify({"status": "no data"}), 400
    msg, price = data.get('message', '').upper(), float(data.get('price', 0))
    
    # Check if a reversal actually happened
    was_reversed = surgical_reversal(msg)
    
    time.sleep(0.5) 
    
    sec_id, strike, qty = get_atm_id(price, msg)
    if not sec_id: return jsonify({"status": "error", "remarks": "Scrip ID not found"}), 404
    
    order_res = dhan.place_order(
        security_id=sec_id, 
        exchange_segment=dhan.NSE_FNO, 
        transaction_type=dhan.BUY, 
        quantity=qty, 
        order_type=dhan.MARKET, 
        product_type=dhan.MARGIN, 
        price=0
    )
    
    trade_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")
    
    # --- SMART REMARKS LOGIC ---
    curr_type = "CE" if "BUY" in msg else "PE"
    opp_type = "PE" if "BUY" in msg else "CE"
    
    if order_res.get('status') == 'success':
        if was_reversed:
            remark = f"Closed {opp_type} & Opened {curr_type} {strike}"
        else:
            remark = f"Opened {curr_type} {strike}" # Accurate for fresh entries
    else:
        remark = order_res.get('remarks', 'Entry Failed')

    status_entry = {
        "time": trade_time, 
        "price": price, 
        "strike": strike, 
        "type": curr_type, 
        "status": "success" if order_res.get('status') == 'success' else "failure", 
        "remarks": remark
    }
    TRADE_HISTORY.insert(0, status_entry)
    return jsonify(status_entry), 200
