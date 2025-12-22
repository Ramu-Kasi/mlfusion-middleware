@app.route('/mlfusion', methods=['POST'])
def mlfusion():
    # 1. Capture the raw data first for debugging
    raw_data = request.get_data(as_text=True)
    log_now(f"RAW RECEIVED: {raw_data}")

    try:
        # 2. Attempt to parse JSON
        data = request.get_json(force=True, silent=True)
        
        if data is None:
            log_now("ERROR: Request body is not valid JSON. Check for curly quotes or missing commas.")
            return jsonify({"error": "Invalid JSON"}), 400

        # 3. Handle 'price' safely (Convert string "59475" to float 59475.0)
        price_val = data.get("price")
        if price_val is None:
            log_now("ERROR: 'price' key missing in JSON")
            return jsonify({"error": "No price found"}), 400
            
        # This converts "{{close}}" string to a number even with quotes
        tv_price = float(price_val) 
        signal = data.get("message", "").upper()

        # 4. Map to Security ID (using our cached RAM data)
        sec_id, strike = get_atm_id(tv_price, signal)
        
        if not sec_id:
            log_now(f"NOT FOUND: No ID for {strike} {signal}")
            return jsonify({"error": "ID not found"}), 404

        log_now(f"MATCH: {signal} at {tv_price} -> ATM {strike} (ID: {sec_id})")

        # 5. Place the Buy Order
        order = dhan.place_order(
            security_id=sec_id,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=dhan.BUY, 
            quantity=35,
            order_type=dhan.MARKET,
            product_type=dhan.MARGIN,
            price=0,
            validity='DAY'
        )

        return jsonify(order), 200

    except Exception as e:
        log_now(f"CRITICAL ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500
