"""
Transaction cost calculator for Indian equity delivery trades
Based on Zerodha pricing structure
"""

def calculate_transaction_costs(buy_price, sell_price, quantity):
    """
    Calculate all transaction costs for delivery equity trade
    
    Args:
        buy_price: Entry price
        sell_price: Exit price
        quantity: Number of shares
    
    Returns:
        dict with breakdown of all costs
    """
    buy_turnover = buy_price * quantity
    sell_turnover = sell_price * quantity
    total_turnover = buy_turnover + sell_turnover
    
    # 1. Brokerage: Rs 15 per order (buy + sell)
    brokerage = 15 + 15  # Rs 30 total
    
    # 2. STT (Securities Transaction Tax): 0.1% on sell side
    stt = sell_turnover * 0.001
    
    # 3. Exchange transaction charges: 0.00345% on total turnover (NSE)
    exchange_charges = total_turnover * 0.0000345
    
    # 4. GST: 18% on (Brokerage + Exchange charges)
    gst = (brokerage + exchange_charges) * 0.18
    
    # 5. SEBI charges: 0.0001% on total turnover
    sebi_charges = total_turnover * 0.000001
    
    # 6. Stamp duty: 0.015% on buy side (capped at Rs 1500 per trade)
    stamp_duty = min(buy_turnover * 0.00015, 1500)
    
    # Total charges
    total_charges = brokerage + stt + exchange_charges + gst + sebi_charges + stamp_duty
    
    # Calculate as percentage of position size (avg of buy and sell)
    avg_position_size = (buy_turnover + sell_turnover) / 2
    cost_percentage = (total_charges / avg_position_size) * 100
    
    # Points to breakeven (charges as price points)
    points_to_breakeven = total_charges / quantity
    
    return {
        'brokerage': round(brokerage, 2),
        'stt': round(stt, 2),
        'exchange_charges': round(exchange_charges, 2),
        'gst': round(gst, 2),
        'sebi_charges': round(sebi_charges, 2),
        'stamp_duty': round(stamp_duty, 2),
        'total_charges': round(total_charges, 2),
        'cost_percentage': round(cost_percentage, 4),
        'points_to_breakeven': round(points_to_breakeven, 2),
        'buy_turnover': round(buy_turnover, 2),
        'sell_turnover': round(sell_turnover, 2)
    }


def calculate_net_pnl(buy_price, sell_price, quantity):
    """
    Calculate net P&L after all transaction costs
    
    Returns:
        tuple: (gross_pnl, costs, net_pnl, net_pnl_percentage)
    """
    costs = calculate_transaction_costs(buy_price, sell_price, quantity)
    
    # Gross P&L
    gross_pnl = (sell_price - buy_price) * quantity
    
    # Net P&L after costs
    net_pnl = gross_pnl - costs['total_charges']
    
    # Net P&L as percentage of investment
    net_pnl_percentage = (net_pnl / costs['buy_turnover']) * 100
    
    return gross_pnl, costs['total_charges'], net_pnl, net_pnl_percentage


# Test with user's example
if __name__ == '__main__':
    # Example from user: Buy 1000, Sell 1100, Qty 400
    buy = 1000
    sell = 1100
    qty = 400
    
    print("=" * 80)
    print("TRANSACTION COST CALCULATION")
    print("=" * 80)
    print(f"\nBUY: ₹{buy}")
    print(f"SELL: ₹{sell}")
    print(f"QUANTITY: {qty}")
    print(f"\nGross Profit: ₹{(sell - buy) * qty:,.2f}")
    
    costs = calculate_transaction_costs(buy, sell, qty)
    
    print(f"\n{'Charge Type':<25} {'Amount (₹)':<15}")
    print("-" * 80)
    print(f"{'Brokerage':<25} {costs['brokerage']:>10,.2f}")
    print(f"{'STT':<25} {costs['stt']:>10,.2f}")
    print(f"{'Exchange charges':<25} {costs['exchange_charges']:>10,.2f}")
    print(f"{'GST':<25} {costs['gst']:>10,.2f}")
    print(f"{'SEBI charges':<25} {costs['sebi_charges']:>10,.2f}")
    print(f"{'Stamp duty':<25} {costs['stamp_duty']:>10,.2f}")
    print("-" * 80)
    print(f"{'TOTAL CHARGES':<25} {costs['total_charges']:>10,.2f}")
    print(f"{'Cost %':<25} {costs['cost_percentage']:>9,.4f}%")
    print(f"{'Points to breakeven':<25} {costs['points_to_breakeven']:>10,.2f}")
    
    gross, total_costs, net, net_pct = calculate_net_pnl(buy, sell, qty)
    
    print(f"\n{'NET P&L':<25} {net:>10,.2f}")
    print(f"{'NET P&L %':<25} {net_pct:>9,.2f}%")
    print("=" * 80)
