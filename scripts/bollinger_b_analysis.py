"""
Bollinger %B Analysis
Simple trend continuation signal: %B > 105 or < -5 with further strength/weakness
Field: Volume
"""

import pandas as pd
import numpy as np
import sys


def calculate_transaction_costs(buy_price, sell_price, quantity):
    """
    Calculate all transaction costs for delivery equity trade
    
    Args:
        buy_price: Entry price
        sell_price: Exit price
        quantity: Number of shares (default 1 for percentage calculation)
    
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
    
    # Calculate as percentage of position size (buy turnover)
    cost_percentage = (total_charges / buy_turnover) * 100
    
    return {
        'total_charges': total_charges,
        'cost_percentage': cost_percentage,
        'brokerage': brokerage,
        'stt': stt,
        'exchange_charges': exchange_charges,
        'gst': gst,
        'sebi_charges': sebi_charges,
        'stamp_duty': stamp_duty
    }


def triangular_ma(series, period):
    """Triangular Moving Average - ChartIQ methodology
    TMA is calculated by applying SMA twice with period (N+1)/2
    This creates a weighted average with triangular weights"""
    # Calculate the smoothing period for TMA
    smooth_period = int(np.ceil((period + 1) / 2))
    
    # First smoothing pass
    sma1 = series.rolling(window=smooth_period).mean()
    
    # Second smoothing pass with same period
    tma = sma1.rolling(window=smooth_period).mean()
    
    return tma


def bollinger_percent_b(series, period=20, std_dev=2, ma_type='sma'):
    """
    Bollinger %B calculation
    %B = (Price - Lower Band) / (Upper Band - Lower Band)
    %B > 1 means price above upper band
    %B < 0 means price below lower band
    """
    # Calculate moving average based on type
    if ma_type == 'triangular':
        middle_band = triangular_ma(series, period)
    else:
        middle_band = series.rolling(window=period).mean()
    
    # Calculate standard deviation
    std = series.rolling(window=period).std()
    
    # Calculate bands
    upper_band = middle_band + (std_dev * std)
    lower_band = middle_band - (std_dev * std)
    
    # Calculate %B
    percent_b = (series - lower_band) / (upper_band - lower_band)
    
    return percent_b * 100, middle_band, upper_band, lower_band


def identify_signals(df, percent_b):
    """
    Identify ALERT candles when %B crosses above 105 or below -5
    These represent significant buying/selling activity
    """
    df['percent_b'] = percent_b
    
    alerts = []
    
    for i in range(1, len(df)):
        current_b = df.at[i, 'percent_b']
        prev_b = df.at[i-1, 'percent_b']
        current_open = df.at[i, 'open']
        current_close = df.at[i, 'close']
        
        # Skip if NaN
        if pd.isna(current_b) or pd.isna(prev_b):
            continue
        
        # Bullish alert: %B crosses above 105 AND candle closes green (close > open)
        if prev_b <= 105 and current_b > 105 and current_close > current_open:
            alerts.append({
                'type': 'BULLISH_ALERT',
                'alert_date': df.at[i, 'date'],
                'alert_index': i,
                'alert_close': df.at[i, 'close'],
                'alert_high': df.at[i, 'high'],
                'alert_low': df.at[i, 'low'],
                'alert_b': current_b
            })
        
        # Bearish alert: %B crosses below -5 AND candle closes red (close < open)
        elif prev_b >= -5 and current_b < -5 and current_close < current_open:
            alerts.append({
                'type': 'BEARISH_ALERT',
                'alert_date': df.at[i, 'date'],
                'alert_index': i,
                'alert_close': df.at[i, 'close'],
                'alert_high': df.at[i, 'high'],
                'alert_low': df.at[i, 'low'],
                'alert_b': current_b
            })
    
    return alerts


def analyze_signal_performance(df, alerts, confirmation_window=10):
    """
    CORRECT STRATEGY:
    1. Trigger: %B > 105 (bullish) or < -5 (bearish) - mark candle high/low
    2. Confirmation: Candle within 10 days with close > trigger high (bullish) or close < trigger low (bearish)
    3. Entry: At confirmation candle's close
    4. Stop Loss: Confirmation candle's low (bullish) or high (bearish)
    5. Exit: When low breaks SL (bullish) or high breaks SL (bearish)
    """
    results = []
    
    for alert in alerts:
        alert_idx = alert['alert_index']
        alert_close = alert['alert_close']
        alert_high = alert['alert_high']
        alert_low = alert['alert_low']
        alert_b = alert['alert_b']
        
        # Look for confirmation ANY time after trigger
        entry_found = False
        entry_idx = None
        entry_price = None
        stop_loss = None
        confirmation_low = None
        confirmation_high = None
        
        if alert['type'] == 'BULLISH_ALERT':
            # Wait for close > trigger high within confirmation_window days
            max_search = min(confirmation_window + 1, len(df) - alert_idx)
            for j in range(1, max_search):
                check_idx = alert_idx + j
                check_close = df.at[check_idx, 'close']
                check_low = df.at[check_idx, 'low']
                
                # Confirmation: close above trigger high
                if check_close > alert_high:
                    entry_found = True
                    entry_idx = check_idx
                    entry_price = check_close  # Enter at confirmation close
                    stop_loss = check_low  # SL = confirmation candle low
                    confirmation_low = check_low
                    break
            
            if not entry_found:
                continue  # No confirmation, skip trade
            
            # Calculate profit target (1:2 risk-reward)
            risk = entry_price - stop_loss
            profit_target = entry_price + (2 * risk)
            
            # Manage the trade - exit on SL or profit target
            exit_price = None
            exit_date = None
            exit_reason = None
            max_favorable = 0
            max_adverse = 0
            bars_held = 0
            
            for k in range(1, len(df) - entry_idx):
                future_idx = entry_idx + k
                future_low = df.at[future_idx, 'low']
                future_high = df.at[future_idx, 'high']
                future_close = df.at[future_idx, 'close']
                future_date = df.at[future_idx, 'date']
                
                bars_held = k
                
                # Track max favorable/adverse
                gain = (future_high - entry_price) / entry_price * 100
                loss = (future_low - entry_price) / entry_price * 100
                if gain > max_favorable:
                    max_favorable = gain
                if loss < max_adverse:
                    max_adverse = loss
                
                # Exit: profit target hit
                if future_high >= profit_target:
                    exit_price = profit_target
                    exit_date = future_date
                    exit_reason = 'TARGET_HIT'
                    break
                
                # Exit: stop loss hit
                if future_low <= stop_loss:
                    exit_price = stop_loss
                    exit_date = future_date
                    exit_reason = 'STOP_LOSS_HIT'
                    break
            
            # If still in trade
            if exit_price is None:
                exit_price = df.iloc[-1]['close']
                exit_date = df.iloc[-1]['date']
                exit_reason = 'STILL_OPEN'
                bars_held = len(df) - entry_idx - 1
            
            # Calculate gross and net P&L
            final_pnl = (exit_price - entry_price) / entry_price * 100
            
            # Calculate transaction costs using ₹50,000 position size
            # Quantity = 50,000 / entry_price (rounded down to whole shares)
            quantity = int(50000 / entry_price)
            costs = calculate_transaction_costs(entry_price, exit_price, quantity)
            cost_pct = costs['cost_percentage']
            net_pnl = final_pnl - cost_pct
            
        else:  # BEARISH_ALERT
            # Wait for close < trigger low within confirmation_window days
            max_search = min(confirmation_window + 1, len(df) - alert_idx)
            for j in range(1, max_search):
                check_idx = alert_idx + j
                check_close = df.at[check_idx, 'close']
                check_high = df.at[check_idx, 'high']
                
                # Confirmation: close below trigger low
                if check_close < alert_low:
                    entry_found = True
                    entry_idx = check_idx
                    entry_price = check_close  # Enter at confirmation close
                    stop_loss = check_high  # SL = confirmation candle high
                    confirmation_high = check_high
                    break
            
            if not entry_found:
                continue  # No confirmation, skip trade
            
            # Calculate profit target (1:2 risk-reward for shorts)
            risk = stop_loss - entry_price
            profit_target = entry_price - (2 * risk)
            
            # Manage the trade - exit on SL or profit target
            exit_price = None
            exit_date = None
            exit_reason = None
            max_favorable = 0
            max_adverse = 0
            bars_held = 0
            
            for k in range(1, len(df) - entry_idx):
                future_idx = entry_idx + k
                future_low = df.at[future_idx, 'low']
                future_high = df.at[future_idx, 'high']
                future_close = df.at[future_idx, 'close']
                future_date = df.at[future_idx, 'date']
                
                bars_held = k
                
                # Track max favorable/adverse (inverted for shorts)
                gain = (entry_price - future_low) / entry_price * 100
                loss = (entry_price - future_high) / entry_price * 100
                if gain > max_favorable:
                    max_favorable = gain
                if loss < max_adverse:
                    max_adverse = loss
                
                # Exit: profit target hit
                if future_low <= profit_target:
                    exit_price = profit_target
                    exit_date = future_date
                    exit_reason = 'TARGET_HIT'
                    break
                
                # Exit: stop loss hit
                if future_high >= stop_loss:
                    exit_price = stop_loss
                    exit_date = future_date
                    exit_reason = 'STOP_LOSS_HIT'
                    break
                    exit_reason = 'STOP_LOSS_HIT'
                    break
            
            # If still in trade
            if exit_price is None:
                exit_price = df.iloc[-1]['close']
                exit_date = df.iloc[-1]['date']
                exit_reason = 'STILL_OPEN'
                bars_held = len(df) - entry_idx - 1
            
            # Calculate gross and net P&L
            final_pnl = (entry_price - exit_price) / entry_price * 100
            
            # Calculate transaction costs using ₹50,000 position size
            # Quantity = 50,000 / entry_price (rounded down to whole shares)
            quantity = int(50000 / entry_price)
            costs = calculate_transaction_costs(entry_price, exit_price, quantity)
            cost_pct = costs['cost_percentage']
            net_pnl = final_pnl - cost_pct
        
        results.append({
            'type': alert['type'].replace('_ALERT', ''),
            'alert_date': alert['alert_date'],
            'alert_price': alert_close,
            'alert_high': alert_high,
            'alert_low': alert_low,
            'alert_b': alert_b,
            'entry_date': df.at[entry_idx, 'date'],
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'exit_date': exit_date,
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'bars_held': bars_held,
            'pnl_gross': final_pnl,
            'transaction_cost': cost_pct,
            'pnl_net': net_pnl,
            'max_favorable': max_favorable,
            'max_adverse': max_adverse
        })
    
    return results


def print_analysis(df, results):
    """Print analysis results"""
    print("=" * 100)
    print(" " * 30 + "BOLLINGER %B ANALYSIS")
    print(" " * 25 + "Extreme Continuation Strategy")
    print("=" * 100)
    
    print(f"\n📊 DATA INFO")
    print(f"   Period: {df.iloc[0]['date'].strftime('%Y-%m-%d')} to {df.iloc[-1]['date'].strftime('%Y-%m-%d')}")
    print(f"   Total Candles: {len(df):,}")
    print(f"   Current Price: ₹{df.iloc[-1]['close']:.2f}")
    print(f"   Current %B: {df.iloc[-1]['percent_b']:.1f}")
    
    print(f"\n📈 INDICATOR SETTINGS")
    print(f"   Source: Volume")
    print(f"   MA Type: Simple")
    print(f"   Period: 20")
    print(f"   Std Dev: 2")
    print(f"   Signals: %B > 105 (bullish) or %B < -5 (bearish)")
    print(f"   Filter: Candle color must align with %B direction")
    
    print(f"\n💼 TRADE MANAGEMENT")
    print(f"   Position Size: ₹50,000 per trade")
    print(f"   Trigger: %B crosses 105 / -5 (mark candle high/low)")
    print(f"   Confirmation: Close > trigger high (bullish) or close < trigger low (bearish) within 10 days")
    print(f"   Entry: At confirmation candle close")
    print(f"   Stop Loss: Confirmation candle low (bullish) or high (bearish)")
    print(f"   Profit Target: 1:2 Risk-Reward (2x the risk)")
    print(f"   Exit: SL hit or Target hit")
    
    # Separate by type
    bullish_signals = [r for r in results if r['type'] == 'BULLISH']
    bearish_signals = [r for r in results if r['type'] == 'BEARISH']
    
    print(f"\n" + "=" * 100)
    print(f"📊 SIGNAL SUMMARY")
    print("=" * 100)
    
    print(f"\nTotal Signals: {len(results)}")
    print(f"  Bullish (%B > 105): {len(bullish_signals)}")
    print(f"  Bearish (%B < -5): {len(bearish_signals)}")
    
    # Bullish stats
    if bullish_signals:
        print(f"\n🟢 BULLISH SIGNALS ANALYSIS:")
        winning_trades = sum(1 for r in bullish_signals if r['pnl_net'] > 0)
        avg_pnl_gross = np.mean([r['pnl_gross'] for r in bullish_signals])
        avg_pnl_net = np.mean([r['pnl_net'] for r in bullish_signals])
        avg_cost = np.mean([r['transaction_cost'] for r in bullish_signals])
        avg_favorable = np.mean([r['max_favorable'] for r in bullish_signals])
        avg_adverse = np.mean([r['max_adverse'] for r in bullish_signals])
        avg_bars = np.mean([r['bars_held'] for r in bullish_signals])
        total_pnl_gross = sum([r['pnl_gross'] for r in bullish_signals])
        total_pnl_net = sum([r['pnl_net'] for r in bullish_signals])
        
        # Exit reason breakdown
        exit_reasons = {}
        for r in bullish_signals:
            exit_reasons[r['exit_reason']] = exit_reasons.get(r['exit_reason'], 0) + 1
        
        print(f"   Total Confirmed Trades: {len(bullish_signals)}")
        print(f"   Winning Trades: {winning_trades} ({winning_trades/len(bullish_signals)*100:.1f}%)")
        print(f"   Average P&L (Gross): {avg_pnl_gross:+.2f}%")
        print(f"   Average P&L (Net): {avg_pnl_net:+.2f}%")
        print(f"   Average Transaction Cost: {avg_cost:.3f}%")
        print(f"   Total P&L (Gross): {total_pnl_gross:+.2f}%")
        print(f"   Total P&L (Net): {total_pnl_net:+.2f}%")
        print(f"   Average Max Gain: {avg_favorable:+.2f}%")
        print(f"   Average Max Drawdown: {avg_adverse:+.2f}%")
        print(f"   Average Hold Time: {avg_bars:.0f} bars")
        
        print(f"\n   Exit Breakdown:")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"      {reason}: {count} ({count/len(bullish_signals)*100:.1f}%)")
        
        print(f"\n   Latest 5 Bullish Trades:")
        for r in bullish_signals[-5:]:
            win_loss = "WIN" if r['pnl_net'] > 0 else "LOSS"
            days_held = r['bars_held']
            print(f"   Trigger: {r['alert_date'].strftime('%Y-%m-%d')} ₹{r['alert_price']:.2f} (%B: {r['alert_b']:.0f})")
            print(f"   Entry: {r['entry_date'].strftime('%Y-%m-%d')} ₹{r['entry_price']:.2f} | SL: ₹{r['stop_loss']:.2f}")
            print(f"   Exit: {r['exit_date'].strftime('%Y-%m-%d')} ₹{r['exit_price']:.2f}")
            print(f"   Hold: {days_held} days | P&L (Gross): {r['pnl_gross']:+.2f}% | P&L (Net): {r['pnl_net']:+.2f}% | {win_loss} | {r['exit_reason']}")
            print()
    
    # Bearish stats
    if bearish_signals:
        print(f"\n🔴 BEARISH SIGNALS ANALYSIS:")
        winning_trades = sum(1 for r in bearish_signals if r['pnl_net'] > 0)
        avg_pnl_gross = np.mean([r['pnl_gross'] for r in bearish_signals])
        avg_pnl_net = np.mean([r['pnl_net'] for r in bearish_signals])
        avg_cost = np.mean([r['transaction_cost'] for r in bearish_signals])
        avg_favorable = np.mean([r['max_favorable'] for r in bearish_signals])
        avg_adverse = np.mean([r['max_adverse'] for r in bearish_signals])
        avg_bars = np.mean([r['bars_held'] for r in bearish_signals])
        total_pnl_gross = sum([r['pnl_gross'] for r in bearish_signals])
        total_pnl_net = sum([r['pnl_net'] for r in bearish_signals])
        
        # Exit reason breakdown
        exit_reasons = {}
        for r in bearish_signals:
            exit_reasons[r['exit_reason']] = exit_reasons.get(r['exit_reason'], 0) + 1
        
        print(f"   Total Confirmed Trades: {len(bearish_signals)}")
        print(f"   Winning Trades: {winning_trades} ({winning_trades/len(bearish_signals)*100:.1f}%)")
        print(f"   Average P&L (Gross): {avg_pnl_gross:+.2f}%")
        print(f"   Average P&L (Net): {avg_pnl_net:+.2f}%")
        print(f"   Average Transaction Cost: {avg_cost:.3f}%")
        print(f"   Total P&L (Gross): {total_pnl_gross:+.2f}%")
        print(f"   Total P&L (Net): {total_pnl_net:+.2f}%")
        print(f"   Average Max Gain: {avg_favorable:+.2f}%")
        print(f"   Average Max Drawdown: {avg_adverse:+.2f}%")
        print(f"   Average Hold Time: {avg_bars:.0f} bars")
        
        print(f"\n   Exit Breakdown:")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"      {reason}: {count} ({count/len(bearish_signals)*100:.1f}%)")
        
        print(f"\n   Latest 5 Bearish Trades:")
        for r in bearish_signals[-5:]:
            win_loss = "WIN" if r['pnl_net'] > 0 else "LOSS"
            days_held = r['bars_held']
            print(f"   Trigger: {r['alert_date'].strftime('%Y-%m-%d')} ₹{r['alert_price']:.2f} (%B: {r['alert_b']:.0f})")
            print(f"   Entry: {r['entry_date'].strftime('%Y-%m-%d')} ₹{r['entry_price']:.2f} | SL: ₹{r['stop_loss']:.2f}")
            print(f"   Exit: {r['exit_date'].strftime('%Y-%m-%d')} ₹{r['exit_price']:.2f}")
            print(f"   Hold: {days_held} days | P&L (Gross): {r['pnl_gross']:+.2f}% | P&L (Net): {r['pnl_net']:+.2f}% | {win_loss} | {r['exit_reason']}")
            print()
    
    # Overall stats
    print(f"\n" + "=" * 100)
    print(f"💰 OVERALL PERFORMANCE")
    print("=" * 100)
    
    if results:
        all_winning = sum(1 for r in results if r['pnl_net'] > 0)
        total_pnl_gross = sum(r['pnl_gross'] for r in results)
        total_pnl_net = sum(r['pnl_net'] for r in results)
        avg_pnl_gross = np.mean([r['pnl_gross'] for r in results])
        avg_pnl_net = np.mean([r['pnl_net'] for r in results])
        avg_cost = np.mean([r['transaction_cost'] for r in results])
        total_cost = sum(r['transaction_cost'] for r in results)
        
        print(f"\n   Total Confirmed Trades: {len(results)}")
        print(f"   Win Rate: {all_winning}/{len(results)} ({all_winning/len(results)*100:.1f}%)")
        print(f"   Total P&L (Gross): {total_pnl_gross:+.2f}%")
        print(f"   Total P&L (Net): {total_pnl_net:+.2f}%")
        print(f"   Total Transaction Costs: {total_cost:.2f}%")
        print(f"   Average P&L per Trade (Gross): {avg_pnl_gross:+.2f}%")
        print(f"   Average P&L per Trade (Net): {avg_pnl_net:+.2f}%")
        print(f"   Average Transaction Cost per Trade: {avg_cost:.3f}%")
        
        # Current position analysis
        current_b = df.iloc[-1]['percent_b']
        print(f"\n   CURRENT STATUS:")
        if current_b > 105:
            print(f"   🟢 %B at {current_b:.1f} - ABOVE 105 (Extreme Bullish ALERT)")
            print(f"   → Wait for confirmation: price must break higher with further %B strength")
        elif current_b < -5:
            print(f"   🔴 %B at {current_b:.1f} - BELOW -5 (Extreme Bearish ALERT)")
            print(f"   → Wait for confirmation: price must break lower with further %B weakness")
        else:
            print(f"   ⚪ %B at {current_b:.1f} - NEUTRAL (between -5 and 105)")
            print(f"   → Wait for extreme readings")
    
    print("\n" + "=" * 100)
    print("Analysis Complete!")
    print("=" * 100)


def main():
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = r"c:\tradingapp\reliance_1min_60days.csv"
    
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    
    print("Calculating Bollinger %B on Volume with Simple MA...")
    percent_b, middle, upper, lower = bollinger_percent_b(df['volume'], period=20, std_dev=2, ma_type='sma')
    
    print("Identifying extreme signals...")
    alerts = identify_signals(df, percent_b)
    
    print(f"Analyzing {len(alerts)} alerts for continuation and trade performance...")
    results = analyze_signal_performance(df, alerts, confirmation_window=10)
    
    print("\n")
    print_analysis(df, results)


if __name__ == '__main__':
    main()
