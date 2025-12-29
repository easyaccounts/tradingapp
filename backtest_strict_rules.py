import pandas as pd
import numpy as np
import pytz

# Load the data
df = pd.read_csv('nifty_futures11_today_all_ticks.csv')
df['time'] = pd.to_datetime(df['time'])

# Convert to IST
ist = pytz.timezone('Asia/Kolkata')
df['time_ist'] = df['time'].dt.tz_convert(ist)

# Calculate metrics
df['cvd_1min'] = df['cvd_change'].rolling(window=60, min_periods=1).sum()
df['cvd_5min'] = df['cvd_change'].rolling(window=300, min_periods=1).sum()

print("=" * 100)
print("BACKTEST: STRICT RULE APPLICATION ON TODAY'S TICK DATA")
print("=" * 100)

# Trade tracking
executed_trades = []

# Scan for entry signals
for i in range(300, len(df) - 600, 30):  # Check every 30 seconds, need room for exit
    row = df.iloc[i]
    
    # Skip lunch time and late afternoon
    hour = row['time_ist'].hour
    minute = row['time_ist'].minute
    if (hour == 11 and minute >= 25) or (hour >= 12 and hour < 14) or (hour == 15 and minute >= 25):
        continue
    
    # Check context for large orders and toxicity
    context = df.iloc[i-20:i+1]
    
    # Entry Condition Check for SHORT
    cvd_5min = row['cvd_5min']
    cvd_1min = row['cvd_1min']
    
    # Condition 1: CVD extreme
    cvd_short_signal = (cvd_5min < -20000) or (cvd_1min < -5000)
    cvd_long_signal = (cvd_5min > 20000) or (cvd_1min > 5000)
    
    # Condition 2: Toxicity
    avg_toxicity = context['depth_toxicity_tick'].mean()
    toxic_signal = avg_toxicity < 0.30
    
    # Condition 3: Large orders confirmation (at least 2)
    large_sells = context[(context['aggressor_side'] == 'SELL') & (context['volume_delta'] > 1000)]
    large_buys = context[(context['aggressor_side'] == 'BUY') & (context['volume_delta'] > 1000)]
    
    short_confirmation = len(large_sells) >= 2
    long_confirmation = len(large_buys) >= 2
    
    # Check if we should enter SHORT
    if cvd_short_signal and toxic_signal and short_confirmation:
        entry_price = row['last_price']
        entry_time = row['time_ist']
        entry_idx = i
        
        # Set stop loss
        stop_loss = entry_price + 18
        
        # Simulate trade execution
        future_window = df.iloc[i:i+2700]  # Next 45 minutes max
        
        trade_result = None
        exit_price = None
        exit_time = None
        exit_reason = None
        max_profit = 0
        
        for j, future_row in future_window.iterrows():
            current_price = future_row['last_price']
            current_time = future_row['time_ist']
            time_elapsed = (current_time - entry_time).total_seconds() / 60
            
            profit = entry_price - current_price
            max_profit = max(max_profit, profit)
            
            # Check stop loss
            if current_price >= stop_loss:
                trade_result = 'LOSS'
                exit_price = stop_loss
                exit_time = current_time
                exit_reason = 'Stop Loss Hit'
                break
            
            # Check time-based exit (11:25 AM or 45 min max)
            if (current_time.hour == 11 and current_time.minute >= 25) or time_elapsed >= 45:
                trade_result = 'WIN' if profit > 0 else 'LOSS'
                exit_price = current_price
                exit_time = current_time
                exit_reason = f"Time Exit ({time_elapsed:.0f}min)"
                break
            
            # Check CVD reversal (hard exit)
            future_cvd_1min = future_row['cvd_1min']
            if future_cvd_1min > 8000:
                trade_result = 'WIN' if profit > 0 else 'LOSS'
                exit_price = current_price
                exit_time = current_time
                exit_reason = 'CVD Reversal'
                break
            
            # Trailing stop logic
            if profit >= 25:  # After 25 points profit, trail by 5
                trail_stop = current_price + 5
                if trail_stop < stop_loss:
                    stop_loss = trail_stop
            elif profit >= 15:  # After 15 points profit, trail by 8
                trail_stop = current_price + 8
                if trail_stop < stop_loss:
                    stop_loss = trail_stop
            
            # Target-based exits
            if profit >= 50:  # Target 3 hit
                trade_result = 'WIN'
                exit_price = current_price
                exit_time = current_time
                exit_reason = 'Target 3 (+50pts)'
                break
            elif profit >= 35:  # Target 2 hit - in reality exit 80% here
                # For simplicity, continue with trailing stop
                pass
            elif profit >= 20:  # Target 1 hit - in reality exit 50% here
                # Lock profit
                if stop_loss > entry_price - 18:
                    stop_loss = entry_price - 18  # Lock minimum profit
        
        # If loop finished without exit, use last price
        if trade_result is None:
            last_row = future_window.iloc[-1]
            profit = entry_price - last_row['last_price']
            trade_result = 'WIN' if profit > 0 else 'LOSS'
            exit_price = last_row['last_price']
            exit_time = last_row['time_ist']
            exit_reason = 'Max Duration'
        
        final_profit = entry_price - exit_price
        
        executed_trades.append({
            'direction': 'SHORT',
            'entry_time': entry_time,
            'entry_price': entry_price,
            'exit_time': exit_time,
            'exit_price': exit_price,
            'points': final_profit,
            'profit_per_lot': final_profit * 75,
            'result': trade_result,
            'exit_reason': exit_reason,
            'max_profit': max_profit,
            'cvd_5min': cvd_5min,
            'cvd_1min': cvd_1min,
            'toxicity': avg_toxicity,
            'duration': (exit_time - entry_time).total_seconds() / 60
        })
    
    # Check if we should enter LONG
    elif cvd_long_signal and toxic_signal and long_confirmation:
        entry_price = row['last_price']
        entry_time = row['time_ist']
        entry_idx = i
        
        # Set stop loss
        stop_loss = entry_price - 18
        
        # Simulate trade execution
        future_window = df.iloc[i:i+2700]  # Next 45 minutes max
        
        trade_result = None
        exit_price = None
        exit_time = None
        exit_reason = None
        max_profit = 0
        
        for j, future_row in future_window.iterrows():
            current_price = future_row['last_price']
            current_time = future_row['time_ist']
            time_elapsed = (current_time - entry_time).total_seconds() / 60
            
            profit = current_price - entry_price
            max_profit = max(max_profit, profit)
            
            # Check stop loss
            if current_price <= stop_loss:
                trade_result = 'LOSS'
                exit_price = stop_loss
                exit_time = current_time
                exit_reason = 'Stop Loss Hit'
                break
            
            # Check time-based exit
            if (current_time.hour == 11 and current_time.minute >= 25) or time_elapsed >= 45:
                trade_result = 'WIN' if profit > 0 else 'LOSS'
                exit_price = current_price
                exit_time = current_time
                exit_reason = f"Time Exit ({time_elapsed:.0f}min)"
                break
            
            # Check CVD reversal
            future_cvd_1min = future_row['cvd_1min']
            if future_cvd_1min < -8000:
                trade_result = 'WIN' if profit > 0 else 'LOSS'
                exit_price = current_price
                exit_time = current_time
                exit_reason = 'CVD Reversal'
                break
            
            # Trailing stop logic
            if profit >= 25:
                trail_stop = current_price - 5
                if trail_stop > stop_loss:
                    stop_loss = trail_stop
            elif profit >= 15:
                trail_stop = current_price - 8
                if trail_stop > stop_loss:
                    stop_loss = trail_stop
            
            # Target-based exits
            if profit >= 50:
                trade_result = 'WIN'
                exit_price = current_price
                exit_time = current_time
                exit_reason = 'Target 3 (+50pts)'
                break
        
        # If loop finished without exit
        if trade_result is None:
            last_row = future_window.iloc[-1]
            profit = last_row['last_price'] - entry_price
            trade_result = 'WIN' if profit > 0 else 'LOSS'
            exit_price = last_row['last_price']
            exit_time = last_row['time_ist']
            exit_reason = 'Max Duration'
        
        final_profit = exit_price - entry_price
        
        executed_trades.append({
            'direction': 'LONG',
            'entry_time': entry_time,
            'entry_price': entry_price,
            'exit_time': exit_time,
            'exit_price': exit_price,
            'points': final_profit,
            'profit_per_lot': final_profit * 75,
            'result': trade_result,
            'exit_reason': exit_reason,
            'max_profit': max_profit,
            'cvd_5min': cvd_5min,
            'cvd_1min': cvd_1min,
            'toxicity': avg_toxicity,
            'duration': (exit_time - entry_time).total_seconds() / 60
        })

# Results
trades_df = pd.DataFrame(executed_trades)

print(f"\n📊 BACKTEST RESULTS:")
print("=" * 100)
print(f"Total Signals Generated: {len(trades_df)}")

if len(trades_df) > 0:
    wins = trades_df[trades_df['result'] == 'WIN']
    losses = trades_df[trades_df['result'] == 'LOSS']
    
    print(f"\n✅ WINNING TRADES: {len(wins)} ({len(wins)/len(trades_df)*100:.1f}%)")
    print(f"❌ LOSING TRADES: {len(losses)} ({len(losses)/len(trades_df)*100:.1f}%)")
    
    print(f"\n💰 PROFIT/LOSS SUMMARY:")
    total_profit = trades_df['profit_per_lot'].sum()
    avg_win = wins['profit_per_lot'].mean() if len(wins) > 0 else 0
    avg_loss = losses['profit_per_lot'].mean() if len(losses) > 0 else 0
    
    print(f"Total P&L (1 lot): ₹{total_profit:,.2f}")
    print(f"Average Win: ₹{avg_win:,.2f} ({wins['points'].mean():.2f} points)")
    print(f"Average Loss: ₹{avg_loss:,.2f} ({losses['points'].mean():.2f} points)")
    print(f"Profit Factor: {abs(wins['profit_per_lot'].sum() / losses['profit_per_lot'].sum()):.2f}" if len(losses) > 0 else "N/A")
    
    print(f"\n📈 PERFORMANCE METRICS:")
    print(f"Win Rate: {len(wins)/len(trades_df)*100:.1f}%")
    print(f"Average Trade: ₹{trades_df['profit_per_lot'].mean():,.2f}")
    print(f"Best Trade: ₹{trades_df['profit_per_lot'].max():,.2f} ({trades_df['points'].max():.2f} points)")
    print(f"Worst Trade: ₹{trades_df['profit_per_lot'].min():,.2f} ({trades_df['points'].min():.2f} points)")
    print(f"Average Duration: {trades_df['duration'].mean():.1f} minutes")
    
    print(f"\n📋 ALL TRADES DETAIL:")
    print("=" * 100)
    
    for idx, trade in trades_df.iterrows():
        result_emoji = "✅" if trade['result'] == 'WIN' else "❌"
        print(f"\n{result_emoji} Trade #{idx+1}: {trade['direction']}")
        print(f"  Entry:  {trade['entry_time'].strftime('%H:%M:%S')} @ ₹{trade['entry_price']:,.2f}")
        print(f"  Exit:   {trade['exit_time'].strftime('%H:%M:%S')} @ ₹{trade['exit_price']:,.2f}")
        print(f"  Result: {trade['points']:+.2f} points = ₹{trade['profit_per_lot']:+,.2f} (1 lot)")
        print(f"  Reason: {trade['exit_reason']}")
        print(f"  Duration: {trade['duration']:.1f} minutes")
        print(f"  Entry Signals: CVD 5min={trade['cvd_5min']:,.0f}, CVD 1min={trade['cvd_1min']:,.0f}, Tox={trade['toxicity']:.3f}")
    
    # Direction breakdown
    shorts = trades_df[trades_df['direction'] == 'SHORT']
    longs = trades_df[trades_df['direction'] == 'LONG']
    
    print(f"\n📊 DIRECTION BREAKDOWN:")
    print("=" * 100)
    print(f"SHORT trades: {len(shorts)} | Win rate: {len(shorts[shorts['result']=='WIN'])/len(shorts)*100:.1f}% | P&L: ₹{shorts['profit_per_lot'].sum():,.2f}")
    print(f"LONG trades:  {len(longs)} | Win rate: {len(longs[longs['result']=='WIN'])/len(longs)*100:.1f}% | P&L: ₹{longs['profit_per_lot'].sum():,.2f}")
    
    # Exit reason breakdown
    print(f"\n📋 EXIT REASON BREAKDOWN:")
    print("=" * 100)
    exit_reasons = trades_df.groupby('exit_reason').agg({
        'result': lambda x: f"{(x=='WIN').sum()}/{len(x)}",
        'profit_per_lot': 'sum',
        'points': 'mean'
    })
    print(exit_reasons.to_string())
    
    print(f"\n💼 POSITION SIZING EXAMPLES:")
    print("=" * 100)
    for account_size in [500000, 1000000, 2000000]:
        risk_per_trade = account_size * 0.01
        lots = int(risk_per_trade / (18 * 75))
        total_pnl = total_profit * lots
        print(f"₹{account_size:,} account (1% risk) = {lots} lots → Total P&L: ₹{total_pnl:,.2f}")
    
    print(f"\n🎯 EXPECTANCY:")
    print("=" * 100)
    win_rate = len(wins) / len(trades_df)
    avg_win_pts = wins['points'].mean() if len(wins) > 0 else 0
    avg_loss_pts = abs(losses['points'].mean()) if len(losses) > 0 else 0
    expectancy = (win_rate * avg_win_pts) - ((1-win_rate) * avg_loss_pts)
    print(f"Expectancy per trade: {expectancy:.2f} points (₹{expectancy * 75:,.2f} per lot)")
    print(f"Expected profit over 100 trades: ₹{expectancy * 75 * 100:,.2f} (1 lot)")
    print(f"Expected profit over 100 trades: ₹{expectancy * 75 * 100 * 10:,.2f} (10 lots)")

else:
    print("\n⚠️ No trades matched the strict entry criteria!")
    print("This means:")
    print("  • Entry rules are very selective (good for quality)")
    print("  • May need to adjust parameters for more opportunities")
    print("  • Or today was not a typical orderflow day")

print("\n" + "=" * 100)
