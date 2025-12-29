import pandas as pd
import numpy as np
import pytz

print("=" * 100)
print("LAST TRADED PRICE & QUANTITY COMPARISON: KITECONNECT vs DHAN")
print("=" * 100)

# Load data
dhan_df = pd.read_csv('dhan_nifty_futures_ticks.csv')
kite_df = pd.read_csv('kiteconnect_nifty_futures_ticks.csv')

# Parse timestamps
dhan_df['time'] = pd.to_datetime(dhan_df['time'])
kite_df['time'] = pd.to_datetime(kite_df['time'])

# Convert to IST
ist = pytz.timezone('Asia/Kolkata')
if kite_df['time'].dt.tz is None:
    kite_df['time'] = kite_df['time'].dt.tz_localize('UTC').dt.tz_convert(ist)
else:
    kite_df['time'] = kite_df['time'].dt.tz_convert(ist)

if dhan_df['time'].dt.tz is None:
    dhan_df['time'] = dhan_df['time'].dt.tz_localize(ist)

# Get LTP and LTQ columns
kite_ltp = kite_df['last_price'].dropna()
kite_ltq = kite_df['last_traded_quantity'].dropna()
dhan_ltp = dhan_df['ltp'].dropna()
dhan_ltq = dhan_df['ltq'].dropna()

print(f"\n📊 LAST TRADED PRICE (LTP) ANALYSIS")
print("=" * 100)
print(f"{'Metric':<40} {'KiteConnect':<30} {'Dhan':<30}")
print("-" * 100)

print(f"{'Total LTP Updates':<40} {len(kite_ltp):<30,} {len(dhan_ltp):<30,}")
print(f"{'Min LTP':<40} ₹{kite_ltp.min():<29,.2f} ₹{dhan_ltp.min():<29,.2f}")
print(f"{'Max LTP':<40} ₹{kite_ltp.max():<29,.2f} ₹{dhan_ltp.max():<29,.2f}")
print(f"{'Average LTP':<40} ₹{kite_ltp.mean():<29,.2f} ₹{dhan_ltp.mean():<29,.2f}")
print(f"{'Median LTP':<40} ₹{kite_ltp.median():<29,.2f} ₹{dhan_ltp.median():<29,.2f}")
print(f"{'Std Deviation':<40} ₹{kite_ltp.std():<29,.2f} ₹{dhan_ltp.std():<29,.2f}")
print(f"{'Price Range':<40} ₹{kite_ltp.max() - kite_ltp.min():<29,.2f} ₹{dhan_ltp.max() - dhan_ltp.min():<29,.2f}")

# Unique prices
kite_unique_prices = kite_ltp.nunique()
dhan_unique_prices = dhan_ltp.nunique()
print(f"{'Unique Price Levels':<40} {kite_unique_prices:<30,} {dhan_unique_prices:<30,}")

# Price precision
kite_precision = []
dhan_precision = []
for price in kite_ltp:
    decimal = str(price).split('.')[-1]
    kite_precision.append(len(decimal))
for price in dhan_ltp:
    decimal = str(price).split('.')[-1]
    dhan_precision.append(len(decimal))

print(f"{'Avg Decimal Places':<40} {np.mean(kite_precision):<30.2f} {np.mean(dhan_precision):<30.2f}")

# Price changes
kite_df['ltp_change'] = kite_df['last_price'].diff()
dhan_df['ltp_change'] = dhan_df['ltp'].diff()

kite_price_changes = kite_df['ltp_change'].dropna()
dhan_price_changes = dhan_df['ltp_change'].dropna()

print(f"\n📈 PRICE MOVEMENT ANALYSIS")
print("-" * 100)
print(f"{'Total Price Changes':<40} {len(kite_price_changes[kite_price_changes != 0]):<30,} {len(dhan_price_changes[dhan_price_changes != 0]):<30,}")
print(f"{'% of Ticks with Price Change':<40} {len(kite_price_changes[kite_price_changes != 0])/len(kite_price_changes)*100:<30.1f} {len(dhan_price_changes[dhan_price_changes != 0])/len(dhan_price_changes)*100:<30.1f}")
print(f"{'Avg Price Change (when changed)':<40} ₹{kite_price_changes[kite_price_changes != 0].abs().mean():<29,.2f} ₹{dhan_price_changes[dhan_price_changes != 0].abs().mean():<29,.2f}")
print(f"{'Max Single Price Jump':<40} ₹{kite_price_changes.abs().max():<29,.2f} ₹{dhan_price_changes.abs().max():<29,.2f}")

print(f"\n📦 LAST TRADED QUANTITY (LTQ) ANALYSIS")
print("=" * 100)
print(f"{'Metric':<40} {'KiteConnect':<30} {'Dhan':<30}")
print("-" * 100)

print(f"{'Total LTQ Updates':<40} {len(kite_ltq):<30,} {len(dhan_ltq):<30,}")
print(f"{'Min LTQ':<40} {kite_ltq.min():<30,} {dhan_ltq.min():<30,.0f}")
print(f"{'Max LTQ':<40} {kite_ltq.max():<30,} {dhan_ltq.max():<30,.0f}")
print(f"{'Average LTQ':<40} {kite_ltq.mean():<30,.1f} {dhan_ltq.mean():<30,.1f}")
print(f"{'Median LTQ':<40} {kite_ltq.median():<30,.0f} {dhan_ltq.median():<30,.0f}")
print(f"{'Std Deviation':<40} {kite_ltq.std():<30,.1f} {dhan_ltq.std():<30,.1f}")

# Unique quantities
kite_unique_qty = kite_ltq.nunique()
dhan_unique_qty = dhan_ltq.nunique()
print(f"{'Unique Quantity Levels':<40} {kite_unique_qty:<30,} {dhan_unique_qty:<30,}")

# Quantity distribution
print(f"\n📊 QUANTITY SIZE DISTRIBUTION")
print("-" * 100)
qty_bins = [0, 25, 50, 75, 100, 150, 200, float('inf')]
qty_labels = ['1-25', '26-50', '51-75', '76-100', '101-150', '151-200', '>200']

kite_qty_dist = pd.cut(kite_ltq, bins=qty_bins, labels=qty_labels).value_counts().sort_index()
dhan_qty_dist = pd.cut(dhan_ltq, bins=qty_bins, labels=qty_labels).value_counts().sort_index()

print(f"{'Quantity Range':<20} {'KiteConnect':<30} {'Dhan':<30}")
print("-" * 80)
for label in qty_labels:
    kite_val = kite_qty_dist.get(label, 0)
    dhan_val = dhan_qty_dist.get(label, 0)
    kite_pct = (kite_val / len(kite_ltq) * 100) if len(kite_ltq) > 0 else 0
    dhan_pct = (dhan_val / len(dhan_ltq) * 100) if len(dhan_ltq) > 0 else 0
    print(f"{label:<20} {kite_val:>8,} ({kite_pct:>5.1f}%)        {dhan_val:>8,} ({dhan_pct:>5.1f}%)")

# Match ticks by time (within 1 second window)
print(f"\n🔍 TICK-BY-TICK COMPARISON (First 20 matched)")
print("=" * 100)
print(f"{'Time (IST)':<20} {'Kite LTP':<15} {'Dhan LTP':<15} {'LTP Diff':<15} {'Kite LTQ':<12} {'Dhan LTQ':<12} {'LTQ Diff':<12}")
print("-" * 100)

matched_count = 0
total_ltp_diff = 0
total_ltq_diff = 0
ltp_differences = []
ltq_differences = []

for idx, kite_row in kite_df.head(50).iterrows():
    kite_time = kite_row['time']
    
    # Find Dhan tick within 1 second
    time_window = dhan_df[
        (dhan_df['time'] >= kite_time - pd.Timedelta(seconds=1)) &
        (dhan_df['time'] <= kite_time + pd.Timedelta(seconds=1))
    ]
    
    if len(time_window) > 0:
        dhan_row = time_window.iloc[0]
        
        kite_ltp_val = kite_row['last_price']
        dhan_ltp_val = dhan_row['ltp']
        kite_ltq_val = kite_row['last_traded_quantity']
        dhan_ltq_val = dhan_row['ltq']
        
        if pd.notna(kite_ltp_val) and pd.notna(dhan_ltp_val):
            ltp_diff = abs(kite_ltp_val - dhan_ltp_val)
            total_ltp_diff += ltp_diff
            ltp_differences.append(ltp_diff)
            
            if pd.notna(kite_ltq_val) and pd.notna(dhan_ltq_val):
                ltq_diff = abs(kite_ltq_val - dhan_ltq_val)
                total_ltq_diff += ltq_diff
                ltq_differences.append(ltq_diff)
            else:
                ltq_diff = 0
            
            if matched_count < 20:
                print(f"{kite_time.strftime('%H:%M:%S'):<20} ₹{kite_ltp_val:<14,.2f} ₹{dhan_ltp_val:<14,.2f} "
                      f"₹{ltp_diff:<14,.2f} {kite_ltq_val:<12,.0f} {dhan_ltq_val:<12,.0f} {ltq_diff:<12,.0f}")
            
            matched_count += 1

print(f"\n📊 MATCHED TICK STATISTICS")
print("=" * 100)
print(f"Total matched ticks: {matched_count}")
print(f"Average LTP difference: ₹{np.mean(ltp_differences):.4f}")
print(f"Max LTP difference: ₹{np.max(ltp_differences):.4f}")
print(f"Ticks with identical LTP: {sum([1 for d in ltp_differences if d == 0])} ({sum([1 for d in ltp_differences if d == 0])/len(ltp_differences)*100:.1f}%)")

if len(ltq_differences) > 0:
    print(f"\nAverage LTQ difference: {np.mean(ltq_differences):.2f} contracts")
    print(f"Max LTQ difference: {np.max(ltq_differences):.0f} contracts")
    print(f"Ticks with identical LTQ: {sum([1 for d in ltq_differences if d == 0])} ({sum([1 for d in ltq_differences if d == 0])/len(ltq_differences)*100:.1f}%)")

print(f"\n📈 PRICE TRACKING ACCURACY")
print("=" * 100)

# Check how many times each API was first to report a price change
kite_first = 0
dhan_first = 0

# Get significant price changes (>0.10)
kite_sig_changes = kite_df[kite_df['ltp_change'].abs() > 0.10][['time', 'last_price', 'ltp_change']].head(10)
dhan_sig_changes = dhan_df[dhan_df['ltp_change'].abs() > 0.10][['time', 'ltp', 'ltp_change']].head(10)

print(f"\nTop Price Movements - KiteConnect:")
print(f"{'Time':<20} {'Price':<15} {'Change':<15}")
print("-" * 50)
for idx, row in kite_sig_changes.iterrows():
    print(f"{row['time'].strftime('%H:%M:%S'):<20} ₹{row['last_price']:<14,.2f} {row['ltp_change']:+.2f}")

print(f"\nTop Price Movements - Dhan:")
print(f"{'Time':<20} {'Price':<15} {'Change':<15}")
print("-" * 50)
for idx, row in dhan_sig_changes.iterrows():
    print(f"{row['time'].strftime('%H:%M:%S'):<20} ₹{row['ltp']:<14,.2f} {row['ltp_change']:+.2f}")

print(f"\n✅ SUMMARY")
print("=" * 100)

if len(dhan_ltp) > len(kite_ltp):
    print(f"✓ Dhan provides {len(dhan_ltp) - len(kite_ltp)} more LTP updates ({(len(dhan_ltp)/len(kite_ltp)-1)*100:.1f}% more)")
else:
    print(f"✓ KiteConnect provides {len(kite_ltp) - len(dhan_ltp)} more LTP updates")

if np.mean(ltp_differences) < 0.01:
    print(f"✓ LTP values are nearly identical (avg diff: ₹{np.mean(ltp_differences):.4f})")
else:
    print(f"⚠ LTP values differ by ₹{np.mean(ltp_differences):.4f} on average")

if dhan_unique_prices > kite_unique_prices:
    print(f"✓ Dhan captured {dhan_unique_prices - kite_unique_prices} more unique price levels")
elif kite_unique_prices > dhan_unique_prices:
    print(f"✓ KiteConnect captured {kite_unique_prices - dhan_unique_prices} more unique price levels")

if kite_ltq.mean() == dhan_ltq.mean():
    print(f"✓ LTQ averages are identical ({kite_ltq.mean():.0f} contracts)")
else:
    print(f"ℹ LTQ averages differ: Kite={kite_ltq.mean():.1f}, Dhan={dhan_ltq.mean():.1f}")

print("\n" + "=" * 100)
