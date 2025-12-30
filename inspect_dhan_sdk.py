from dhanhq import marketfeed
import inspect

print("DhanFeed signature:")
print(inspect.signature(marketfeed.DhanFeed.__init__))
print()

print("Available marketfeed attributes:")
for attr in dir(marketfeed):
    if not attr.startswith('_'):
        print(f"  {attr}")
print()

print("Checking for subscription codes:")
for attr in ['Ticker', 'Quote', 'Depth', 'Full', 'FullMarketDepth', 'TICKER', 'QUOTE', 'DEPTH', 'FULL']:
    if hasattr(marketfeed, attr):
        val = getattr(marketfeed, attr)
        print(f"  marketfeed.{attr} = {val}")
