"""
Dhan Binary Packet Parser
Handles unpacking of Little Endian binary messages from Dhan WebSocket feed

Reference: https://dhanhq.co/docs/v2/live-market-feed/
All data is Little Endian format
"""

import struct
import structlog
from typing import Dict, Optional, List, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo

logger = structlog.get_logger()

# Timezone
IST = ZoneInfo('Asia/Kolkata')

# Feed Response Codes
RESPONSE_TICKER = 2          # LTP + LTT (16 bytes)
RESPONSE_QUOTE = 4           # Complete quote data (51 bytes)
RESPONSE_OI = 5              # Open Interest (12 bytes)
RESPONSE_PREV_CLOSE = 6      # Previous close (16 bytes)
RESPONSE_FULL = 8            # Full packet with depth (171 bytes)
RESPONSE_DISCONNECT = 50     # Disconnection packet

# Exchange Segment Enum (from Annexure)
EXCHANGE_SEGMENTS = {
    0: 'NSE_EQ',
    1: 'NSE_FNO',
    2: 'NSE_CURRENCY',
    3: 'BSE_EQ',
    4: 'BSE_FNO',
    5: 'BSE_CURRENCY',
    6: 'MCX_COMM'
}


def parse_response_header(data: bytes) -> Optional[Dict]:
    """
    Parse 8-byte response header (common to all packets)
    
    Structure:
    - Byte 1: Response code
    - Bytes 2-3: Message length (int16)
    - Byte 4: Exchange segment
    - Bytes 5-8: Security ID (int32)
    
    Args:
        data: First 8 bytes of packet
    
    Returns:
        Dict with response_code, message_length, exchange_segment, security_id
    """
    if len(data) < 8:
        return None
    
    try:
        # Little Endian unpacking: B=uint8, h=int16, B=uint8, i=int32
        response_code = struct.unpack('<B', data[0:1])[0]
        message_length = struct.unpack('<h', data[1:3])[0]
        exchange_segment = struct.unpack('<B', data[3:4])[0]
        security_id = struct.unpack('<i', data[4:8])[0]
        
        return {
            'response_code': response_code,
            'message_length': message_length,
            'exchange_segment': EXCHANGE_SEGMENTS.get(exchange_segment, 'UNKNOWN'),
            'exchange_segment_code': exchange_segment,
            'security_id': str(security_id)
        }
    except struct.error as e:
        logger.error("header_parse_failed", error=str(e))
        return None


def parse_ticker_packet(data: bytes) -> Optional[Dict]:
    """
    Parse Ticker Packet (Response Code 2) - 16 bytes
    
    Structure after header:
    - Bytes 9-12: Last Traded Price (float32)
    - Bytes 13-16: Last Trade Time EPOCH (int32)
    """
    if len(data) < 16:
        return None
    
    try:
        header = parse_response_header(data[0:8])
        if not header:
            return None
        
        last_price = struct.unpack('<f', data[8:12])[0]
        ltt_epoch = struct.unpack('<i', data[12:16])[0]
        last_trade_time = datetime.fromtimestamp(ltt_epoch, tz=IST) if ltt_epoch > 0 else None
        
        return {
            **header,
            'last_price': round(last_price, 2) if last_price > 0 else None,
            'last_trade_time': last_trade_time
        }
    except struct.error as e:
        logger.error("ticker_parse_failed", error=str(e))
        return None


def parse_quote_packet(data: bytes) -> Optional[Dict]:
    """
    Parse Quote Packet (Response Code 4) - 51 bytes
    
    Structure after header (bytes 9-50):
    - 9-12: Last Traded Price (float32)
    - 13-14: Last Traded Quantity (int16)
    - 15-18: Last Trade Time (int32 EPOCH)
    - 19-22: Average Trade Price (float32)
    - 23-26: Volume (int32)
    - 27-30: Total Sell Quantity (int32)
    - 31-34: Total Buy Quantity (int32)
    - 35-38: Day Open (float32)
    - 39-42: Day Close (float32)
    - 43-46: Day High (float32)
    - 47-50: Day Low (float32)
    """
    if len(data) < 51:
        return None
    
    try:
        header = parse_response_header(data[0:8])
        if not header:
            return None
        
        last_price = struct.unpack('<f', data[8:12])[0]
        last_traded_qty = struct.unpack('<h', data[12:14])[0]
        ltt_epoch = struct.unpack('<i', data[14:18])[0]
        avg_price = struct.unpack('<f', data[18:22])[0]
        volume = struct.unpack('<i', data[22:26])[0]
        sell_qty = struct.unpack('<i', data[26:30])[0]
        buy_qty = struct.unpack('<i', data[30:34])[0]
        day_open = struct.unpack('<f', data[34:38])[0]
        day_close = struct.unpack('<f', data[38:42])[0]
        day_high = struct.unpack('<f', data[42:46])[0]
        day_low = struct.unpack('<f', data[46:50])[0]
        
        last_trade_time = datetime.fromtimestamp(ltt_epoch, tz=IST) if ltt_epoch > 0 else None
        
        return {
            **header,
            'last_price': round(last_price, 2) if last_price > 0 else None,
            'last_traded_quantity': last_traded_qty if last_traded_qty > 0 else None,
            'last_trade_time': last_trade_time,
            'average_traded_price': round(avg_price, 2) if avg_price > 0 else None,
            'volume_traded': volume if volume > 0 else None,
            'total_sell_quantity': sell_qty if sell_qty > 0 else None,
            'total_buy_quantity': buy_qty if buy_qty > 0 else None,
            'day_open': round(day_open, 2) if day_open > 0 else None,
            'day_close': round(day_close, 2) if day_close > 0 else None,
            'day_high': round(day_high, 2) if day_high > 0 else None,
            'day_low': round(day_low, 2) if day_low > 0 else None
        }
    except struct.error as e:
        logger.error("quote_parse_failed", error=str(e))
        return None


def parse_oi_packet(data: bytes) -> Optional[Dict]:
    """
    Parse OI Packet (Response Code 5) - 12 bytes
    
    Structure after header:
    - Bytes 9-12: Open Interest (int32)
    """
    if len(data) < 12:
        return None
    
    try:
        header = parse_response_header(data[0:8])
        if not header:
            return None
        
        oi = struct.unpack('<i', data[8:12])[0]
        
        return {
            **header,
            'oi': oi if oi > 0 else None
        }
    except struct.error as e:
        logger.error("oi_parse_failed", error=str(e))
        return None


def parse_prev_close_packet(data: bytes) -> Optional[Dict]:
    """
    Parse Previous Close Packet (Response Code 6) - 16 bytes
    
    Structure after header:
    - Bytes 9-12: Previous day close (float32)
    - Bytes 13-16: Previous day OI (int32)
    """
    if len(data) < 16:
        return None
    
    try:
        header = parse_response_header(data[0:8])
        if not header:
            return None
        
        prev_close = struct.unpack('<f', data[8:12])[0]
        prev_oi = struct.unpack('<i', data[12:16])[0]
        
        return {
            **header,
            'prev_close': round(prev_close, 2) if prev_close > 0 else None,
            'prev_oi': prev_oi if prev_oi > 0 else None
        }
    except struct.error as e:
        logger.error("prev_close_parse_failed", error=str(e))
        return None


def parse_market_depth_level(data: bytes, offset: int) -> Dict:
    """
    Parse single market depth level (20 bytes)
    
    Structure:
    - 0-3: Bid Quantity (int32)
    - 4-7: Ask Quantity (int32)
    - 8-9: Bid Orders (int16)
    - 10-11: Ask Orders (int16)
    - 12-15: Bid Price (float32)
    - 16-19: Ask Price (float32)
    """
    bid_qty = struct.unpack('<i', data[offset:offset+4])[0]
    ask_qty = struct.unpack('<i', data[offset+4:offset+8])[0]
    bid_orders = struct.unpack('<h', data[offset+8:offset+10])[0]
    ask_orders = struct.unpack('<h', data[offset+10:offset+12])[0]
    bid_price = struct.unpack('<f', data[offset+12:offset+16])[0]
    ask_price = struct.unpack('<f', data[offset+16:offset+20])[0]
    
    return {
        'bid_quantity': bid_qty if bid_qty > 0 else None,
        'ask_quantity': ask_qty if ask_qty > 0 else None,
        'bid_orders': bid_orders if bid_orders > 0 else None,
        'ask_orders': ask_orders if ask_orders > 0 else None,
        'bid_price': round(bid_price, 2) if bid_price > 0 else None,
        'ask_price': round(ask_price, 2) if ask_price > 0 else None
    }


def parse_full_packet(data: bytes) -> Optional[Dict]:
    """
    Parse Full Packet (Response Code 8) - 171 bytes
    
    Complete trade data + 5 levels of market depth
    
    Structure after header (bytes 9-170):
    - 9-12: LTP (float32)
    - 13-14: LTQ (int16)
    - 15-18: LTT (int32)
    - 19-22: ATP (float32)
    - 23-26: Volume (int32)
    - 27-30: Sell Qty (int32)
    - 31-34: Buy Qty (int32)
    - 35-38: OI (int32)
    - 39-42: OI High (int32)
    - 43-46: OI Low (int32)
    - 47-50: Day Open (float32)
    - 51-54: Day Close (float32)
    - 55-58: Day High (float32)
    - 59-62: Day Low (float32)
    - 63-162: Market Depth (5 levels × 20 bytes)
    """
    if len(data) < 171:
        return None
    
    try:
        header = parse_response_header(data[0:8])
        if not header:
            return None
        
        # Parse trade data
        last_price = struct.unpack('<f', data[8:12])[0]
        last_traded_qty = struct.unpack('<h', data[12:14])[0]
        ltt_epoch = struct.unpack('<i', data[14:18])[0]
        avg_price = struct.unpack('<f', data[18:22])[0]
        volume = struct.unpack('<i', data[22:26])[0]
        sell_qty = struct.unpack('<i', data[26:30])[0]
        buy_qty = struct.unpack('<i', data[30:34])[0]
        oi = struct.unpack('<i', data[34:38])[0]
        oi_high = struct.unpack('<i', data[38:42])[0]
        oi_low = struct.unpack('<i', data[42:46])[0]
        day_open = struct.unpack('<f', data[46:50])[0]
        day_close = struct.unpack('<f', data[50:54])[0]
        day_high = struct.unpack('<f', data[54:58])[0]
        day_low = struct.unpack('<f', data[58:62])[0]
        
        last_trade_time = datetime.fromtimestamp(ltt_epoch, tz=IST) if ltt_epoch > 0 else None
        
        # Parse 5 levels of market depth
        depth_levels = []
        for i in range(5):
            offset = 62 + (i * 20)
            level = parse_market_depth_level(data, offset)
            depth_levels.append(level)
        
        return {
            **header,
            'last_price': round(last_price, 2) if last_price > 0 else None,
            'last_traded_quantity': last_traded_qty if last_traded_qty > 0 else None,
            'last_trade_time': last_trade_time,
            'average_traded_price': round(avg_price, 2) if avg_price > 0 else None,
            'volume_traded': volume if volume > 0 else None,
            'total_sell_quantity': sell_qty if sell_qty > 0 else None,
            'total_buy_quantity': buy_qty if buy_qty > 0 else None,
            'oi': oi if oi > 0 else None,
            'oi_day_high': oi_high if oi_high > 0 else None,
            'oi_day_low': oi_low if oi_low > 0 else None,
            'day_open': round(day_open, 2) if day_open > 0 else None,
            'day_close': round(day_close, 2) if day_close > 0 else None,
            'day_high': round(day_high, 2) if day_high > 0 else None,
            'day_low': round(day_low, 2) if day_low > 0 else None,
            'depth': depth_levels
        }
    except struct.error as e:
        logger.error("full_packet_parse_failed", error=str(e))
        return None


def parse_disconnect_packet(data: bytes) -> Optional[Dict]:
    """
    Parse Disconnect Packet (Response Code 50) - 10 bytes
    
    Structure after header:
    - Bytes 9-10: Disconnection reason code (int16)
    """
    if len(data) < 10:
        return None
    
    try:
        header = parse_response_header(data[0:8])
        if not header:
            return None
        
        reason_code = struct.unpack('<h', data[8:10])[0]
        
        return {
            **header,
            'reason_code': reason_code
        }
    except struct.error as e:
        logger.error("disconnect_parse_failed", error=str(e))
        return None


def parse_packet(data: bytes) -> Optional[Dict]:
    """
    Main packet parser - routes to appropriate parser based on response code
    
    Args:
        data: Binary packet data
    
    Returns:
        Parsed packet dict or None if parsing fails
    """
    if len(data) < 8:
        logger.warning("packet_too_short", length=len(data))
        return None
    
    # Parse header to get response code
    header = parse_response_header(data[0:8])
    if not header:
        return None
    
    response_code = header['response_code']
    
    # Route to appropriate parser
    parsers = {
        RESPONSE_TICKER: parse_ticker_packet,
        RESPONSE_QUOTE: parse_quote_packet,
        RESPONSE_OI: parse_oi_packet,
        RESPONSE_PREV_CLOSE: parse_prev_close_packet,
        RESPONSE_FULL: parse_full_packet,
        RESPONSE_DISCONNECT: parse_disconnect_packet
    }
    
    parser = parsers.get(response_code)
    if parser:
        return parser(data)
    else:
        logger.warning("unknown_response_code", code=response_code)
        return None
