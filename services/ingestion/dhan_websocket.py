"""
Dhan WebSocket Feed Handler
Manages connection to Dhan's market feed WebSocket, subscribes to instruments,
processes binary packets, and publishes to RabbitMQ

Reference: https://dhanhq.co/docs/v2/live-market-feed/
"""

import asyncio
import json
import structlog
import time
from typing import List, Dict, Set, Callable, Optional
from datetime import datetime
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from dhan_parser import parse_packet, RESPONSE_DISCONNECT
from dhan_auth import get_dhan_credentials, get_websocket_url

logger = structlog.get_logger()


class DhanWebSocketClient:
    """Async WebSocket client for Dhan market feed"""
    
    # Request codes for subscription messages
    REQUEST_CONNECT = 11
    REQUEST_SUBSCRIBE = 15
    REQUEST_UNSUBSCRIBE = 16
    REQUEST_SUBSCRIBE_FULL = 21  # Full packet with depth
    REQUEST_UNSUBSCRIBE_FULL = 22
    
    # Feed modes
    FEED_MODE_TICKER = 2
    FEED_MODE_QUOTE = 4
    FEED_MODE_FULL = 8
    
    # Packet types
    PACKET_CONNECT_ACK = 20
    PACKET_SUBSCRIBE_ACK = 25
    PACKET_UNSUBSCRIBE_ACK = 26
    
    def __init__(
        self,
        on_tick: Callable[[Dict], None],
        on_connect: Optional[Callable] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        on_close: Optional[Callable[[int, str], None]] = None,
        ping_interval: int = 30,
        ping_timeout: int = 10,
        max_reconnect_attempts: int = 5,
        reconnect_delay: int = 5
    ):
        """
        Initialize Dhan WebSocket client
        
        Args:
            on_tick: Callback for parsed tick data
            on_connect: Callback on successful connection
            on_error: Callback for errors
            on_close: Callback on connection close
            ping_interval: Seconds between ping messages
            ping_timeout: Seconds to wait for pong
            max_reconnect_attempts: Max reconnection tries
            reconnect_delay: Seconds between reconnect attempts
        """
        self.on_tick = on_tick
        self.on_connect = on_connect
        self.on_error = on_error
        self.on_close = on_close
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_delay = reconnect_delay
        
        # Connection state
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.subscribed_instruments: Set[str] = set()
        self.is_connected = False
        self.reconnect_count = 0
        self.should_run = True
        
        # Credentials (loaded on connect)
        self.access_token: Optional[str] = None
        self.client_id: Optional[str] = None
        self.ws_url: Optional[str] = None
        
        # Statistics
        self.packets_received = 0
        self.packets_parsed = 0
        self.packets_failed = 0
        self.last_packet_time: Optional[datetime] = None
    
    async def connect(self):
        """Establish WebSocket connection to Dhan"""
        try:
            # Get credentials
            creds = get_dhan_credentials()
            self.access_token = creds['access_token']
            self.client_id = creds['client_id']
            self.ws_url = get_websocket_url(self.access_token, self.client_id)
            
            logger.info("connecting_to_dhan", client_id=self.client_id)
            
            # Connect with custom settings
            self.ws = await websockets.connect(
                self.ws_url,
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                max_size=2**20,  # 1MB max message size
                compression=None  # No compression for binary
            )
            
            self.is_connected = True
            self.reconnect_count = 0
            
            logger.info("dhan_connected", ws_url=self.ws_url[:50])
            
            # Trigger on_connect callback
            if self.on_connect:
                self.on_connect()
            
        except Exception as e:
            logger.error("connection_failed", error=str(e))
            if self.on_error:
                self.on_error(e)
            raise
    
    async def disconnect(self):
        """Close WebSocket connection"""
        self.should_run = False
        if self.ws:
            await self.ws.close()
            self.is_connected = False
            logger.info("dhan_disconnected")
    
    def _build_subscription_message(
        self,
        request_code: int,
        exchange_segment: int,
        security_ids: List[str]
    ) -> str:
        """
        Build JSON subscription message
        
        Args:
            request_code: 15 (subscribe), 16 (unsubscribe), 21 (full), 22 (unsubscribe full)
            exchange_segment: 0=NSE_EQ, 1=NSE_FNO, etc.
            security_ids: List of Dhan security IDs (max 100)
        
        Returns:
            JSON string for subscription
        """
        # Map segment integer to string (per Dhan API docs)
        segment_map = {
            0: 'IDX_I',
            1: 'NSE_EQ',
            2: 'NSE_FNO',
            3: 'NSE_CURRENCY',
            4: 'BSE_EQ',
            5: 'MCX_COMM',
            7: 'BSE_CURRENCY',
            8: 'BSE_FNO'
        }
        segment_str = segment_map.get(exchange_segment, 'NSE_FNO')
        
        message = {
            "RequestCode": request_code,
            "InstrumentCount": len(security_ids),
            "InstrumentList": [
                {
                    "ExchangeSegment": segment_str,
                    "SecurityId": security_id
                }
                for security_id in security_ids
            ]
        }
        return json.dumps(message)
    
    async def subscribe(
        self,
        instruments: List[Dict[str, any]],
        mode: str = "full"
    ):
        """
        Subscribe to instruments
        
        Args:
            instruments: List of dicts with 'security_id' and 'exchange_segment'
            mode: "ticker", "quote", or "full" (default full with depth)
        
        Example:
            await client.subscribe([
                {'security_id': '13', 'exchange_segment': 0},  # NIFTY 50
                {'security_id': '26000', 'exchange_segment': 1}  # NIFTY FUT
            ], mode='full')
        """
        if not self.is_connected or not self.ws:
            raise RuntimeError("Not connected to Dhan WebSocket")
        
        # Determine request code based on mode
        request_code_map = {
            "ticker": self.REQUEST_SUBSCRIBE,
            "quote": self.REQUEST_SUBSCRIBE,
            "full": self.REQUEST_SUBSCRIBE_FULL
        }
        request_code = request_code_map.get(mode, self.REQUEST_SUBSCRIBE_FULL)
        
        # Group by exchange segment (max 100 per message)
        by_exchange: Dict[int, List[str]] = {}
        for inst in instruments:
            exchange = inst.get('exchange_segment', 1)
            security_id = str(inst['security_id'])
            if exchange not in by_exchange:
                by_exchange[exchange] = []
            by_exchange[exchange].append(security_id)
        
        # Send subscription messages (chunked to max 100)
        for exchange, security_ids in by_exchange.items():
            # Split into chunks of 100
            for i in range(0, len(security_ids), 100):
                chunk = security_ids[i:i+100]
                message = self._build_subscription_message(request_code, exchange, chunk)
                
                await self.ws.send(message)
                logger.info(
                    "subscription_sent",
                    exchange=exchange,
                    count=len(chunk),
                    mode=mode
                )
                
                # Track subscribed instruments
                self.subscribed_instruments.update(chunk)
                
                # Small delay between chunks to avoid rate limiting
                if i + 100 < len(security_ids):
                    await asyncio.sleep(0.1)
        
        logger.info("subscription_complete", total_instruments=len(instruments))
    
    async def unsubscribe(self, instruments: List[Dict[str, any]]):
        """
        Unsubscribe from instruments
        
        Args:
            instruments: List of dicts with 'security_id' and 'exchange_segment'
        """
        if not self.is_connected or not self.ws:
            raise RuntimeError("Not connected to Dhan WebSocket")
        
        # Group by exchange
        by_exchange: Dict[int, List[str]] = {}
        for inst in instruments:
            exchange = inst.get('exchange_segment', 1)
            security_id = str(inst['security_id'])
            if exchange not in by_exchange:
                by_exchange[exchange] = []
            by_exchange[exchange].append(security_id)
        
        # Send unsubscribe messages
        for exchange, security_ids in by_exchange.items():
            for i in range(0, len(security_ids), 100):
                chunk = security_ids[i:i+100]
                message = self._build_subscription_message(
                    self.REQUEST_UNSUBSCRIBE_FULL,
                    exchange,
                    chunk
                )
                
                await self.ws.send(message)
                logger.info("unsubscription_sent", exchange=exchange, count=len(chunk))
                
                # Remove from tracking
                self.subscribed_instruments.difference_update(chunk)
    
    async def _handle_message(self, message: bytes):
        """
        Process incoming binary message
        
        Args:
            message: Binary packet data
        """
        self.packets_received += 1
        self.last_packet_time = datetime.now()
        
        # Parse packet
        parsed = parse_packet(message)
        
        if parsed:
            self.packets_parsed += 1
            
            # Check for disconnect packet
            if parsed.get('response_code') == RESPONSE_DISCONNECT:
                reason_code = parsed.get('reason_code', 0)
                logger.warning("disconnect_received", reason_code=reason_code)
                if self.on_close:
                    self.on_close(reason_code, "Server disconnect")
                return
            
            # Pass to callback
            if self.on_tick:
                try:
                    self.on_tick(parsed)
                except Exception as e:
                    logger.error("tick_callback_error", error=str(e))
        else:
            self.packets_failed += 1
            if self.packets_failed % 100 == 0:
                logger.warning(
                    "parsing_failures",
                    failed=self.packets_failed,
                    total=self.packets_received
                )
    
    async def _receive_loop(self):
        """Main loop to receive and process messages"""
        try:
            while self.should_run and self.is_connected:
                try:
                    message = await self.ws.recv()
                    
                    # Handle binary data
                    if isinstance(message, bytes):
                        await self._handle_message(message)
                    
                    # Handle text data (subscription acks, etc.)
                    elif isinstance(message, str):
                        try:
                            ack = json.loads(message)
                            logger.info("subscription_ack", data=ack)
                        except json.JSONDecodeError:
                            logger.warning("non_json_text_message", message=message[:100])
                
                except ConnectionClosed as e:
                    logger.warning("connection_closed", code=e.code, reason=e.reason)
                    self.is_connected = False
                    if self.on_close:
                        self.on_close(e.code, e.reason)
                    break
                
                except Exception as e:
                    logger.error("receive_error", error=str(e))
                    if self.on_error:
                        self.on_error(e)
                    # Continue receiving unless fatal
        
        except Exception as e:
            logger.error("receive_loop_fatal", error=str(e))
            if self.on_error:
                self.on_error(e)
    
    async def start(self):
        """Start WebSocket connection and receive loop"""
        while self.should_run and self.reconnect_count < self.max_reconnect_attempts:
            try:
                await self.connect()
                await self._receive_loop()
                
            except Exception as e:
                logger.error("connection_error", error=str(e), attempt=self.reconnect_count)
                if self.on_error:
                    self.on_error(e)
            
            # Reconnection logic
            if self.should_run and self.reconnect_count < self.max_reconnect_attempts:
                self.reconnect_count += 1
                logger.info(
                    "reconnecting",
                    attempt=self.reconnect_count,
                    delay=self.reconnect_delay
                )
                await asyncio.sleep(self.reconnect_delay)
            else:
                break
        
        logger.info("websocket_stopped", reconnect_attempts=self.reconnect_count)
    
    def get_stats(self) -> Dict:
        """Get connection statistics"""
        return {
            'is_connected': self.is_connected,
            'subscribed_instruments': len(self.subscribed_instruments),
            'packets_received': self.packets_received,
            'packets_parsed': self.packets_parsed,
            'packets_failed': self.packets_failed,
            'last_packet_time': self.last_packet_time,
            'reconnect_count': self.reconnect_count
        }


# Example usage for testing
if __name__ == "__main__":
    import sys
    
    def on_tick(tick: Dict):
        """Example tick handler"""
        print(f"[TICK] {tick.get('security_id')} | LTP: {tick.get('last_price')} | "
              f"Volume: {tick.get('volume_traded')}")
    
    def on_connect():
        print("[CONNECTED] Dhan WebSocket connected")
    
    def on_error(error: Exception):
        print(f"[ERROR] {error}")
    
    def on_close(code: int, reason: str):
        print(f"[CLOSED] Code: {code}, Reason: {reason}")
    
    async def main():
        # Create client
        client = DhanWebSocketClient(
            on_tick=on_tick,
            on_connect=on_connect,
            on_error=on_error,
            on_close=on_close
        )
        
        # Start connection
        connect_task = asyncio.create_task(client.start())
        
        # Wait for connection
        await asyncio.sleep(2)
        
        # Subscribe to NIFTY 50 index
        if client.is_connected:
            await client.subscribe([
                {'security_id': '13', 'exchange_segment': 0}  # NIFTY 50
            ], mode='full')
        
        # Run for 60 seconds
        await asyncio.sleep(60)
        
        # Disconnect
        await client.disconnect()
        await connect_task
        
        # Print stats
        print("\n[STATS]", client.get_stats())
    
    # Run
    asyncio.run(main())
