"""
RabbitMQ Publisher
Publishes enriched tick data to RabbitMQ queue for worker consumption
"""

import json
import time
import pika
import structlog
from typing import Dict, Any

logger = structlog.get_logger()


class RabbitMQPublisher:
    """
    RabbitMQ publisher with connection management and retry logic
    """
    
    QUEUE_NAME = "ticks_queue"
    EXCHANGE_NAME = ""  # Use default exchange
    MAX_RETRIES = 5
    RETRY_DELAY = 5  # seconds
    
    def __init__(self, rabbitmq_url: str):
        """
        Initialize RabbitMQ publisher
        
        Args:
            rabbitmq_url: RabbitMQ connection URL (amqp://...)
        """
        self.rabbitmq_url = rabbitmq_url
        self.connection = None
        self.channel = None
        self._connect()
    
    def _connect(self):
        """Establish connection to RabbitMQ with retry logic"""
        for attempt in range(self.MAX_RETRIES):
            try:
                logger.info(
                    "rabbitmq_connecting",
                    attempt=attempt + 1,
                    max_retries=self.MAX_RETRIES
                )
                
                # Parse connection parameters
                parameters = pika.URLParameters(self.rabbitmq_url)
                parameters.heartbeat = 600
                parameters.blocked_connection_timeout = 300
                
                # Create connection
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Declare queue (idempotent - creates if doesn't exist)
                self.channel.queue_declare(
                    queue=self.QUEUE_NAME,
                    durable=True,  # Survive broker restart
                    arguments={
                        'x-max-length': 1000000,  # Max 1M messages
                        'x-message-ttl': 86400000  # 24 hours TTL
                    }
                )
                
                logger.info(
                    "rabbitmq_connected",
                    queue=self.QUEUE_NAME,
                    durable=True
                )
                
                return
            
            except Exception as e:
                logger.error(
                    "rabbitmq_connection_failed",
                    attempt=attempt + 1,
                    error=str(e)
                )
                
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))  # Exponential backoff
                else:
                    raise Exception(f"Failed to connect to RabbitMQ after {self.MAX_RETRIES} attempts: {e}")
    
    def publish(self, message: Dict[str, Any]) -> bool:
        """
        Publish message to RabbitMQ queue
        
        Args:
            message: Dictionary to publish (will be JSON serialized)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check connection
            if not self.connection or self.connection.is_closed:
                logger.warning("rabbitmq_connection_lost", action="reconnecting")
                self._connect()
            
            # Serialize message to JSON
            body = json.dumps(message)
            
            # Publish with persistence
            self.channel.basic_publish(
                exchange=self.EXCHANGE_NAME,
                routing_key=self.QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json',
                    timestamp=int(time.time())
                )
            )
            
            return True
        
        except Exception as e:
            logger.error(
                "publish_failed",
                error=str(e),
                message_keys=list(message.keys()) if message else None
            )
            
            # Try to reconnect
            try:
                self._connect()
            except:
                pass
            
            return False
    
    def publish_batch(self, messages: list) -> int:
        """
        Publish multiple messages in batch
        
        Args:
            messages: List of dictionaries to publish
        
        Returns:
            int: Number of successfully published messages
        """
        success_count = 0
        
        for message in messages:
            if self.publish(message):
                success_count += 1
        
        logger.info(
            "batch_published",
            total=len(messages),
            successful=success_count,
            failed=len(messages) - success_count
        )
        
        return success_count
    
    def get_queue_depth(self) -> int:
        """
        Get current queue depth (number of messages waiting)
        
        Returns:
            int: Number of messages in queue
        """
        try:
            if not self.channel:
                return -1
            
            method = self.channel.queue_declare(
                queue=self.QUEUE_NAME,
                passive=True  # Don't create, just check
            )
            
            return method.method.message_count
        
        except Exception as e:
            logger.error("queue_depth_check_failed", error=str(e))
            return -1
    
    def close(self):
        """Close RabbitMQ connection gracefully"""
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
                logger.info("rabbitmq_channel_closed")
            
            if self.connection and self.connection.is_open:
                self.connection.close()
                logger.info("rabbitmq_connection_closed")
        
        except Exception as e:
            logger.error("rabbitmq_close_error", error=str(e))
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
