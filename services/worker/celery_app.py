"""
Celery Application Configuration
"""

import os
from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Celery configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672")

# Initialize Celery app
app = Celery(
    'tradingapp',
    broker=RABBITMQ_URL,
    backend=REDIS_URL,
    include=['tasks']
)

# Configure Celery
app.conf.update(
    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    
    # Timezone
    timezone='Asia/Kolkata',
    enable_utc=True,
    
    # Results
    result_expires=3600,  # 1 hour
    result_backend_transport_options={
        'master_name': 'mymaster',
    },
    
    # Task routing
    task_routes={
        'tasks.process_tick': {'queue': 'ticks'},
        'tasks.check_token_expiry': {'queue': 'maintenance'},
    },
    
    # Worker configuration
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=1000,
    worker_disable_rate_limits=True,
    
    # Task execution
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    
    # Retry configuration
    task_default_retry_delay=5,  # 5 seconds
    task_max_retries=3,
    
    # Monitoring
    worker_send_task_events=True,
    task_send_sent_event=True,
    
    # Beat schedule (periodic tasks)
    beat_schedule={
        'check-token-expiry-daily': {
            'task': 'tasks.check_token_expiry',
            'schedule': crontab(hour=7, minute=0),  # Daily at 7 AM IST
        },
        'cleanup-old-results': {
            'task': 'tasks.cleanup_old_results',
            'schedule': crontab(hour=2, minute=0),  # Daily at 2 AM IST
        },
    },
)

# Logging configuration
app.conf.update(
    worker_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
    worker_task_log_format='[%(asctime)s: %(levelname)s/%(processName)s] [%(task_name)s(%(task_id)s)] %(message)s',
)

if __name__ == '__main__':
    app.start()
