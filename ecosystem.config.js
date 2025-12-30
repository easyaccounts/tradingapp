module.exports = {
  apps: [
    {
      name: 'api',
      script: 'services/api/main.py',
      interpreter: '/opt/tradingapp/venv/bin/python',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: true,
      watch: true,
      watch_delay: 1000,
      ignore_watch: [
        'node_modules',
        'data',
        'logs',
        '*.log',
        '*.csv',
        '.git'
      ],
      max_memory_restart: '500M',
      env_file: '/opt/tradingapp/.env',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/opt/tradingapp'
      },
      error_file: '/opt/tradingapp/logs/api-error.log',
      out_file: '/opt/tradingapp/logs/api-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      kill_timeout: 5000
    },
    {
      name: 'ingestion',
      script: 'services/ingestion/main.py',
      interpreter: '/opt/tradingapp/venv/bin/python',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: true,
      watch: true,
      watch_delay: 1000,
      ignore_watch: [
        'node_modules',
        'data',
        'logs',
        '*.log',
        '*.csv',
        '.git'
      ],
      max_memory_restart: '500M',
      max_restarts: 5,
      min_uptime: '10s',
      restart_delay: 4000,
      env_file: '/opt/tradingapp/.env',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/opt/tradingapp'
      },
      error_file: '/opt/tradingapp/logs/ingestion-error.log',
      out_file: '/opt/tradingapp/logs/ingestion-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      kill_timeout: 5000
    },
    {
      name: 'worker-1',
      script: 'services/worker/consumer.py',
      interpreter: '/opt/tradingapp/venv/bin/python',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: true,
      watch: true,
      watch_delay: 1000,
      ignore_watch: [
        'node_modules',
        'data',
        'logs',
        '*.log',
        '*.csv',
        '.git'
      ],
      max_memory_restart: '1G',
      env_file: '/opt/tradingapp/.env',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/opt/tradingapp',
        WORKER_ID: '1'
      },
      error_file: '/opt/tradingapp/logs/worker-1-error.log',
      out_file: '/opt/tradingapp/logs/worker-1-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      kill_timeout: 10000
    },
    {
      name: 'worker-2',
      script: 'services/worker/consumer.py',
      interpreter: '/opt/tradingapp/venv/bin/python',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: true,
      watch: true,
      watch_delay: 1000,
      ignore_watch: [
        'node_modules',
        'data',
        'logs',
        '*.log',
        '*.csv',
        '.git'
      ],
      max_memory_restart: '1G',
      env_file: '/opt/tradingapp/.env',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/opt/tradingapp',
        WORKER_ID: '2'
      },
      error_file: '/opt/tradingapp/logs/worker-2-error.log',
      out_file: '/opt/tradingapp/logs/worker-2-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      kill_timeout: 10000
    },
    {
      name: 'worker-3',
      script: 'services/worker/consumer.py',
      interpreter: '/opt/tradingapp/venv/bin/python',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: true,
      watch: true,
      watch_delay: 1000,
      ignore_watch: [
        'node_modules',
        'data',
        'logs',
        '*.log',
        '*.csv',
        '.git'
      ],
      max_memory_restart: '1G',
      env_file: '/opt/tradingapp/.env',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/opt/tradingapp',
        WORKER_ID: '3'
      },
      error_file: '/opt/tradingapp/logs/worker-3-error.log',
      out_file: '/opt/tradingapp/logs/worker-3-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      kill_timeout: 10000
    },
    {
      name: 'depth-collector',
      script: 'services/depth_collector/dhan_200depth_websocket.py',
      interpreter: '/opt/tradingapp/venv/bin/python',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: true,
      watch: true,
      watch_delay: 1000,
      ignore_watch: [
        'node_modules',
        'data',
        'logs',
        '*.log',
        '*.csv',
        '.git'
      ],
      max_memory_restart: '500M',
      max_restarts: 5,
      min_uptime: '10s',
      restart_delay: 4000,
      env_file: '/opt/tradingapp/.env',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/opt/tradingapp'
      },
      error_file: '/opt/tradingapp/logs/depth-collector-error.log',
      out_file: '/opt/tradingapp/logs/depth-collector-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      kill_timeout: 5000
    },
    {
      name: 'signal-generator',
      script: 'services/signal_generator/main.py',
      interpreter: '/opt/tradingapp/venv/bin/python',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: true,
      watch: true,
      watch_delay: 1000,
      ignore_watch: [
        'node_modules',
        'data',
        'logs',
        '*.log',
        '*.csv',
        '.git'
      ],
      max_memory_restart: '500M',
      env_file: '/opt/tradingapp/.env',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/opt/tradingapp'
      },
      error_file: '/opt/tradingapp/logs/signal-generator-error.log',
      out_file: '/opt/tradingapp/logs/signal-generator-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      kill_timeout: 5000
    }
  ]
};
