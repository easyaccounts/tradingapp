// Helper to load .env file and return as object
const fs = require('fs');
const path = require('path');

function loadEnv() {
  const envPath = path.join(__dirname, '.env');
  const envFile = fs.readFileSync(envPath, 'utf8');
  const env = {};
  
  envFile.split('\n').forEach(line => {
    line = line.trim();
    if (!line || line.startsWith('#')) return;
    const [key, ...valueParts] = line.split('=');
    if (key && valueParts.length) {
      env[key.trim()] = valueParts.join('=').trim();
    }
  });
  
  return env;
}

const envVars = loadEnv();

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
      env: {
        ...envVars,
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
      env: {
        ...envVars,
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
      env: {
        ...envVars,
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
      env: {
        ...envVars,
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
      env: {
        ...envVars,
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
      env: {
        ...envVars,
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
      env: {
        ...envVars,
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/opt/tradingapp'
      },
      error_file: '/opt/tradingapp/logs/signal-generator-error.log',
      out_file: '/opt/tradingapp/logs/signal-generator-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      kill_timeout: 5000
    },
    {
      name: 'ssl-monitor',
      script: 'monitor-ssl.py',
      interpreter: '/opt/tradingapp/venv/bin/python',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: true,
      watch: false,  // No need to watch - just monitors
      max_memory_restart: '100M',
      env: {
        PYTHONUNBUFFERED: '1'
      },
      error_file: '/opt/tradingapp/logs/ssl-monitor-error.log',
      out_file: '/opt/tradingapp/logs/ssl-monitor-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true
    },
    {
      name: 'depth-insights-slack',
      script: 'depth_insights_slack.py',
      interpreter: '/opt/tradingapp/venv/bin/python',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: false,  // Cron mode - don't restart on exit
      cron_restart: '*/5 9-15 * * 1-5',  // Every 5 mins, 9 AM-3 PM IST, Mon-Fri only
      watch: false,
      max_memory_restart: '200M',
      env: {
        ...envVars,
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: '/opt/tradingapp'
      },
      error_file: '/opt/tradingapp/logs/depth-insights-slack-error.log',
      out_file: '/opt/tradingapp/logs/depth-insights-slack-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true
    },
    {
      name: 'market-start',
      script: 'market_hours_control.py',
      args: 'start',
      interpreter: '/usr/bin/python3',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: false,
      cron_restart: '30 3 * * 1-5',  // 9:00 AM IST = 03:30 UTC, Mon-Fri
      watch: false,
      env: {
        PATH: '/usr/local/bin:/usr/bin:/bin'
      },
      error_file: '/opt/tradingapp/logs/market-start-error.log',
      out_file: '/opt/tradingapp/logs/market-start-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true
    },
    {
      name: 'market-stop',
      script: 'market_hours_control.py',
      args: 'stop',
      interpreter: '/usr/bin/python3',
      cwd: '/opt/tradingapp',
      instances: 1,
      autorestart: false,
      cron_restart: '30 11 * * 1-5',  // 4:00 PM IST = 11:30 CET (winter), Mon-Fri
      watch: false,
      env: {
        PATH: '/usr/local/bin:/usr/bin:/bin'
      },
      error_file: '/opt/tradingapp/logs/market-stop-error.log',
      out_file: '/opt/tradingapp/logs/market-stop-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true
    }
  ]
};
