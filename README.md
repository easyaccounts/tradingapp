# 🚀 Trading Data Platform

Production-grade trading data platform for ingesting F&O market data from KiteConnect to TimescaleDB with real-time processing and analytics.

## 📊 Architecture

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐
│   Kite API  │─────▶│  Ingestion   │─────▶│  RabbitMQ    │
│  WebSocket  │      │   Service    │      │   (Queue)    │
└─────────────┘      └──────────────┘      └──────────────┘
                             │                      │
                             ▼                      ▼
                     ┌──────────────┐      ┌──────────────┐
                     │    Redis     │      │    Celery    │
                     │   (Cache)    │      │   Workers    │
                     └──────────────┘      └──────────────┘
                                                   │
┌─────────────┐                                    ▼
│   FastAPI   │                           ┌──────────────┐
│  (REST API) │◀─────────────────────────▶│ TimescaleDB  │
└─────────────┘                           │  (PgBouncer) │
       │                                  └──────────────┘
       ▼                                          │
┌─────────────┐                                   ▼
│   Nginx +   │                           ┌──────────────┐
│  Frontend   │                           │  Prometheus  │
└─────────────┘                           │  + Grafana   │
                                          └──────────────┘
```

## 🎯 Features

- **Real-time Data Ingestion**: KiteConnect WebSocket integration with auto-reconnect
- **High-Performance Storage**: TimescaleDB with automatic compression and retention policies
- **Scalable Architecture**: Distributed workers with RabbitMQ message queue
- **Connection Pooling**: PgBouncer for optimal database performance
- **Data Enrichment**: Market depth, order imbalance, bid-ask spread calculations
- **Production-Ready**: Comprehensive error handling, logging, and monitoring
- **Containerized**: Full Docker Compose setup for easy deployment
- **Observability**: Prometheus metrics, Grafana dashboards, and Flower for Celery monitoring

## 📋 Prerequisites

- Docker and Docker Compose
- KiteConnect API credentials ([Get them here](https://kite.trade/))
- Minimum 4GB RAM, 2 CPU cores recommended
- Linux/macOS/Windows with WSL2

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd tradingapp
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` and fill in your credentials:

```env
# Update these required values:
KITE_API_KEY=your_kite_api_key_here
KITE_API_SECRET=your_kite_api_secret_here
DB_PASSWORD=strong_password_here
RABBITMQ_PASSWORD=rabbitmq_password_here
```

### 3. Start the Platform

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### 4. Authenticate with Kite

1. Open your browser and navigate to `http://localhost`
2. Click "Connect Kite"
3. Login with your Zerodha credentials
4. Authorize the application
5. You'll be redirected to the success page

### 5. Load Instrument Master Data

```bash
# Run the instruments update script
docker-compose exec ingestion python /app/scripts/update_instruments.py
```

### 6. Monitor the Platform

Access monitoring dashboards:

- **Frontend**: http://localhost (Nginx reverse proxy)
- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Flower (Celery)**: http://localhost:5555
- **RabbitMQ Management**: http://localhost:15672 (admin/RABBITMQ_PASSWORD)
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/GRAFANA_PASSWORD)

## 📁 Project Structure

```
tradingapp/
├── docker-compose.yml          # Multi-service orchestration
├── .env.example                # Environment variables template
├── .gitignore                  # Git ignore rules
├── README.md                   # This file
│
├── services/
│   ├── api/                    # FastAPI REST API
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py
│   │   └── routes/
│   │       ├── __init__.py
│   │       ├── kite.py         # Kite authentication endpoints
│   │       └── health.py       # Health check endpoints
│   │
│   ├── frontend/               # Static HTML frontend
│   │   ├── Dockerfile
│   │   └── public/
│   │       ├── index.html      # Landing page
│   │       ├── login.html      # Kite login page
│   │       └── success.html    # Success confirmation
│   │
│   ├── ingestion/              # Real-time data ingestion
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py             # Entry point
│   │   ├── config.py           # Configuration management
│   │   ├── kite_auth.py        # Kite authentication handler
│   │   ├── kite_websocket.py   # WebSocket connection manager
│   │   ├── validator.py        # Tick data validation
│   │   ├── enricher.py         # Data enrichment logic
│   │   ├── publisher.py        # RabbitMQ publisher
│   │   └── models.py           # Pydantic data models
│   │
│   └── worker/                 # Celery workers
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── celery_app.py       # Celery configuration
│       ├── tasks.py            # Celery task definitions
│       ├── db_writer.py        # Database write operations
│       └── models.py           # SQLAlchemy models
│
├── database/
│   └── init.sql                # TimescaleDB schema initialization
│
├── config/
│   ├── nginx/
│   │   └── default.conf        # Nginx reverse proxy config
│   └── prometheus/
│       └── prometheus.yml      # Prometheus config (create manually)
│
└── scripts/
    └── update_instruments.py   # Instrument master data loader
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KITE_API_KEY` | KiteConnect API key | Required |
| `KITE_API_SECRET` | KiteConnect API secret | Required |
| `DB_USER` | PostgreSQL username | tradinguser |
| `DB_PASSWORD` | PostgreSQL password | Required |
| `DB_NAME` | Database name | tradingdb |
| `BATCH_SIZE` | Number of ticks per batch | 1000 |
| `BATCH_TIMEOUT` | Max seconds before flushing batch | 5 |
| `INSTRUMENTS` | Comma-separated instrument tokens | 260105,261897,265 |
| `LOG_LEVEL` | Logging level (DEBUG/INFO/WARNING) | INFO |

### Instrument Tokens

Common F&O instrument tokens:
- `260105` - NIFTY
- `261897` - BANKNIFTY
- `265` - FINNIFTY

Find more tokens using the `scripts/update_instruments.py` script.

## 🗄️ Database Schema

### Ticks Table

High-frequency tick data with TimescaleDB hypertable:

- **Retention**: 90 days (automatic cleanup)
- **Compression**: After 7 days (10x storage reduction)
- **Partitioning**: 1-day chunks
- **Indexes**: On instrument_token, trading_symbol, exchange

Key fields:
- Price data: `last_price`, `average_traded_price`
- Volume: `volume_traded`, `total_buy_quantity`, `total_sell_quantity`
- Market depth: 5-level bid/ask prices, quantities, and orders
- Derived metrics: `bid_ask_spread`, `mid_price`, `order_imbalance`

### Instruments Table

Master data for all tradable instruments with metadata.

## 🔍 Monitoring & Observability

### Health Checks

```bash
# Check overall system health
curl http://localhost:8000/health

# Check Kite authentication status
curl http://localhost:8000/api/kite/status
```

### Logs

```bash
# View all logs
docker-compose logs -f

# View specific service
docker-compose logs -f ingestion
docker-compose logs -f worker

# View last 100 lines
docker-compose logs --tail=100 -f
```

### Metrics

Prometheus metrics are automatically collected from:
- FastAPI application (request latency, error rates)
- Celery workers (task counts, processing time)
- RabbitMQ (queue depth, message rates)
- PostgreSQL (via pg_exporter - add manually)

## 🛠️ Troubleshooting

### Kite Authentication Failed

```bash
# Check if access token is stored in Redis
docker-compose exec redis redis-cli GET kite_access_token

# If empty, re-authenticate via the frontend
```

### WebSocket Connection Issues

```bash
# Check ingestion service logs
docker-compose logs -f ingestion

# Verify access token is valid
docker-compose exec ingestion python -c "from kite_auth import check_token_validity; print(check_token_validity())"
```

### Database Connection Errors

```bash
# Check PgBouncer status
docker-compose exec pgbouncer psql -h localhost -U tradinguser tradingdb -c "SELECT 1;"

# Check TimescaleDB
docker-compose exec timescaledb pg_isready
```

### Worker Not Processing Messages

```bash
# Check RabbitMQ queue depth
# Visit http://localhost:15672 and check 'ticks_queue'

# Check worker logs
docker-compose logs -f worker

# Restart workers
docker-compose restart worker
```

### High Memory Usage

```bash
# Check container stats
docker stats

# Reduce batch size in .env
BATCH_SIZE=500
BATCH_TIMEOUT=3

# Restart affected services
docker-compose restart worker ingestion
```

## 🔒 Production Deployment

### SSL Certificate Setup

```bash
# Generate Let's Encrypt certificate
docker-compose run --rm certbot certonly --webroot \
  --webroot-path=/var/www/certbot \
  --email your@email.com \
  --agree-tos \
  --no-eff-email \
  -d yourdomain.com
```

### Security Best Practices

1. **Change default passwords** in `.env`
2. **Restrict CORS origins** in `services/api/main.py`
3. **Enable firewall rules** (only expose 80, 443)
4. **Use secrets management** (AWS Secrets Manager, Vault)
5. **Enable audit logging** for database operations
6. **Set up backup strategy** for PostgreSQL
7. **Implement rate limiting** on API endpoints

### Performance Tuning

1. **Scale workers**: Increase `replicas` in docker-compose.yml
2. **Optimize batch size**: Test different `BATCH_SIZE` values
3. **Database tuning**: Adjust `shared_buffers`, `work_mem` in PostgreSQL
4. **Connection pooling**: Tune PgBouncer `MAX_CLIENT_CONN`
5. **Redis persistence**: Use AOF for durability vs RDB for performance

## 📈 Data Flow

1. **Authentication**: User logs in via frontend → API stores token in Redis
2. **Ingestion**: Service connects to Kite WebSocket → Receives real-time ticks
3. **Validation**: Each tick validated for data quality and outliers
4. **Enrichment**: Calculate derived metrics (spread, imbalance, mid-price)
5. **Publishing**: Send enriched tick to RabbitMQ queue
6. **Processing**: Celery workers consume from queue in batches
7. **Storage**: Bulk insert into TimescaleDB using COPY command
8. **Analysis**: Query data via API or directly from Grafana

## 🧪 Development

### Running Tests

```bash
# Install dev dependencies
pip install pytest pytest-asyncio pytest-cov

# Run tests (implement test suite)
pytest tests/

# With coverage
pytest --cov=services --cov-report=html
```

### Local Development Without Docker

```bash
# Start external services only
docker-compose up -d timescaledb redis rabbitmq

# Run API locally
cd services/api
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload

# Run ingestion locally
cd services/ingestion
python main.py
```

## 📝 License

MIT License - See LICENSE file for details

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

For issues and questions:
- Open an issue on GitHub
- Check troubleshooting section above
- Review KiteConnect documentation: https://kite.trade/docs/

---

**⚠️ Disclaimer**: This platform is for educational and research purposes. Trading involves risk. Always test thoroughly before using with real money.
