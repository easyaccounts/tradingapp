# Quick Reference - Trading Platform Commands

## Initial Deployment
```bash
# 1. Setup VPS (run once)
sudo bash setup-vps.sh

# 2. Clone repository
cd /opt
git clone <your-repo-url> tradingapp
cd tradingapp

# 3. Configure environment
cp .env.example .env
nano .env  # Update all credentials

# 4. Deploy with SSL
bash deploy-ssl.sh
```

## Daily Operations

### View Logs
```bash
docker-compose logs -f                    # All services
docker-compose logs -f api                # API only
docker-compose logs -f ingestion          # Ingestion only
docker-compose logs -f worker             # Workers only
```

### Restart Services
```bash
docker-compose restart [service]          # Restart specific service
docker-compose restart                    # Restart all
```

### Stop/Start Application
```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml down
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### Update Code
```bash
git pull
docker-compose -f docker-compose.yml -f docker-compose.prod.yml down
docker-compose -f docker-compose.yml -f docker-compose.prod.yml build
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### Database Operations
```bash
# Access database
docker-compose exec timescaledb psql -U tradinguser -d tradingdb

# Backup database
docker-compose exec timescaledb pg_dump -U tradinguser tradingdb | gzip > backup_$(date +%Y%m%d).sql.gz

# Restore database
gunzip -c backup.sql.gz | docker-compose exec -T timescaledb psql -U tradinguser -d tradingdb
```

### Check Status
```bash
docker-compose ps                         # Service status
docker stats                              # Resource usage
ufw status                                # Firewall status
```

### SSL Certificate
```bash
# Test renewal (dry run)
docker-compose run --rm certbot renew --dry-run

# Force renewal
docker-compose run --rm certbot renew --force-renewal
docker-compose restart nginx
```

## Monitoring URLs

- Main App: https://zopilot.in
- API Docs: https://zopilot.in/api/docs
- Grafana: https://zopilot.in/grafana
- Flower: https://zopilot.in/flower
- RabbitMQ: https://zopilot.in/rabbitmq

## Troubleshooting

### Check DNS
```bash
nslookup zopilot.in
ping zopilot.in
```

### Check Ports
```bash
netstat -tulpn | grep -E ':(80|443) '
```

### Check Certificates
```bash
docker-compose exec certbot ls -la /etc/letsencrypt/live/zopilot.in/
openssl s_client -connect zopilot.in:443 -servername zopilot.in
```

### Re-authenticate Kite
```bash
# 1. Clear old token
docker-compose exec api redis-cli -h redis -a $REDIS_PASSWORD del kite_access_token

# 2. Visit https://zopilot.in and login
```

### Clean Restart
```bash
docker-compose down
docker system prune -f
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```
