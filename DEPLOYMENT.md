# 🚀 Production Deployment Guide - Hetzner VPS

Complete guide for deploying the Trading Platform to your Hetzner VPS at **82.180.144.255** with domain **zopilot.in**.

---

## 📋 Prerequisites Checklist

- ✅ Hetzner VPS: **82.180.144.255**
- ✅ Domain: **zopilot.in** (Cloudflare managed)
- ✅ SSH access to VPS
- ✅ KiteConnect API credentials
- ✅ Root or sudo access

---

## Part 1: Cloudflare DNS Setup

### 1.1 Login to Cloudflare
Go to [Cloudflare Dashboard](https://dash.cloudflare.com) and select **zopilot.in**

### 1.2 Add DNS Records
Navigate to **DNS** → **Records** and add:

| Type | Name | Content | Proxy Status | TTL |
|------|------|---------|--------------|-----|
| A | @ | 82.180.144.255 | DNS only (gray cloud) | Auto |
| A | www | 82.180.144.255 | DNS only (gray cloud) | Auto |

**⚠️ IMPORTANT**: Set proxy status to **DNS only** (gray cloud icon). Orange cloud will break Let's Encrypt SSL verification.

### 1.3 SSL/TLS Settings
Go to **SSL/TLS** → **Overview**
- Set SSL/TLS encryption mode to: **Full (strict)** or **Flexible**
- We'll use Let's Encrypt on the VPS, so **Flexible** works for initial setup

### 1.4 Verify DNS Propagation
```bash
# Check from your local machine
nslookup zopilot.in
ping zopilot.in

# Should return: 82.180.144.255
```

Wait 5-10 minutes for DNS to propagate globally.

---

## Part 2: VPS Server Setup

### 2.1 Connect to VPS
```bash
ssh root@82.180.144.255
```

### 2.2 Update System
```bash
apt update && apt upgrade -y
```

### 2.3 Install Docker & Docker Compose
```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Install Docker Compose
apt install docker-compose -y

# Verify installation
docker --version
docker-compose --version
```

### 2.4 Install Git
```bash
apt install git -y
```

### 2.5 Configure Firewall
```bash
# Install UFW if not present
apt install ufw -y

# Allow SSH (IMPORTANT - do this first!)
ufw allow 22/tcp

# Allow HTTP & HTTPS
ufw allow 80/tcp
ufw allow 443/tcp

# Enable firewall
ufw --force enable

# Check status
ufw status
```

### 2.6 Create Application Directory
```bash
cd /opt
```

---

## Part 3: Deploy Application

### 3.1 Clone Repository
```bash
cd /opt
git clone <your-git-repository-url> tradingapp
cd tradingapp
```

### 3.2 Create Production Environment File
```bash
cp .env.example .env
nano .env
```

Update these values in `.env`:
```env
# Database
DB_PASSWORD=<generate-strong-password>

# Redis
REDIS_PASSWORD=<generate-strong-password>

# RabbitMQ
RABBITMQ_PASSWORD=<generate-strong-password>

# KiteConnect API
KITE_API_KEY=your_kite_api_key_here
KITE_API_SECRET=your_kite_api_secret_here

# Domain
DOMAIN=zopilot.in

# Environment
ENVIRONMENT=production

# CORS
ALLOWED_ORIGINS=https://zopilot.in,https://www.zopilot.in

# Grafana
GRAFANA_PASSWORD=<generate-strong-password>
```

**Save and exit**: `Ctrl+X`, then `Y`, then `Enter`

### 3.3 Generate Strong Passwords
```bash
# Generate random passwords
openssl rand -base64 32
```
Run this 4 times for DB, Redis, RabbitMQ, and Grafana passwords.

### 3.4 Update Nginx Config with Domain
```bash
# Replace ${DOMAIN} placeholder in nginx config
sed -i 's/${DOMAIN}/zopilot.in/g' config/nginx/nginx.prod.conf
```

---

## Part 4: SSL Certificate Setup

### 4.1 Initial HTTP Setup (for SSL verification)
```bash
# Start only nginx and certbot initially
docker-compose up -d nginx certbot
```

### 4.2 Generate Let's Encrypt Certificate
```bash
docker-compose run --rm certbot certonly --webroot \
  --webroot-path=/var/www/certbot \
  --email your-email@example.com \
  --agree-tos \
  --no-eff-email \
  -d zopilot.in \
  -d www.zopilot.in
```

Replace `your-email@example.com` with your actual email.

### 4.3 Verify Certificate
```bash
docker-compose exec certbot ls -la /etc/letsencrypt/live/zopilot.in/
```

You should see:
- `fullchain.pem`
- `privkey.pem`
- `chain.pem`

---

## Part 5: Launch Full Application

### 5.1 Start All Services
```bash
# Use production compose configuration
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### 5.2 Verify Services Are Running
```bash
docker-compose ps
```

All services should show "Up" status.

### 5.3 Check Logs
```bash
# View all logs
docker-compose logs -f

# View specific service
docker-compose logs -f api
docker-compose logs -f ingestion
docker-compose logs -f worker
```

Press `Ctrl+C` to exit logs.

---

## Part 6: Initial Configuration

### 6.1 Access the Application
Open browser and go to: **https://zopilot.in**

### 6.2 Authenticate with Kite
1. Click "Connect Kite"
2. Login with your Zerodha credentials
3. Authorize the application
4. You'll be redirected back to success page

### 6.3 Load Instrument Data
```bash
docker-compose exec ingestion python /app/scripts/update_instruments.py
```

### 6.4 Verify Data Ingestion
Check logs to ensure WebSocket is connected:
```bash
docker-compose logs -f ingestion
```

You should see:
```
websocket_connected
ticks_received
```

---

## Part 7: Access Monitoring Dashboards

All dashboards are accessible via HTTPS:

- **Main App**: https://zopilot.in
- **API Docs**: https://zopilot.in/api/docs
- **Grafana**: https://zopilot.in/grafana (admin / your_grafana_password)
- **Flower (Celery)**: https://zopilot.in/flower
- **RabbitMQ**: https://zopilot.in/rabbitmq (admin / your_rabbitmq_password)

---

## Part 8: Post-Deployment

### 8.1 Enable Auto-Restart
```bash
# Add to crontab for auto-start on reboot
(crontab -l 2>/dev/null; echo "@reboot cd /opt/tradingapp && docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d") | crontab -
```

### 8.2 Setup Log Rotation
```bash
cat > /etc/docker/daemon.json <<EOF
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
EOF

systemctl restart docker
```

### 8.3 Setup Database Backups
```bash
# Create backup script
mkdir -p /opt/backups
cat > /opt/backups/backup-db.sh <<'EOF'
#!/bin/bash
BACKUP_DIR="/opt/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
docker-compose -f /opt/tradingapp/docker-compose.yml exec -T timescaledb \
  pg_dump -U tradinguser tradingdb | gzip > "$BACKUP_DIR/tradingdb_$TIMESTAMP.sql.gz"

# Keep only last 7 days of backups
find "$BACKUP_DIR" -name "tradingdb_*.sql.gz" -mtime +7 -delete
EOF

chmod +x /opt/backups/backup-db.sh

# Add to crontab (daily at 2 AM)
(crontab -l 2>/dev/null; echo "0 2 * * * /opt/backups/backup-db.sh") | crontab -
```

### 8.4 Setup SSL Auto-Renewal
SSL certificates auto-renew via the certbot container. Verify renewal works:
```bash
docker-compose run --rm certbot renew --dry-run
```

---

## 🔧 Common Operations

### View Logs
```bash
docker-compose logs -f [service_name]
```

### Restart Specific Service
```bash
docker-compose restart [service_name]
```

### Update Application (Git Pull)
```bash
cd /opt/tradingapp
git pull
docker-compose -f docker-compose.yml -f docker-compose.prod.yml down
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

### Check Resource Usage
```bash
docker stats
```

### Database Access
```bash
docker-compose exec timescaledb psql -U tradinguser -d tradingdb
```

---

## 🐛 Troubleshooting

### Issue: SSL Certificate Error
**Solution**: 
1. Ensure DNS is pointing to 82.180.144.255
2. Ensure Cloudflare proxy is OFF (gray cloud)
3. Re-run certbot command from Part 4.2

### Issue: Can't Access Application
**Solution**:
```bash
# Check if services are running
docker-compose ps

# Check nginx logs
docker-compose logs nginx

# Check firewall
ufw status
```

### Issue: Ingestion Not Working
**Solution**:
```bash
# Check if access token exists
docker-compose exec api curl http://localhost:8000/api/kite/status

# Re-authenticate via web interface
# Open: https://zopilot.in
```

### Issue: Database Connection Error
**Solution**:
```bash
# Check database is running
docker-compose exec timescaledb pg_isready

# Restart database services
docker-compose restart timescaledb pgbouncer
```

---

## 📊 Performance Tuning

### For High-Frequency Data (1000+ ticks/sec)

1. **Increase Worker Replicas**:
   Edit `docker-compose.prod.yml`:
   ```yaml
   worker:
     deploy:
       replicas: 5  # Increase from 3
   ```

2. **Optimize Batch Settings** in `.env`:
   ```env
   BATCH_SIZE=2000
   BATCH_TIMEOUT=3
   ```

3. **Increase Database Connections**:
   Edit docker-compose.yml:
   ```yaml
   pgbouncer:
     environment:
       DEFAULT_POOL_SIZE: 50  # Increase from 25
   ```

---

## 🔒 Security Hardening

### 1. Restrict Monitoring Dashboards
Edit `config/nginx/nginx.prod.conf` and uncomment authentication sections for:
- Grafana
- Flower
- RabbitMQ

### 2. Setup HTTP Basic Auth
```bash
# Install htpasswd
apt install apache2-utils -y

# Create password file
htpasswd -c /opt/tradingapp/config/nginx/.htpasswd admin

# Enter password when prompted
```

### 3. Enable Docker Security
```bash
# Run Docker rootless (advanced)
# Follow: https://docs.docker.com/engine/security/rootless/
```

---

## 📞 Support

If you encounter issues:
1. Check logs: `docker-compose logs -f`
2. Verify DNS: `nslookup zopilot.in`
3. Check firewall: `ufw status`
4. Test SSL: `curl -I https://zopilot.in`

---

**✅ Deployment Complete!**

Your trading platform is now live at **https://zopilot.in**
