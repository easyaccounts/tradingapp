#!/bin/bash
# Setup Let's Encrypt SSL certificates for zopilot.in

DOMAIN="zopilot.in"
EMAIL="your-email@example.com"  # UPDATE THIS

echo "Setting up Let's Encrypt for $DOMAIN"

# Stop nginx temporarily
cd /opt/tradingapp
docker-compose stop nginx

# Install certbot
apt-get update
apt-get install -y certbot

# Get certificate
certbot certonly --standalone \
  -d $DOMAIN \
  -d www.$DOMAIN \
  --email $EMAIL \
  --agree-tos \
  --non-interactive \
  --preferred-challenges http

# Copy certificates to nginx ssl directory
mkdir -p /opt/tradingapp/ssl
cp /etc/letsencrypt/live/$DOMAIN/fullchain.pem /opt/tradingapp/ssl/cert.pem
cp /etc/letsencrypt/live/$DOMAIN/privkey.pem /opt/tradingapp/ssl/key.pem

# Restart nginx
docker-compose start nginx

echo "✓ Let's Encrypt certificates installed!"
echo "Now set Cloudflare SSL/TLS mode to 'Full (strict)'"

# Setup auto-renewal
echo "0 0 * * 0 certbot renew --quiet && cp /etc/letsencrypt/live/$DOMAIN/fullchain.pem /opt/tradingapp/ssl/cert.pem && cp /etc/letsencrypt/live/$DOMAIN/privkey.pem /opt/tradingapp/ssl/key.pem && docker-compose restart nginx" | crontab -
