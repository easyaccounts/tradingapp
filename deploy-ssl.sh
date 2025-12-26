#!/bin/bash

# Trading Platform - SSL Certificate Setup
# Run this after setup-vps.sh and configuring .env

set -e

echo "=================================================="
echo "Trading Platform - SSL Certificate Setup"
echo "Domain: zopilot.in"
echo "=================================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${RED}Error: .env file not found${NC}"
    echo "Please copy .env.example to .env and configure it first"
    exit 1
fi

# Load domain from .env
source .env

if [ -z "$DOMAIN" ]; then
    echo -e "${RED}Error: DOMAIN not set in .env${NC}"
    exit 1
fi

echo -e "${YELLOW}Domain: $DOMAIN${NC}"
echo ""

# Ask for email
read -p "Enter your email for Let's Encrypt notifications: " EMAIL

if [ -z "$EMAIL" ]; then
    echo -e "${RED}Error: Email is required${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}[1/5] Replacing domain placeholder in nginx config...${NC}"
sed -i "s/\${DOMAIN}/$DOMAIN/g" config/nginx/nginx.prod.conf
echo -e "${GREEN}✓ Nginx config updated${NC}"

echo ""
echo -e "${YELLOW}[2/5] Starting nginx and certbot...${NC}"
docker-compose up -d nginx certbot
sleep 5
echo -e "${GREEN}✓ Services started${NC}"

echo ""
echo -e "${YELLOW}[3/5] Generating Let's Encrypt SSL certificate...${NC}"
echo "This may take a minute..."
docker-compose run --rm certbot certonly --webroot \
  --webroot-path=/var/www/certbot \
  --email "$EMAIL" \
  --agree-tos \
  --no-eff-email \
  -d "$DOMAIN" \
  -d "www.$DOMAIN" || {
    echo -e "${RED}Failed to generate SSL certificate${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "1. Ensure DNS is pointing to this server"
    echo "2. Check: nslookup $DOMAIN"
    echo "3. Ensure Cloudflare proxy is OFF (gray cloud)"
    echo "4. Wait 5-10 minutes for DNS propagation"
    exit 1
}

echo -e "${GREEN}✓ SSL certificate generated${NC}"

echo ""
echo -e "${YELLOW}[4/5] Verifying certificate...${NC}"
if docker-compose exec certbot ls /etc/letsencrypt/live/$DOMAIN/fullchain.pem &>/dev/null; then
    echo -e "${GREEN}✓ Certificate verified${NC}"
else
    echo -e "${RED}Certificate verification failed${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}[5/5] Starting all services with SSL...${NC}"
docker-compose -f docker-compose.yml -f docker-compose.prod.yml down
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
echo -e "${GREEN}✓ All services started${NC}"

echo ""
echo -e "${GREEN}=================================================="
echo "✓ SSL Setup Complete!"
echo "==================================================${NC}"
echo ""
echo "Your application is now running at:"
echo -e "${GREEN}https://$DOMAIN${NC}"
echo ""
echo "Next steps:"
echo "1. Open https://$DOMAIN in your browser"
echo "2. Authenticate with Kite"
echo "3. Load instruments: docker-compose exec ingestion python /app/scripts/update_instruments.py"
echo ""
echo "Check logs: docker-compose logs -f"
