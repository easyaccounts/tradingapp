#!/bin/bash

# Dhan OAuth Deployment Script
# This script deploys the Dhan OAuth integration to the VPS

set -e  # Exit on any error

echo "====================================="
echo "Dhan OAuth Integration Deployment"
echo "====================================="
echo ""

# Configuration
VPS_HOST="root@82.180.144.255"
VPS_PATH="/opt/tradingapp"
DOMAIN="zopilot.in"

echo "Step 1: Pushing code to git repository..."
git add .
git commit -m "Add Dhan OAuth integration - Web-based authentication" || echo "No changes to commit"
git push

echo ""
echo "Step 2: Connecting to VPS and pulling changes..."
ssh $VPS_HOST << 'ENDSSH'
cd /opt/tradingapp
echo "Current directory: $(pwd)"
git pull
echo "Git pull complete"
ENDSSH

echo ""
echo "Step 3: Checking environment variables..."
echo "Please ensure the following variables are set in .env on VPS:"
echo "  - DHAN_API_KEY"
echo "  - DHAN_API_SECRET"
echo "  - DHAN_CLIENT_ID"
echo "  - DHAN_REDIRECT_URL=https://zopilot.in/api/dhan/callback"
echo ""
read -p "Press Enter to continue or Ctrl+C to abort..."

echo ""
echo "Step 4: Rebuilding and restarting services..."
ssh $VPS_HOST << 'ENDSSH'
cd /opt/tradingapp
echo "Building API service..."
docker-compose build api

echo "Restarting services..."
docker-compose restart api frontend

echo "Waiting for services to stabilize..."
sleep 5

echo "Checking service status..."
docker-compose ps api frontend
ENDSSH

echo ""
echo "Step 5: Testing OAuth endpoints..."
echo "Testing /api/dhan/login-url endpoint..."
RESPONSE=$(curl -s "https://$DOMAIN/api/dhan/login-url" || echo "ERROR")
if echo "$RESPONSE" | grep -q "login_url"; then
    echo "✓ Login URL endpoint working"
else
    echo "✗ Login URL endpoint failed"
    echo "Response: $RESPONSE"
fi

echo ""
echo "Step 6: Verifying token file directory..."
ssh $VPS_HOST << 'ENDSSH'
cd /opt/tradingapp
echo "Checking /app/data directory in API container..."
docker exec api ls -lah /app/data/ || docker exec api mkdir -p /app/data
docker exec api chmod 755 /app/data
echo "Directory ready for token storage"
ENDSSH

echo ""
echo "====================================="
echo "Deployment Complete!"
echo "====================================="
echo ""
echo "Next steps:"
echo "1. Visit https://$DOMAIN/"
echo "2. Click 'Connect Dhan' button"
echo "3. Login with Dhan credentials"
echo "4. Verify redirect to success page"
echo "5. Check token stored:"
echo "   ssh $VPS_HOST 'docker exec api cat /app/data/dhan_token.json'"
echo ""
echo "Monitoring commands:"
echo "  API logs:        ssh $VPS_HOST 'docker logs api --tail 100 -f'"
echo "  Frontend logs:   ssh $VPS_HOST 'docker logs frontend --tail 100 -f'"
echo "  Depth collector: ssh $VPS_HOST 'docker logs depth-collector --tail 100 -f'"
echo "  Redis keys:      ssh $VPS_HOST 'docker exec api redis-cli -u \$REDIS_URL KEYS \"dhan_*\"'"
echo ""
echo "Troubleshooting:"
echo "  If OAuth fails, check environment variables:"
echo "  ssh $VPS_HOST 'docker exec api env | grep DHAN'"
echo ""
