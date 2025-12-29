#!/bin/bash
# Setup tokendata directory for OAuth tokens
# This creates the host directory that will be bind-mounted into Docker containers

echo "Setting up tokendata directory..."

# Create data directory on VPS host
mkdir -p /opt/tradingapp/data
chmod 755 /opt/tradingapp/data

echo "✓ Directory /opt/tradingapp/data created"
echo "✓ This directory will be accessible from:"
echo "  - Docker containers at: /app/data"
echo "  - VPS host scripts at: /opt/tradingapp/data"
echo ""
echo "Next steps:"
echo "1. Ensure DHAN_API_KEY, DHAN_API_SECRET, DHAN_CLIENT_ID are in .env"
echo "2. Run: docker-compose up -d"
echo "3. Complete OAuth flow via: https://yourdomain.com"
echo "4. Token will be stored at: /opt/tradingapp/data/dhan_token.json"
