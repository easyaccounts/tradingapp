#!/bin/bash
# Update .env file from Docker service names to localhost
# Run this on VPS: bash update-env-for-pm2.sh

set -e

if [ ! -f ".env" ]; then
    echo "Error: .env file not found"
    exit 1
fi

echo "Creating backup: .env.docker.backup"
cp .env .env.docker.backup

echo "Updating .env for PM2 (localhost connections)..."

# Update host names
sed -i 's/DB_HOST=pgbouncer/DB_HOST=localhost/' .env
sed -i 's/DB_HOST=timescaledb/DB_HOST=localhost/' .env
sed -i 's/REDIS_HOST=redis/REDIS_HOST=localhost/' .env
sed -i 's/RABBITMQ_HOST=rabbitmq/RABBITMQ_HOST=localhost/' .env

# Update connection strings
sed -i 's/@pgbouncer:/@localhost:6432\//' .env
sed -i 's/@timescaledb:/@localhost:5432\//' .env
sed -i 's/@redis:/@localhost:/' .env
sed -i 's/@rabbitmq:/@localhost:/' .env

# Show changes
echo ""
echo "Changes made:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
diff .env.docker.backup .env || true
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo "✓ .env updated for PM2"
echo ""
echo "Verify these settings:"
grep "DB_HOST=" .env
grep "REDIS_HOST=" .env
grep "RABBITMQ_HOST=" .env
grep "DATABASE_URL=" .env
grep "REDIS_URL=" .env
grep "RABBITMQ_URL=" .env

echo ""
echo "Backup saved at: .env.docker.backup"
echo "To revert: cp .env.docker.backup .env"
