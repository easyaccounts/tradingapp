#!/bin/bash
# PM2 Migration Deployment Script
# Run this on VPS at /opt/tradingapp

set -e  # Exit on error

echo "=== PM2 Migration Deployment ==="
echo "Step-by-step migration from Docker to PM2"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Phase 1: Preparation
echo -e "${YELLOW}Phase 1: Preparation${NC}"
echo "1.1 Creating Python virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ venv created${NC}"
else
    echo -e "${GREEN}✓ venv already exists${NC}"
fi

echo "1.2 Installing Python dependencies..."
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.pm2.txt
echo -e "${GREEN}✓ Dependencies installed${NC}"

echo "1.3 Creating logs directory..."
mkdir -p logs
echo -e "${GREEN}✓ logs/ created${NC}"

echo "1.4 Checking .env file..."
if [ ! -f ".env" ]; then
    echo -e "${RED}✗ .env file not found!${NC}"
    echo "Please create .env file with localhost connections:"
    echo "  DB_HOST=localhost"
    echo "  REDIS_HOST=localhost"
    echo "  RABBITMQ_HOST=localhost"
    exit 1
fi
echo -e "${GREEN}✓ .env exists${NC}"

# Check critical env vars
DB_HOST=$(grep "^DB_HOST=" .env | cut -d'=' -f2)
if [[ "$DB_HOST" == "pgbouncer" ]] || [[ "$DB_HOST" == "timescaledb" ]]; then
    echo -e "${RED}✗ DB_HOST still set to Docker service name: $DB_HOST${NC}"
    echo "Update .env: DB_HOST=localhost"
    exit 1
fi
echo -e "${GREEN}✓ DB_HOST=$DB_HOST (correct for PM2)${NC}"

# Phase 2: Install PM2
echo ""
echo -e "${YELLOW}Phase 2: Install PM2${NC}"
if ! command -v pm2 &> /dev/null; then
    echo "2.1 Installing PM2..."
    npm install -g pm2
    echo -e "${GREEN}✓ PM2 installed${NC}"
else
    echo -e "${GREEN}✓ PM2 already installed: $(pm2 --version)${NC}"
fi

# Phase 3: Test Database Connections
echo ""
echo -e "${YELLOW}Phase 3: Testing Database Connections${NC}"

# Load DB credentials for testing
DB_PASSWORD=$(grep "^DB_PASSWORD=" .env | cut -d'=' -f2)
DB_USER=$(grep "^DB_USER=" .env | cut -d'=' -f2)
DB_NAME=$(grep "^DB_NAME=" .env | cut -d'=' -f2)

echo "3.1 Testing PostgreSQL connection..."
if PGPASSWORD=$DB_PASSWORD psql -h localhost -p 6432 -U $DB_USER -d $DB_NAME -c "SELECT 1;" &> /dev/null; then
    echo -e "${GREEN}✓ PostgreSQL connection OK${NC}"
else
    echo -e "${RED}✗ PostgreSQL connection failed${NC}"
    exit 1
fi

echo "3.2 Testing Redis connection..."
if redis-cli -h localhost -p 6379 ping &> /dev/null; then
    echo -e "${GREEN}✓ Redis connection OK${NC}"
else
    echo -e "${RED}✗ Redis connection failed${NC}"
    exit 1
fi

# Phase 4: Stop Docker Services
echo ""
echo -e "${YELLOW}Phase 4: Stop Docker Services${NC}"
read -p "Stop Docker services and start PM2? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "4.1 Stopping Docker services..."
    docker-compose stop ingestion worker-1 worker-2 worker-3 depth-collector signal-generator api
    echo -e "${GREEN}✓ Docker services stopped${NC}"
    
    # Phase 5: Start PM2
    echo ""
    echo -e "${YELLOW}Phase 5: Start PM2 Services${NC}"
    echo "5.1 Starting services with PM2..."
    pm2 start ecosystem.config.js
    echo -e "${GREEN}✓ PM2 services started${NC}"
    
    echo "5.2 Saving PM2 process list..."
    pm2 save
    echo -e "${GREEN}✓ PM2 process list saved${NC}"
    
    echo "5.3 Setting up PM2 startup script..."
    pm2 startup
    echo -e "${GREEN}✓ Run the command above to enable PM2 on boot${NC}"
    
    # Phase 6: Verify
    echo ""
    echo -e "${YELLOW}Phase 6: Verification${NC}"
    sleep 5  # Give services time to start
    
    echo "6.1 PM2 Status:"
    pm2 status
    
    echo ""
    echo "6.2 Checking service health..."
    
    # Check API
    if curl -s http://localhost:8000/health &> /dev/null; then
        echo -e "${GREEN}✓ API responding${NC}"
    else
        echo -e "${RED}✗ API not responding${NC}"
    fi
    
    # Check logs for errors
    echo ""
    echo "6.3 Recent logs (check for errors):"
    pm2 logs --nostream --lines 10
    
    echo ""
    echo -e "${GREEN}=== Migration Complete! ===${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Monitor logs: pm2 logs"
    echo "2. Check status: pm2 status"
    echo "3. Test frontend: https://zopilot.in"
    echo "4. Update systemd timers to use PM2"
    echo ""
    echo "To rollback: pm2 stop all && docker-compose up -d"
else
    echo "Migration cancelled."
    exit 0
fi
