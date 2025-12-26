#!/bin/bash

# Trading Platform - VPS Setup Script
# For Hetzner VPS at 82.180.144.255 with domain zopilot.in

set -e

echo "=================================================="
echo "Trading Platform - VPS Setup Script"
echo "Domain: zopilot.in"
echo "=================================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Please run as root (use: sudo bash setup-vps.sh)${NC}"
    exit 1
fi

# Step 1: Update system
echo -e "${YELLOW}[1/8] Updating system...${NC}"
apt update && apt upgrade -y

# Step 2: Install Docker
echo -e "${YELLOW}[2/8] Installing Docker...${NC}"
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    rm get-docker.sh
    echo -e "${GREEN}✓ Docker installed${NC}"
else
    echo -e "${GREEN}✓ Docker already installed${NC}"
fi

# Step 3: Install Docker Compose
echo -e "${YELLOW}[3/8] Installing Docker Compose...${NC}"
if ! command -v docker-compose &> /dev/null; then
    apt install docker-compose -y
    echo -e "${GREEN}✓ Docker Compose installed${NC}"
else
    echo -e "${GREEN}✓ Docker Compose already installed${NC}"
fi

# Step 4: Install Git
echo -e "${YELLOW}[4/8] Installing Git...${NC}"
if ! command -v git &> /dev/null; then
    apt install git -y
    echo -e "${GREEN}✓ Git installed${NC}"
else
    echo -e "${GREEN}✓ Git already installed${NC}"
fi

# Step 5: Install UFW
echo -e "${YELLOW}[5/8] Installing UFW firewall...${NC}"
if ! command -v ufw &> /dev/null; then
    apt install ufw -y
fi

# Step 6: Configure Firewall
echo -e "${YELLOW}[6/8] Configuring firewall...${NC}"
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp comment 'SSH'
ufw allow 80/tcp comment 'HTTP'
ufw allow 443/tcp comment 'HTTPS'
ufw --force enable
echo -e "${GREEN}✓ Firewall configured${NC}"

# Step 7: Configure Docker logging
echo -e "${YELLOW}[7/8] Configuring Docker log rotation...${NC}"
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
echo -e "${GREEN}✓ Docker logging configured${NC}"

# Step 8: Create backup directory
echo -e "${YELLOW}[8/8] Creating backup directory...${NC}"
mkdir -p /opt/backups
echo -e "${GREEN}✓ Backup directory created${NC}"

echo ""
echo -e "${GREEN}=================================================="
echo "✓ VPS Setup Complete!"
echo "==================================================${NC}"
echo ""
echo "Next steps:"
echo "1. Clone your repository to /opt/tradingapp"
echo "2. Copy .env.example to .env and configure"
echo "3. Run: bash deploy-ssl.sh"
echo ""
echo "For detailed instructions, see DEPLOYMENT.md"
