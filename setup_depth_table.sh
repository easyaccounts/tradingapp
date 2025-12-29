#!/bin/bash

echo "Setting up depth_200_snapshots table..."

# Drop existing table if any
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "DROP TABLE IF EXISTS depth_200_snapshots CASCADE;"

# Create table with corrected schema
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb < database/depth_200_snapshots.sql

echo "✓ Table setup complete"
