#!/usr/bin/env python3
"""
Automated SSL monitoring - runs continuously and logs connection attempts
Logs to: /opt/tradingapp/logs/ssl-monitor.log
"""
import requests
import time
import json
from datetime import datetime
import logging
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
logging.basicConfig(
    filename='/opt/tradingapp/logs/ssl-monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_connection(url, description, verify=True, timeout=5):
    """Test connection and return status"""
    try:
        start = time.time()
        response = requests.get(url, timeout=timeout, verify=verify, headers={"Host": "zopilot.in"})
        elapsed = time.time() - start
        return {
            "success": True,
            "status_code": response.status_code,
            "elapsed_ms": int(elapsed * 1000),
            "cf_ray": response.headers.get("CF-RAY", "N/A"),
            "description": description
        }
    except requests.exceptions.SSLError as e:
        return {"success": False, "error": f"SSL_ERROR: {str(e)}", "description": description}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "TIMEOUT", "description": description}
    except requests.exceptions.ConnectionError as e:
        return {"success": False, "error": f"CONNECTION_ERROR: {str(e)}", "description": description}
    except Exception as e:
        return {"success": False, "error": f"UNKNOWN: {str(e)}", "description": description}

def monitor_loop():
    """Run monitoring checks every 60 seconds"""
    logging.info("=== SSL Monitor Started ===")
    
    while True:
        timestamp = datetime.now().isoformat()
        
        # Test 1: Direct to origin (bypassing Cloudflare)
        direct = test_connection("https://82.180.144.255/health", "DIRECT_ORIGIN", verify=False)
        
        # Test 2: Through Cloudflare
        cloudflare = test_connection("https://zopilot.in/health", "CLOUDFLARE")
        
        # Log results
        if direct["success"] and cloudflare["success"]:
            logging.info(f"OK | Direct: {direct['elapsed_ms']}ms | Cloudflare: {cloudflare['elapsed_ms']}ms (CF-RAY: {cloudflare['cf_ray']})")
        else:
            # Log failures prominently
            logging.error(f"FAILURE | Direct: {direct} | Cloudflare: {cloudflare}")
        
        # Check every 60 seconds
        time.sleep(60)

if __name__ == "__main__":
    try:
        monitor_loop()
    except KeyboardInterrupt:
        logging.info("=== SSL Monitor Stopped ===")
