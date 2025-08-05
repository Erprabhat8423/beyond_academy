#!/usr/bin/env python
"""
Production ETL Runner
Run this script on your production server to trigger incremental ETL loads
"""
import requests
import json
import time
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_production.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Production configuration
PRODUCTION_URL = "https://your-production-domain.com"  # Update this
API_TIMEOUT = 600  # 10 minutes timeout for ETL operations

class ProductionETLRunner:
    def __init__(self, base_url=PRODUCTION_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.timeout = API_TIMEOUT

    def run_incremental_etl(self, entity='all'):
        """Run incremental ETL load"""
        logger.info(f"Starting incremental ETL for entity: {entity}")
        
        try:
            url = f"{self.base_url}/api/etl/trigger/"
            params = {'entity': entity}
            
            logger.info(f"Making request to: {url}")
            response = self.session.post(url, params=params)
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"✅ ETL completed successfully")
                logger.info(f"Duration: {result.get('duration', 'N/A')}")
                logger.info(f"Results: {result.get('results', {})}")
                return result
            else:
                logger.error(f"❌ ETL failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error("❌ ETL request timed out")
            return None
        except Exception as e:
            logger.error(f"❌ ETL failed with error: {e}")
            return None

    def run_full_etl(self, entity='all'):
        """Run full ETL load (override incremental)"""
        logger.info(f"Starting FULL ETL for entity: {entity}")
        
        try:
            url = f"{self.base_url}/api/etl/trigger/"
            params = {'entity': entity, 'full': 'true'}
            
            logger.info(f"Making request to: {url}")
            response = self.session.post(url, params=params)
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"✅ Full ETL completed successfully")
                logger.info(f"Duration: {result.get('duration', 'N/A')}")
                logger.info(f"Results: {result.get('results', {})}")
                return result
            else:
                logger.error(f"❌ Full ETL failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error("❌ Full ETL request timed out")
            return None
        except Exception as e:
            logger.error(f"❌ Full ETL failed with error: {e}")
            return None

    def check_etl_status(self):
        """Check current ETL status and sync tracker"""
        try:
            url = f"{self.base_url}/api/etl/status/"
            response = self.session.get(url)
            
            if response.status_code == 200:
                result = response.json()
                logger.info("📊 Current ETL Status:")
                
                stats = result.get('statistics', {})
                logger.info(f"Records - Contacts: {stats.get('contacts_count', 0)}, "
                          f"Accounts: {stats.get('accounts_count', 0)}, "
                          f"Intern Roles: {stats.get('intern_roles_count', 0)}")
                
                for tracker in stats.get('sync_trackers', []):
                    logger.info(f"  {tracker['entity_type']}: "
                              f"Last sync: {tracker.get('last_sync_timestamp', 'Never')}, "
                              f"Records: {tracker.get('records_synced', 0)}")
                
                return result
            else:
                logger.error(f"❌ Status check failed with status {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Status check failed: {e}")
            return None

def main():
    """Main execution function"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python production_etl.py incremental [entity]")
        print("  python production_etl.py full [entity]")
        print("  python production_etl.py status")
        print("")
        print("Examples:")
        print("  python production_etl.py incremental")
        print("  python production_etl.py incremental contacts")
        print("  python production_etl.py full")
        print("  python production_etl.py status")
        sys.exit(1)

    command = sys.argv[1].lower()
    entity = sys.argv[2] if len(sys.argv) > 2 else 'all'
    
    runner = ProductionETLRunner()
    
    logger.info(f"🚀 Production ETL Runner Started - Command: {command}, Entity: {entity}")
    
    if command == 'incremental':
        result = runner.run_incremental_etl(entity)
        if result:
            logger.info("🎉 Incremental ETL completed successfully!")
        else:
            logger.error("💥 Incremental ETL failed!")
            sys.exit(1)
            
    elif command == 'full':
        result = runner.run_full_etl(entity)
        if result:
            logger.info("🎉 Full ETL completed successfully!")
        else:
            logger.error("💥 Full ETL failed!")
            sys.exit(1)
            
    elif command == 'status':
        result = runner.check_etl_status()
        if not result:
            logger.error("💥 Status check failed!")
            sys.exit(1)
            
    else:
        logger.error(f"❌ Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()
