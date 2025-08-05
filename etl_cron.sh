#!/bin/bash
# Production ETL Cron Job Script
# Place this in your production server and add to crontab

# Set up environment
export PATH="/usr/local/bin:/usr/bin:/bin"
cd /path/to/your/zoho_job_automation  # Update this path

# Log file
LOG_FILE="/var/log/etl_cron.log"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$DATE] Starting incremental ETL cron job" >> $LOG_FILE

# Run incremental ETL
python3 production_etl.py incremental >> $LOG_FILE 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$DATE] ✅ Incremental ETL completed successfully" >> $LOG_FILE
else
    echo "[$DATE] ❌ Incremental ETL failed with exit code $EXIT_CODE" >> $LOG_FILE
    
    # Optional: Send notification on failure
    # curl -X POST "https://hooks.slack.com/your-webhook" -d '{"text":"ETL failed on production"}'
    # or send email notification
fi

echo "[$DATE] ETL cron job finished" >> $LOG_FILE
