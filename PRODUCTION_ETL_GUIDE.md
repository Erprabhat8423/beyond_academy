# Production ETL Deployment Guide

## 1. Setup Production Environment

### Update production_etl.py with your domain:
```python
PRODUCTION_URL = "https://your-actual-domain.com"
```

### Make scripts executable:
```bash
chmod +x production_etl.py
chmod +x etl_cron.sh
chmod +x django_etl.py
```

## 2. Cron Job Setup (Recommended for Regular Incremental Loads)

### Add to crontab for automated incremental loads:
```bash
# Edit crontab
crontab -e

# Add these lines for different schedules:

# Every 15 minutes (high frequency)
*/15 * * * * /path/to/your/project/etl_cron.sh

# Every hour
0 * * * * /path/to/your/project/etl_cron.sh

# Every 4 hours  
0 */4 * * * /path/to/your/project/etl_cron.sh

# Daily at 2 AM
0 2 * * * /path/to/your/project/etl_cron.sh

# Weekly full sync on Sunday at 3 AM
0 3 * * 0 cd /path/to/your/project && python3 production_etl.py full
```

## 3. Manual Production Commands

### API Method (Remote):
```bash
# Incremental load (recommended for production)
curl -X POST "https://your-domain.com/api/etl/trigger/?entity=all"

# Full load (use sparingly)
curl -X POST "https://your-domain.com/api/etl/trigger/?entity=all&full=true"

# Check status
curl -X GET "https://your-domain.com/api/etl/status/"
```

### Script Method (On server):
```bash
# Incremental loads
python3 production_etl.py incremental
python3 production_etl.py incremental contacts
python3 production_etl.py incremental accounts
python3 production_etl.py incremental intern_roles

# Full loads
python3 production_etl.py full
python3 production_etl.py full contacts

# Check status
python3 production_etl.py status
```

### Django Command Method (On server):
```bash
# Incremental (default)
python manage.py run_etl

# Full load
python manage.py run_etl --full

# Entity specific
python manage.py run_etl --contacts-only
python manage.py run_etl --accounts-only --full
```

## 4. Monitoring & Logging

### Check logs:
```bash
# Cron job logs
tail -f /var/log/etl_cron.log

# Application logs
tail -f etl_production.log

# Django logs
tail -f /path/to/django/logs/django.log
```

### Monitor sync tracker via API:
```bash
# Check last sync times and record counts
curl -X GET "https://your-domain.com/api/etl/status/" | jq '.'
```

## 5. Production Best Practices

### Incremental Load Schedule:
- **High-traffic**: Every 15-30 minutes
- **Medium-traffic**: Every 1-2 hours  
- **Low-traffic**: Every 4-6 hours
- **Batch processing**: Daily

### Full Load Schedule:
- **Weekly**: Sunday nights
- **Monthly**: First Sunday of month
- **On-demand**: Only when needed

### Error Handling:
- Monitor logs regularly
- Set up alerts for failures
- Have rollback procedures ready
- Test in staging first

### Performance Tips:
- Use incremental loads by default
- Schedule full loads during low-traffic periods
- Monitor API rate limits
- Use appropriate timeouts

## 6. Troubleshooting

### Common Issues:
1. **Timeout**: Increase timeout in scripts
2. **API Rate Limits**: Add delays between requests
3. **Network Issues**: Implement retry logic
4. **Database Locks**: Run during low-traffic periods

### Debug Commands:
```bash
# Check current status
python3 production_etl.py status

# Test connectivity
curl -X GET "https://your-domain.com/webhook/health/"

# Check sync tracker
curl -X GET "https://your-domain.com/api/etl/status/"
```
