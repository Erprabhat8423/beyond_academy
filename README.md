# Zoho Job Automation - Django Implementation

This Django project implements a complete ETL pipeline and job matching system for Zoho CRM integration. It includes contact management, CV processing, skill extraction, and automated job matching capabilities.

## Features

- **ETL Pipeline**: Sync contacts, accounts, and intern roles from Zoho CRM
- **CV Management**: Download and process CV attachments from Zoho
- **Skill Extraction**: Extract skills from CVs using OpenAI GPT
- **Job Matching**: Automated matching between contacts and intern roles
- **Webhook Support**: Real-time processing of Zoho CRM updates
- **Django Admin**: Web interface for data management

## Project Structure

```
zoho_job_automation/
├── zoho_app/                 # Main Django app
│   ├── models.py            # Database models
│   ├── views.py             # Webhook and API views
│   ├── admin.py             # Django admin configuration
│   ├── management/commands/ # Django management commands
│   └── migrations/          # Database migrations
├── etl/                     # ETL pipeline modules
│   ├── pipeline.py          # Data sync from Zoho
│   └── job_matcher.py       # Job matching logic
├── zoho/                    # Zoho API integration
│   ├── api_client.py        # Zoho API client
│   ├── auth.py              # Authentication
│   ├── attachments.py       # CV download manager
│   └── skill_extractor.py   # AI skill extraction
└── requirements.txt         # Dependencies
```

## Setup Instructions

### 1. Environment Setup

1. Clone/copy the project files
2. Create a virtual environment:
   ```bash
   python -m venv venv
   venv\\Scripts\\activate  # Windows
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Copy environment file:
   ```bash
   copy .env.example .env
   ```

5. Edit `.env` file with your credentials:
   ```env
   ZOHO_CLIENT_ID=your_zoho_client_id
   ZOHO_CLIENT_SECRET=your_zoho_client_secret
   ZOHO_REFRESH_TOKEN=your_zoho_refresh_token
   OPENAI_API_KEY=your_openai_api_key
   ```

### 2. Database Setup

1. Run migrations:
   ```bash
   python manage.py makemigrations
   python manage.py migrate
   ```

2. Create superuser:
   ```bash
   python manage.py createsuperuser
   ```

### 3. Run the Application

1. Start Django development server:
   ```bash
   python manage.py runserver
   ```

2. Access Django Admin: http://localhost:8000/admin/

## Management Commands

### ETL Pipeline

```bash
# Run full ETL pipeline (all entities)
python manage.py run_etl

# Run incremental sync (default)
python manage.py run_etl

# Force full sync
python manage.py run_etl --full

# Sync specific entities only
python manage.py run_etl --contacts-only
python manage.py run_etl --accounts-only
python manage.py run_etl --intern-roles-only
```

### Job Matching

```bash
# Run job matching for all contacts
python manage.py run_job_matching

# Run for specific contact
python manage.py run_job_matching --contact-id CONTACT_ID

# Show detailed statistics
python manage.py run_job_matching --show-stats

# Clean old matches before running
python manage.py run_job_matching --clean-old-matches
```

### CV Download

```bash
# Download CVs for all contacts
python manage.py download_cvs

# Download for specific contact
python manage.py download_cvs --contact-id CONTACT_ID

# Specify download directory
python manage.py download_cvs --download-dir /path/to/downloads

# Limit number of contacts
python manage.py download_cvs --limit 50
```

## API Endpoints

### Webhook Endpoints

- `POST /webhook/zoho/contact/` - Handle contact updates from Zoho
- `GET /webhook/health/` - Health check endpoint

### Test Endpoints

- `POST /webhook/test-cv-download/<contact_id>/` - Test CV download
- `POST /webhook/jobs/match/<contact_id>/` - Trigger job matching

### Data Endpoints

- `GET /webhook/jobs/matches/<contact_id>/` - Get job matches for contact
- `GET /webhook/skills/<contact_id>/` - Get extracted skills for contact

## Usage Examples

### 1. Complete ETL and Matching Workflow

```bash
# Step 1: Sync data from Zoho
python manage.py run_etl

# Step 2: Download CVs and extract skills
python manage.py download_cvs

# Step 3: Run job matching
python manage.py run_job_matching --show-stats
```

### 2. Process Specific Contact

```bash
# Download CV for specific contact
python manage.py download_cvs --contact-id "123456789"

# Run job matching for specific contact
python manage.py run_job_matching --contact-id "123456789"
```

### 3. API Usage

```bash
# Trigger job matching via API
curl -X POST http://localhost:8000/webhook/jobs/match/123456789/

# Get job matches
curl http://localhost:8000/webhook/jobs/matches/123456789/

# Get extracted skills
curl http://localhost:8000/webhook/skills/123456789/
```

## Configuration

### Zoho CRM Setup

1. Create Zoho CRM application
2. Get Client ID and Client Secret
3. Generate refresh token using OAuth flow
4. Configure webhook URLs in Zoho CRM

### OpenAI Setup (Optional)

1. Get OpenAI API key
2. Add to `.env` file
3. Skill extraction will work automatically when CVs are downloaded

### Webhook Configuration

1. Set webhook secret in `.env`
2. Configure Zoho CRM to send webhooks to your Django server
3. Use ngrok or similar for local development

## Database Models

- **Contact**: Student/candidate information
- **Account**: Company information
- **InternRole**: Available internship positions
- **Document**: Downloaded CV files and metadata
- **Skill**: Extracted skills from CVs
- **JobMatch**: Matching results between contacts and roles
- **SyncTracker**: ETL sync status tracking

## Troubleshooting

### Common Issues

1. **Import errors**: Make sure all dependencies are installed
2. **Database errors**: Run migrations after any model changes
3. **API errors**: Check Zoho credentials and token validity
4. **File download errors**: Ensure download directory is writable

### Logs

- Django logs: `django.log`
- ETL logs: Check console output during command execution
- Debug mode: Set `DEBUG=True` in `.env`

## Development

### Adding New Features

1. Models: Add to `zoho_app/models.py`
2. ETL: Extend `etl/pipeline.py`
3. Matching: Enhance `etl/job_matcher.py`
4. API: Add views to `zoho_app/views.py`

### Testing

```bash
# Run Django tests
python manage.py test

# Test specific functionality
python manage.py run_etl --contacts-only
python manage.py run_job_matching --contact-id TEST_ID
```

## Production Deployment

1. Use PostgreSQL database
2. Set up proper logging
3. Configure CORS settings
4. Use environment variables for all secrets
5. Set up monitoring and alerts
6. Use proper web server (Gunicorn, uWSGI)

## Support

For issues or questions:
1. Check the logs first
2. Verify environment configuration
3. Test API connectivity
4. Check Zoho CRM webhook configuration
