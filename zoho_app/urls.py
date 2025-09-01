from django.urls import path
from . import views
from . import outreach_views

app_name = 'zoho_app'

urlpatterns = [
    # Webhook endpoints
    path('webhook/zoho/contact/', views.handle_contact_webhook, name='contact_webhook'),
    path('webhook/zoho/account/', views.handle_account_webhook, name='account_webhook'),
    path('webhook/zoho/intern_role/', views.handle_intern_role_webhook, name='intern_role_webhook'),
    path('webhook/zoho/contact_sync/', views.contact_sync_webhook, name='contact_sync_webhook'),
    path('webhook/health/', views.health_check, name='health_check'),
    
    # ETL endpoints
    path('api/etl/trigger/', views.trigger_etl_sync, name='trigger_etl'),
    path('api/etl/status/', views.etl_status, name='etl_status'),
    
    # Test endpoints
    path('webhook/manual-cv-extraction/<str:contact_id>/', views.manual_cv_extraction, name='manual_cv_extraction'),

    
    # Job matching endpoints
    path('webhook/jobs/match/<str:contact_id>/', views.trigger_job_matching, name='trigger_job_matching'),
    path('webhook/jobs/matches/<str:contact_id>/', views.get_job_matches, name='get_job_matches'),
    
    # Skills endpoints
    path('webhook/skills/<str:contact_id>/', views.get_contact_skills, name='get_contact_skills'),
    
    # Outreach automation endpoints
    path('api/outreach/trigger/', outreach_views.trigger_outreach_automation, name='trigger_outreach'),
    path('api/outreach/status/', outreach_views.get_outreach_status, name='outreach_status'),
    path('api/outreach/analytics/', outreach_views.get_outreach_analytics, name='outreach_analytics'),
    path('api/outreach/follow-up/trigger/', outreach_views.trigger_follow_up_workflow, name='trigger_follow_up'),
    path('api/outreach/follow-up/pending/', outreach_views.get_pending_follow_ups, name='pending_follow_ups'),
    path('api/outreach/process-email-replies/', outreach_views.process_email_replies_view, name='process_email_replies'),
]
