"""
Outreach Views

API endpoints for managing outreach automation and follow-up workflows
"""

import json
import logging
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.db.models import Q, Count
from django.utils import timezone
from datetime import timedelta
from zoho_app.models import OutreachLog, FollowUpTask, Contact, InternRole, JobMatch
from zoho_app.outreach_automation import run_outreach_automation
from zoho_app.follow_up_workflow import process_follow_up_workflow
from zoho_app.email_reply_parser import EmailReplyParser

logger = logging.getLogger(__name__)


@csrf_exempt
@require_http_methods(["POST", "GET"])
def trigger_outreach_automation(request):
    """
    Trigger outreach automation manually or get status
    """
    try:
        if request.method == 'GET':
            # Return current outreach status
            return get_outreach_status(request)
        
        # Parse parameters
        dry_run = request.GET.get('dry_run', 'false').lower() == 'true'
        max_roles = request.GET.get('max_roles')
        
        if max_roles:
            max_roles = int(max_roles)
        
        logger.info(f"Manual outreach trigger: dry_run={dry_run}, max_roles={max_roles}")
        
        # Run outreach automation
        results = run_outreach_automation(dry_run=dry_run, max_roles=max_roles)
        
        return JsonResponse({
            'status': 'success',
            'results': results,
            'timestamp': timezone.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error triggering outreach automation: {e}")
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_outreach_status(request):
    """
    Get current outreach automation status and statistics
    """
    try:
        # Get recent outreach statistics
        last_24h = timezone.now() - timedelta(hours=24)
        last_week = timezone.now() - timedelta(days=7)
        
        stats = {
            'total_outreach_logs': OutreachLog.objects.count(),
            'sent_last_24h': OutreachLog.objects.filter(
                sent_at__gte=last_24h,
                is_sent=True
            ).count(),
            'sent_last_week': OutreachLog.objects.filter(
                sent_at__gte=last_week,
                is_sent=True
            ).count(),
            'pending_follow_ups': FollowUpTask.objects.filter(
                completed=False,
                scheduled_date__lte=timezone.now()
            ).count(),
            'total_responses': OutreachLog.objects.filter(
                response_received=True
            ).count(),
            'response_rate': 0
        }
        
        # Calculate response rate
        total_sent = OutreachLog.objects.filter(is_sent=True).count()
        if total_sent > 0:
            stats['response_rate'] = round((stats['total_responses'] / total_sent) * 100, 2)
        
        # Get recent outreach logs
        recent_logs = OutreachLog.objects.filter(
            is_sent=True
        ).order_by('-sent_at')[:10]
        
        logs_data = []
        for log in recent_logs:
            logs_data.append({
                'id': log.id,
                'role_title': log.role_title,
                'company_name': log.company_name,
                'email_type': log.email_type,
                'candidates_count': log.candidates_count,
                'is_urgent': log.is_urgent,
                'sent_at': log.sent_at.isoformat() if log.sent_at else None,
                'response_received': log.response_received,
                'response_type': log.response_type,
                'follow_up_count': log.follow_up_count
            })
        
        return JsonResponse({
            'status': 'success',
            'stats': stats,
            'recent_logs': logs_data,
            'timestamp': timezone.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting outreach status: {e}")
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def trigger_follow_up_workflow(request):
    """
    Trigger follow-up workflow processing
    """
    try:
        logger.info("Manual follow-up workflow trigger")
        
        results = process_follow_up_workflow()
        
        return JsonResponse({
            'status': 'success',
            'results': results,
            'timestamp': timezone.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error triggering follow-up workflow: {e}")
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)



@require_http_methods(["GET"])
def get_outreach_analytics(request):
    """
    Get outreach analytics and insights
    """
    try:
        # Time ranges
        last_week = timezone.now() - timedelta(days=7)
        last_month = timezone.now() - timedelta(days=30)
        
        # Basic metrics
        analytics = {
            'overview': {
                'total_outreach_sent': OutreachLog.objects.filter(is_sent=True).count(),
                'total_responses': OutreachLog.objects.filter(response_received=True).count(),
                'total_companies_contacted': OutreachLog.objects.filter(is_sent=True).values('company_id').distinct().count(),
                'total_roles_promoted': OutreachLog.objects.filter(is_sent=True).values('intern_role_id').distinct().count(),
            },
            'recent_performance': {
                'sent_last_week': OutreachLog.objects.filter(sent_at__gte=last_week, is_sent=True).count(),
                'responses_last_week': OutreachLog.objects.filter(response_date__gte=last_week).count(),
                'sent_last_month': OutreachLog.objects.filter(sent_at__gte=last_month, is_sent=True).count(),
                'responses_last_month': OutreachLog.objects.filter(response_date__gte=last_month).count(),
            },
            'email_types': {},
            'response_types': {},
            'urgent_vs_normal': {},
            'top_performing_roles': [],
            'follow_up_effectiveness': {}
        }
        
        # Email type breakdown
        email_types = OutreachLog.objects.filter(is_sent=True).values('email_type').annotate(count=Count('id'))
        analytics['email_types'] = {item['email_type']: item['count'] for item in email_types}
        
        # Response type breakdown
        response_types = OutreachLog.objects.filter(response_received=True).values('response_type').annotate(count=Count('id'))
        analytics['response_types'] = {item['response_type']: item['count'] for item in response_types}
        
        # Urgent vs normal outreach
        urgent_sent = OutreachLog.objects.filter(is_sent=True, is_urgent=True).count()
        normal_sent = OutreachLog.objects.filter(is_sent=True, is_urgent=False).count()
        urgent_responses = OutreachLog.objects.filter(response_received=True, is_urgent=True).count()
        normal_responses = OutreachLog.objects.filter(response_received=True, is_urgent=False).count()
        
        analytics['urgent_vs_normal'] = {
            'urgent': {
                'sent': urgent_sent,
                'responses': urgent_responses,
                'response_rate': round((urgent_responses / urgent_sent * 100) if urgent_sent > 0 else 0, 2)
            },
            'normal': {
                'sent': normal_sent,
                'responses': normal_responses,
                'response_rate': round((normal_responses / normal_sent * 100) if normal_sent > 0 else 0, 2)
            }
        }
        
        # Top performing roles (by response rate)
        role_performance = OutreachLog.objects.filter(is_sent=True).values(
            'intern_role_id', 'role_title'
        ).annotate(
            sent_count=Count('id'),
            response_count=Count('id', filter=Q(response_received=True))
        ).order_by('-response_count')[:10]
        
        for role in role_performance:
            response_rate = (role['response_count'] / role['sent_count'] * 100) if role['sent_count'] > 0 else 0
            analytics['top_performing_roles'].append({
                'role_id': role['intern_role_id'],
                'role_title': role['role_title'],
                'sent_count': role['sent_count'],
                'response_count': role['response_count'],
                'response_rate': round(response_rate, 2)
            })
        
        # Follow-up effectiveness
        initial_responses = OutreachLog.objects.filter(
            email_type='initial',
            response_received=True
        ).count()
        
        follow_up_responses = OutreachLog.objects.filter(
            email_type='follow_up',
            response_received=True
        ).count()
        
        final_responses = OutreachLog.objects.filter(
            email_type='final',
            response_received=True
        ).count()
        
        analytics['follow_up_effectiveness'] = {
            'initial_responses': initial_responses,
            'follow_up_responses': follow_up_responses,
            'final_responses': final_responses,
            'total_follow_up_responses': follow_up_responses + final_responses
        }
        
        return JsonResponse({
            'status': 'success',
            'analytics': analytics,
            'timestamp': timezone.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting outreach analytics: {e}")
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_pending_follow_ups(request):
    """
    Get list of pending follow-up tasks
    """
    try:
        now = timezone.now()
        limit = int(request.GET.get('limit', 50))
        
        # Get pending follow-up tasks
        pending_tasks = FollowUpTask.objects.filter(
            completed=False
        ).select_related('outreach_log').order_by('scheduled_date')[:limit]
        
        tasks_data = []
        for task in pending_tasks:
            outreach = task.outreach_log
            is_due = task.scheduled_date <= now
            
            tasks_data.append({
                'task_id': task.id,
                'follow_up_type': task.follow_up_type,
                'scheduled_date': task.scheduled_date.isoformat(),
                'is_due': is_due,
                'hours_until_due': round((task.scheduled_date - now).total_seconds() / 3600, 1),
                'outreach_info': {
                    'id': outreach.id,
                    'role_title': outreach.role_title,
                    'company_name': outreach.company_name,
                    'candidates_count': outreach.candidates_count,
                    'is_urgent': outreach.is_urgent,
                    'sent_at': outreach.sent_at.isoformat() if outreach.sent_at else None
                }
            })
        
        # Count by type
        due_count = sum(1 for task in tasks_data if task['is_due'])
        
        return JsonResponse({
            'status': 'success',
            'pending_tasks': tasks_data,
            'total_pending': len(tasks_data),
            'due_now': due_count,
            'timestamp': timezone.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting pending follow-ups: {e}")
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def process_email_replies_view(request):
    """
    Trigger processing of email replies (same as management command `process_email_replies`).
    Use POST to invoke; returns processing results.
    """
    try:
        parser = EmailReplyParser()
        results = parser.process_replies()
        return JsonResponse({
            'status': 'success',
            'results': results
        })
    except Exception as e:
        logger.error(f"Error processing email replies via HTTP: {e}")
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)
