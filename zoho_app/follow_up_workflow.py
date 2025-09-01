"""
Follow-up Workflow Management

This module handles the automated follow-up sequence for outreach emails:
- Follow-up 1: after 48h
- Follow-up 2: after 96h (48h after follow-up 1)
- Move to next 3 roles: after 192h (48h after follow-up 2) if no response
"""

import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from django.db import transaction
from django.utils import timezone
from zoho_app.models import OutreachLog, FollowUpTask, Contact, InternRole, JobMatch, CandidateOutreachHistory
from zoho_app.outreach_automation import OutreachAutomation

logger = logging.getLogger(__name__)


class FollowUpWorkflow:
    """
    Manages follow-up workflow for outreach emails
    """
    
    def __init__(self):
        self.outreach_automation = OutreachAutomation()
    
    def process_pending_follow_ups(self) -> Dict[str, Any]:
        """
        Process all pending follow-up tasks that are due
        """
        try:
            now = timezone.now()
            
            # Get all pending follow-up tasks that are due
            pending_tasks = FollowUpTask.objects.filter(
                completed=False,
                scheduled_date__lte=now
            ).select_related('outreach_log').order_by('scheduled_date')
            
            results = {
                'total_tasks': len(pending_tasks),
                'follow_up_sent': 0,
                'final_sent': 0,
                'moved_to_next': 0,
                'errors': 0,
                'task_results': []
            }
            
            for task in pending_tasks:
                try:
                    result = self.process_follow_up_task(task)
                    results['task_results'].append(result)
                    
                    if result['status'] == 'success':
                        if task.follow_up_type == 'follow_up':
                            results['follow_up_sent'] += 1
                        elif task.follow_up_type == 'final':
                            results['final_sent'] += 1
                        elif task.follow_up_type == 'move_to_next':
                            results['moved_to_next'] += 1
                    else:
                        results['errors'] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing follow-up task {task.id}: {e}")
                    results['errors'] += 1
                    results['task_results'].append({
                        'task_id': task.id,
                        'status': 'error',
                        'error': str(e)
                    })
            
            logger.info(f"Follow-up processing completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error processing pending follow-ups: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def process_follow_up_task(self, task: FollowUpTask) -> Dict[str, Any]:
        """
        Process a specific follow-up task
        """
        try:
            outreach_log = task.outreach_log
            
            # Check if outreach has already received a response
            if outreach_log.response_received:
                logger.info(f"Skipping follow-up task {task.id} - response already received")
                task.completed = True
                task.completed_at = timezone.now()
                task.save()
                return {
                    'task_id': task.id,
                    'status': 'skipped',
                    'reason': 'response_received'
                }
            
            if task.follow_up_type in ['follow_up', 'final']:
                return self.send_follow_up_email(task, outreach_log)
            elif task.follow_up_type == 'move_to_next':
                return self.move_candidates_to_next_roles(task, outreach_log)
            else:
                logger.warning(f"Unknown follow-up type: {task.follow_up_type}")
                return {
                    'task_id': task.id,
                    'status': 'error',
                    'error': 'unknown_follow_up_type'
                }
                
        except Exception as e:
            logger.error(f"Error processing follow-up task {task.id}: {e}")
            return {
                'task_id': task.id,
                'status': 'error',
                'error': str(e)
            }
    
    def send_follow_up_email(self, task: FollowUpTask, outreach_log: OutreachLog) -> Dict[str, Any]:
        """
        Send a follow-up email for an outreach, using urgent or non-urgent templates as appropriate
        """
        try:
            candidate_ids = json.loads(outreach_log.candidate_ids)
            recipients = json.loads(outreach_log.recipients)
            role = InternRole.objects.get(id=outreach_log.intern_role_id)
            candidates = []
            for candidate_id in candidate_ids:
                try:
                    contact = Contact.objects.get(id=candidate_id)
                    candidates.append({
                        'contact_id': contact.id,
                        'contact': contact,
                        'full_name': contact.full_name,
                        'email': contact.email,
                        'start_date': contact.start_date,
                        'end_date': contact.end_date,
                        'student_bio': contact.student_bio,
                        'requires_visa': contact.requires_a_visa,
                        'partnership_specialist_id': contact.partnership_specialist_id,
                        'skills': contact.skills,
                        'university_name': contact.university_name,
                        'graduation_date': contact.graduation_date,
                        'industry_choice_1': contact.industry_choice_1,
                        'industry_choice_2': contact.industry_choice_2,
                        'industry_choice_3': contact.industry_choice_3,
                    })
                except Contact.DoesNotExist:
                    logger.warning(f"Contact {candidate_id} not found for follow-up")
                    continue
            if not candidates:
                logger.warning(f"No candidates found for follow-up task {task.id}")
                task.completed = True
                task.completed_at = timezone.now()
                task.save()
                return {
                    'task_id': task.id,
                    'status': 'skipped',
                    'reason': 'no_candidates'
                }
            company_contacts = [{'email': email} for email in recipients]
            urgent = bool(getattr(outreach_log, 'is_urgent', False))
            email_content = self.outreach_automation.create_outreach_email(
                role, candidates, company_contacts, task.follow_up_type, outreach_log, urgent=urgent
            )
            if not email_content:
                return {
                    'task_id': task.id,
                    'status': 'error',
                    'error': 'email_creation_failed'
                }
            sender_info = None
            for candidate in candidates:
                if candidate.get('partnership_specialist_id'):
                    sender_info = self.outreach_automation.get_partnership_specialist_email(
                        candidate['partnership_specialist_id']
                    )
                    if sender_info:
                        break
            if not sender_info:
                return {
                    'task_id': task.id,
                    'status': 'error',
                    'error': 'no_partnership_specialist'
                }
            success = self.outreach_automation.send_email(
                email_content,
                sender_info['email'],
                sender_info['full_name']
            )
            if success:
                message_id = email_content.get('message_id')
                thread_id = email_content.get('thread_id')
                in_reply_to = email_content.get('in_reply_to')
                follow_up_log = OutreachLog.objects.create(
                    intern_role_id=outreach_log.intern_role_id,
                    role_title=outreach_log.role_title,
                    company_id=outreach_log.company_id,
                    company_name=outreach_log.company_name,
                    subject=email_content.get('subject', ''),
                    email_type=task.follow_up_type,
                    sender_email=sender_info['email'],
                    sender_name=sender_info['full_name'],
                    recipients=outreach_log.recipients,
                    candidate_ids=outreach_log.candidate_ids,
                    candidates_count=outreach_log.candidates_count,
                    is_urgent=outreach_log.is_urgent,
                    is_sent=True,
                    sent_at=timezone.now(),
                    follow_up_count=outreach_log.follow_up_count + 1,
                    message_id=message_id,
                    thread_id=thread_id,
                    in_reply_to=in_reply_to,
                    parent_outreach_log=outreach_log
                )
                outreach_log.follow_up_count += 1
                outreach_log.last_follow_up_date = timezone.now()
                outreach_log.save()
                task.completed = True
                task.completed_at = timezone.now()
                task.save()
                logger.info(f"Follow-up email sent for task {task.id}")
                return {
                    'task_id': task.id,
                    'status': 'success',
                    'follow_up_log_id': follow_up_log.id,
                    'email_type': task.follow_up_type
                }
            else:
                return {
                    'task_id': task.id,
                    'status': 'error',
                    'error': 'email_send_failed'
                }
        except Exception as e:
            logger.error(f"Error sending follow-up email for task {task.id}: {e}")
            return {
                'task_id': task.id,
                'status': 'error',
                'error': str(e)
            }
    
    def move_candidates_to_next_roles(self, task: FollowUpTask, outreach_log: OutreachLog) -> Dict[str, Any]:
        """
        Move candidates to next 3 roles if no response received after final follow-up
        Start new outreach process for each candidate with their top 3 next matches
        """
        try:
            candidate_ids = json.loads(outreach_log.candidate_ids)
            moved_candidates = []
            new_outreach_initiated = []
            
            for candidate_id in candidate_ids:
                try:
                    # Get next 3 best matching roles for this candidate
                    next_role_ids = self.get_next_roles_for_candidate(candidate_id, outreach_log.intern_role_id)
                    
                    if next_role_ids:
                        # Group candidate with each of their next top roles
                        for role_id in next_role_ids:
                            try:
                                # Create outreach for this candidate to this new role
                                result = self.initiate_outreach_for_next_role(candidate_id, role_id)
                                if result['status'] == 'success':
                                    new_outreach_initiated.append(result)
                                    logger.info(f"Initiated new outreach for candidate {candidate_id} to role {role_id}")
                                else:
                                    logger.warning(f"Failed to initiate outreach for candidate {candidate_id} to role {role_id}: {result.get('reason')}")
                                    
                            except Exception as e:
                                logger.error(f"Error initiating outreach for candidate {candidate_id} to role {role_id}: {e}")
                                continue
                        
                        moved_candidates.append({
                            'candidate_id': candidate_id,
                            'next_roles_count': len(next_role_ids),
                            'outreach_initiated': len([r for r in new_outreach_initiated if r.get('candidate_id') == candidate_id])
                        })
                        
                        logger.info(f"Moved candidate {candidate_id} to {len(next_role_ids)} next roles")
                    else:
                        logger.info(f"No next roles found for candidate {candidate_id}")
                    
                except Exception as e:
                    logger.error(f"Error moving candidate {candidate_id} to next roles: {e}")
                    continue
            
            # Mark task as completed
            task.completed = True
            task.completed_at = timezone.now()
            task.save()
            
            # Update outreach log to indicate candidates moved
            outreach_log.response_type = 'moved_to_next'
            outreach_log.save()
            
            # Update candidate outreach history for the original role
            candidate_ids = json.loads(outreach_log.candidate_ids)
            CandidateOutreachHistory.objects.filter(
                outreach_log=outreach_log,
                contact_id__in=candidate_ids
            ).update(
                status='moved_to_next',
                last_follow_up_date=timezone.now()
            )
            
            return {
                'task_id': task.id,
                'status': 'success',
                'moved_candidates': moved_candidates,
                'total_moved': len(moved_candidates),
                'new_outreach_initiated': new_outreach_initiated,
                'total_new_outreach': len(new_outreach_initiated)
            }
            
        except Exception as e:
            logger.error(f"Error moving candidates to next roles for task {task.id}: {e}")
            return {
                'task_id': task.id,
                'status': 'error',
                'error': str(e)
            }
    
    def get_next_roles_for_candidate(self, candidate_id: str, exclude_role_id: str, limit: int = 3) -> List[str]:
        """
        Get next best matching roles for a candidate, excluding already tried roles
        """
        try:
            # Get all roles this candidate has already been pitched to
            tried_roles = CandidateOutreachHistory.objects.filter(
                contact_id=candidate_id
            ).values_list('intern_role_id', flat=True).distinct()
            
            # Get job matches for this candidate, excluding already tried roles
            matches = JobMatch.objects.filter(
                contact_id=candidate_id,
                status='active'
            ).exclude(
                intern_role_id__in=tried_roles
            ).order_by('-match_score')[:limit]
            
            role_ids = [match.intern_role_id for match in matches]
            
            logger.info(f"Found {len(role_ids)} next roles for candidate {candidate_id} (excluding {len(tried_roles)} already tried)")
            return role_ids
            
        except Exception as e:
            logger.error(f"Error getting next roles for candidate {candidate_id}: {e}")
            return []
    
    def initiate_outreach_for_next_role(self, candidate_id: str, role_id: str) -> Dict[str, Any]:
        """
        Initiate new outreach process for a candidate to a specific role
        This creates a new outreach cycle for the candidate-role combination
        Uses urgent or non-urgent templates as appropriate
        """
        try:
            contact = Contact.objects.get(id=candidate_id)
            role = InternRole.objects.get(id=role_id)
            candidate_info = {
                'contact_id': contact.id,
                'contact': contact,
                'full_name': contact.full_name,
                'email': contact.email,
                'start_date': contact.start_date,
                'end_date': contact.end_date,
                'student_bio': contact.student_bio,
                'requires_visa': contact.requires_a_visa,
                'partnership_specialist_id': contact.partnership_specialist_id,
                'skills': contact.skills,
                'university_name': contact.university_name,
                'graduation_date': contact.graduation_date,
                'industry_choice_1': contact.industry_choice_1,
                'industry_choice_2': contact.industry_choice_2,
                'industry_choice_3': contact.industry_choice_3,
            }
            urgent = self.outreach_automation.check_urgency(contact)
            # Use the outreach automation to process this single candidate-role pair
            # Pass urgent flag if needed in future for more granular control
            result = self.outreach_automation.process_outreach_for_role(role_id, [candidate_info])
            if result['status'] == 'success':
                return {
                    'status': 'success',
                    'candidate_id': candidate_id,
                    'role_id': role_id,
                    'role_title': role.role_title or role.name,
                    'company_name': role.intern_company_name,
                    'outreach_log_created': True
                }
            else:
                return {
                    'status': 'failed',
                    'candidate_id': candidate_id,
                    'role_id': role_id,
                    'reason': result.get('reason', 'unknown_error')
                }
        except Contact.DoesNotExist:
            logger.error(f"Contact {candidate_id} not found")
            return {
                'status': 'failed',
                'candidate_id': candidate_id,
                'role_id': role_id,
                'reason': 'contact_not_found'
            }
        except InternRole.DoesNotExist:
            logger.error(f"Intern role {role_id} not found")
            return {
                'status': 'failed',
                'candidate_id': candidate_id,
                'role_id': role_id,
                'reason': 'role_not_found'
            }
        except Exception as e:
            logger.error(f"Error initiating outreach for candidate {candidate_id} to role {role_id}: {e}")
            return {
                'status': 'failed',
                'candidate_id': candidate_id,
                'role_id': role_id,
                'reason': str(e)
            }
    
    def mark_response_received(self, outreach_log_id: int, response_type: str = 'interested') -> bool:
        """
        Mark that a response was received for an outreach email
        """
        try:
            outreach_log = OutreachLog.objects.get(id=outreach_log_id)
            outreach_log.response_received = True
            outreach_log.response_date = timezone.now()
            outreach_log.response_type = response_type
            outreach_log.save()
            
            # Update candidate outreach history
            candidate_ids = json.loads(outreach_log.candidate_ids)
            CandidateOutreachHistory.objects.filter(
                outreach_log=outreach_log,
                contact_id__in=candidate_ids
            ).update(
                response_received=True,
                response_date=timezone.now(),
                response_type=response_type,
                status='responded'
            )
            
            # Cancel any pending follow-up tasks for this outreach
            FollowUpTask.objects.filter(
                outreach_log=outreach_log,
                completed=False
            ).update(
                completed=True,
                completed_at=timezone.now()
            )
            
            logger.info(f"Response marked for outreach log {outreach_log_id}: {response_type}")
            return True
            
        except OutreachLog.DoesNotExist:
            logger.error(f"Outreach log {outreach_log_id} not found")
            return False
        except Exception as e:
            logger.error(f"Error marking response for outreach log {outreach_log_id}: {e}")
            return False


def process_follow_up_workflow() -> Dict[str, Any]:
    """
    Main function to process follow-up workflow
    """
    logger.info("Starting follow-up workflow processing")
    
    workflow = FollowUpWorkflow()
    return workflow.process_pending_follow_ups()
