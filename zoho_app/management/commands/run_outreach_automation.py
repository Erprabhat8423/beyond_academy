"""
Django management command for running outreach automation
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from zoho_app.outreach_automation import run_outreach_automation
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run outreach automation to send candidate information to companies'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Run in dry-run mode without sending actual emails',
        )
        parser.add_argument(
            '--max-roles',
            type=int,
            help='Maximum number of roles to process (for testing)',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Enable verbose logging',
        )

    def handle(self, *args, **options):
        if options['verbose']:
            logging.basicConfig(level=logging.INFO)
        
        dry_run = options['dry_run']
        max_roles = options['max_roles']
        
        self.stdout.write(
            self.style.SUCCESS(
                f'Starting outreach automation (dry_run={dry_run}, max_roles={max_roles})'
            )
        )
        
        try:
            results = run_outreach_automation(dry_run=dry_run, max_roles=max_roles)
            
            if results['status'] == 'dry_run_completed':
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Dry run completed: Found {results['roles_found']} roles "
                        f"with {results['total_candidates']} total candidates"
                    )
                )
                
                # Show summary of what would be processed
                for role_id, candidates in results.get('role_candidates', {}).items():
                    self.stdout.write(f"  Role {role_id}: {len(candidates)} candidates")
                    for candidate in candidates:
                        urgency = ""
                        if candidate.get('start_date'):
                            start_date = candidate['start_date']
                            if hasattr(start_date, 'date'):
                                start_date = start_date.date()
                            
                            today = timezone.now().date()
                            days_until_start = (start_date - today).days
                            
                            requires_visa = candidate.get('requires_visa', '').lower()
                            if requires_visa == 'yes' and days_until_start < 120:
                                urgency = "URGENT"
                            elif requires_visa == 'no' and days_until_start < 60:
                                urgency = "URGENT"
                        
                        self.stdout.write(
                            f"    - {candidate['full_name']} (Score: {candidate['match_score']:.2f}) {urgency}"
                        )
            
            elif results['status'] == 'completed':
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Outreach automation completed: {results['successful']}/{results['roles_processed']} roles processed successfully"
                    )
                )
                
                # Show detailed results
                for result in results.get('results', []):
                    if result['status'] == 'success':
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"  ✓ Role {result['role_id']}: {result['candidates_count']} candidates "
                                f"sent to {result['recipients_count']} recipients"
                                f"{' (URGENT)' if result.get('is_urgent') else ''}"
                            )
                        )
                    else:
                        self.stdout.write(
                            self.style.WARNING(
                                f"  ✗ Role {result['role_id']}: {result.get('reason', 'Unknown error')}"
                            )
                        )
            
            elif results['status'] == 'failed':
                self.stdout.write(
                    self.style.ERROR(f"Outreach automation failed: {results.get('error')}")
                )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error running outreach automation: {e}')
            )
            logger.error(f"Management command error: {e}")
