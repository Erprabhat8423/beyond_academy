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

            # Handle dry run mode
            if results.get('status') == 'dry_run_completed':
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Dry run completed: Found {results.get('roles_found', 0)} roles "
                        f"with {results.get('total_candidates', 0)} total candidates"
                    )
                )
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
                # Also show urgent candidates if present
                if results.get('urgent_roles_found', 0) > 0:
                    self.stdout.write(self.style.NOTICE(f"Urgent roles found: {results['urgent_roles_found']} with {results['urgent_total_candidates']} urgent candidates"))
                    for role_id, candidates in results.get('urgent_role_candidates', {}).items():
                        self.stdout.write(f"  URGENT Role {role_id}: {len(candidates)} candidates")
                        for candidate in candidates:
                            self.stdout.write(f"    - {candidate['full_name']} (Score: {candidate['match_score']:.2f}) URGENT")

            # Handle normal/urgent outreach (not dry run)
            elif 'normal_outreach' in results or 'urgent_outreach' in results:
                # Normal outreach
                normal = results.get('normal_outreach', {})
                urgent = results.get('urgent_outreach', {})

                # Normal
                self.stdout.write(self.style.SUCCESS("Normal Outreach Results:"))
                if normal.get('status') == 'completed':
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  {normal.get('successful', 0)}/{normal.get('roles_processed', 0)} roles processed successfully"
                        )
                    )
                    for result in normal.get('results', []):
                        if result.get('status') == 'success':
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f"    ✓ Role {result['role_id']}: {result['candidates_count']} candidates "
                                    f"sent to {result['recipients_count']} recipients"
                                )
                            )
                        else:
                            self.stdout.write(
                                self.style.WARNING(
                                    f"    ✗ Role {result.get('role_id', '?')}: {result.get('reason', 'Unknown error')}"
                                )
                            )
                elif normal.get('status') == 'failed':
                    self.stdout.write(self.style.ERROR(f"  Outreach failed: {normal.get('error')}"))
                else:
                    self.stdout.write(self.style.WARNING("  No normal candidates found for outreach."))

                # Urgent
                self.stdout.write(self.style.SUCCESS("Urgent Outreach Results:"))
                if urgent.get('status') == 'completed':
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  {urgent.get('successful', 0)}/{urgent.get('roles_processed', 0)} urgent roles processed successfully"
                        )
                    )
                    for result in urgent.get('results', []):
                        if result.get('status') == 'success':
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f"    ✓ URGENT Role {result['role_id']}: {result['candidates_count']} candidates "
                                    f"sent to {result['recipients_count']} recipients (URGENT)"
                                )
                            )
                        else:
                            self.stdout.write(
                                self.style.WARNING(
                                    f"    ✗ URGENT Role {result.get('role_id', '?')}: {result.get('reason', 'Unknown error')}"
                                )
                            )
                elif urgent.get('status') == 'failed':
                    self.stdout.write(self.style.ERROR(f"  Urgent outreach failed: {urgent.get('error')}"))
                else:
                    self.stdout.write(self.style.WARNING("  No urgent candidates found for outreach."))

            # Handle top-level failed
            elif results.get('status') == 'failed':
                self.stdout.write(
                    self.style.ERROR(f"Outreach automation failed: {results.get('error')}")
                )

            else:
                self.stdout.write(self.style.WARNING("No outreach results to display."))

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error running outreach automation: {e}')
            )
            logger.error(f"Management command error: {e}")
