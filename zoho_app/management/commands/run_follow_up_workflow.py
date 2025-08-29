"""
Django management command for running follow-up workflow
"""

from django.core.management.base import BaseCommand
from zoho_app.follow_up_workflow import process_follow_up_workflow
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Process follow-up workflow for outreach emails (run every few hours via cron)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Enable verbose logging',
        )

    def handle(self, *args, **options):
        if options['verbose']:
            logging.basicConfig(level=logging.INFO)
        
        self.stdout.write(
            self.style.SUCCESS('Starting follow-up workflow processing')
        )
        
        try:
            results = process_follow_up_workflow()
            
            if results.get('status') == 'error':
                self.stdout.write(
                    self.style.ERROR(f"Follow-up workflow failed: {results.get('error')}")
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Follow-up workflow completed: "
                        f"{results['total_tasks']} tasks processed, "
                        f"{results['follow_up_sent']} follow-ups sent, "
                        f"{results['final_sent']} final emails sent, "
                        f"{results['moved_to_next']} candidates moved to next roles, "
                        f"{results['errors']} errors"
                    )
                )
                
                # Show detailed results if verbose
                if options['verbose']:
                    for result in results.get('task_results', []):
                        if result['status'] == 'success':
                            self.stdout.write(f"  ✓ Task {result['task_id']}: {result.get('email_type', 'moved')}")
                        elif result['status'] == 'skipped':
                            self.stdout.write(f"  - Task {result['task_id']}: skipped ({result.get('reason')})")
                        else:
                            self.stdout.write(f"  ✗ Task {result['task_id']}: {result.get('error')}")
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error running follow-up workflow: {e}')
            )
            logger.error(f"Management command error: {e}")
