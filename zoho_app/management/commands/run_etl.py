import logging
from django.core.management.base import BaseCommand
from django.utils import timezone
from etl.pipeline import sync_contacts, sync_accounts, sync_intern_roles, sync_deals, sync_contact_deals

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run ETL pipeline to sync data from Zoho CRM'

    def add_arguments(self, parser):
        parser.add_argument(
            '--full',
            action='store_true',
            help='Force full sync instead of incremental sync',
        )
        parser.add_argument(
            '--contacts-only',
            action='store_true',
            help='Sync only contacts',
        )
        parser.add_argument(
            '--accounts-only',
            action='store_true',
            help='Sync only accounts',
        )
        parser.add_argument(
            '--intern-roles-only',
            action='store_true',
            help='Sync only intern roles',
        )
        parser.add_argument(
            '--deals-only',
            action='store_true',
            help='Sync only deals',
        )
        parser.add_argument(
            '--contact-deals-only',
            action='store_true',
            help='Sync only contact deals',
        )

    def handle(self, *args, **options):
        """Main ETL execution function"""
        start_time = timezone.now()
        self.stdout.write(
            self.style.SUCCESS(f"Starting ETL job at {start_time}...")
        )
        
        incremental = not options['full']
        sync_type = "incremental" if incremental else "full"
        
        try:
            if options['contacts_only']:
                self.stdout.write("Syncing contacts only...")
                sync_contacts(incremental=incremental)
                self.stdout.write(
                    self.style.SUCCESS(" Contacts sync completed successfully")
                )
                
            elif options['accounts_only']:
                self.stdout.write("Syncing accounts only...")
                sync_accounts(incremental=incremental)
                self.stdout.write(
                    self.style.SUCCESS(" Accounts sync completed successfully")
                )
                
            elif options['intern_roles_only']:
                self.stdout.write("Syncing intern roles only...")
                sync_intern_roles(incremental=incremental)
                self.stdout.write(
                    self.style.SUCCESS(" Intern roles sync completed successfully")
                )
                
            elif options['deals_only']:
                self.stdout.write("Syncing deals only...")
                sync_deals(incremental=incremental)
                self.stdout.write(
                    self.style.SUCCESS(" Deals sync completed successfully")
                )
                
            elif options['contact_deals_only']:
                self.stdout.write("Syncing contact deals only...")
                sync_contact_deals()
                self.stdout.write(
                    self.style.SUCCESS(" Contact deals sync completed successfully")
                )
                
            else:
                # Run full pipeline
                self.stdout.write(f"Running {sync_type} sync for all entities...")
                
                self.stdout.write("Step 1: Syncing contacts...")
                sync_contacts(incremental=incremental)
                self.stdout.write(self.style.SUCCESS(" Contacts sync completed"))
                
                self.stdout.write("Step 2: Syncing accounts...")
                sync_accounts(incremental=incremental)
                self.stdout.write(self.style.SUCCESS(" Accounts sync completed"))
                
                self.stdout.write("Step 3: Syncing intern roles...")
                sync_intern_roles(incremental=incremental)
                self.stdout.write(self.style.SUCCESS(" Intern roles sync completed"))
                
                self.stdout.write("Step 4: Syncing deals...")
                sync_deals(incremental=incremental)
                self.stdout.write(self.style.SUCCESS(" Deals sync completed"))
                
                self.stdout.write("Step 5: Syncing contact deals...")
                sync_contact_deals()
                self.stdout.write(self.style.SUCCESS(" Contact deals sync completed"))
                
            end_time = timezone.now()
            duration = end_time - start_time
            
            self.stdout.write(
                self.style.SUCCESS(f"🎉 ETL job completed successfully in {duration}")
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ ETL job failed: {str(e)}")
            )
            logger.error(f"ETL job failed: {str(e)}")
            raise
