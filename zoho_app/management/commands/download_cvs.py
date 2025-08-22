import logging
from django.core.management.base import BaseCommand
from zoho.attachments import ZohoAttachmentManager
from zoho_app.models import Contact

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Download CV attachments from Zoho CRM for contacts'

    def add_arguments(self, parser):
        parser.add_argument(
            '--contact-id',
            type=str,
            help='Download CVs for specific contact ID only',
        )
        parser.add_argument(
            '--download-dir',
            type=str,
            default='downloads',
            help='Directory to store downloaded files (default: downloads)',
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit number of contacts to process',
        )

    def handle(self, *args, **options):
        """Download CV attachments"""
        self.stdout.write(
            self.style.SUCCESS("Starting CV download process...")
        )
        
        # Initialize attachment manager
        download_dir = options['download_dir']
        attachment_manager = ZohoAttachmentManager(download_dir=download_dir)
        
        try:
            if options['contact_id']:
                # Download for specific contact
                contact_id = options['contact_id']
                self.stdout.write(f"Downloading CVs for contact {contact_id}...")
                
                try:
                    contact = Contact.objects.get(id=contact_id)
                    contact_name = contact.full_name or contact.email
                except Contact.DoesNotExist:
                    contact_name = None
                    self.stdout.write(
                        self.style.WARNING(f"Contact {contact_id} not found in database, proceeding anyway...")
                    )
                
                downloaded_files = attachment_manager.download_contact_cvs(contact_id, contact_name)
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f" Downloaded {len(downloaded_files)} CV files for contact {contact_id}"
                    )
                )
                
                for file_path in downloaded_files:
                    self.stdout.write(f"  - {file_path}")
                    
            else:
                # Download for all contacts
                contacts_query = Contact.objects.all()
                
                if options['limit']:
                    contacts_query = contacts_query[:options['limit']]
                    self.stdout.write(f"Processing first {options['limit']} contacts...")
                else:
                    self.stdout.write("Processing all contacts...")
                
                contacts = list(contacts_query)
                total_contacts = len(contacts)
                total_files_downloaded = 0
                contacts_with_cvs = 0
                
                for i, contact in enumerate(contacts, 1):
                    try:
                        self.stdout.write(f"Processing contact {i}/{total_contacts}: {contact.id}")
                        
                        contact_name = contact.full_name or contact.email
                        downloaded_files = attachment_manager.download_contact_cvs(contact.id, contact_name)
                        
                        if downloaded_files:
                            contacts_with_cvs += 1
                            total_files_downloaded += len(downloaded_files)
                            self.stdout.write(
                                f"   Downloaded {len(downloaded_files)} CV files"
                            )
                        else:
                            self.stdout.write("  ℹ️ No CV files found")
                        
                        # Progress update every 10 contacts
                        if i % 10 == 0:
                            self.stdout.write(
                                f"Progress: {i}/{total_contacts} contacts processed, "
                                f"{total_files_downloaded} files downloaded"
                            )
                            
                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(f"  ❌ Error processing contact {contact.id}: {e}")
                        )
                        continue
                
                # Final summary
                self.stdout.write("\n" + "="*60)
                self.stdout.write(self.style.SUCCESS("CV DOWNLOAD SUMMARY"))
                self.stdout.write("="*60)
                self.stdout.write(f"Total contacts processed: {total_contacts}")
                self.stdout.write(f"Contacts with CVs: {contacts_with_cvs}")
                self.stdout.write(f"Total CV files downloaded: {total_files_downloaded}")
                self.stdout.write(f"Download directory: {download_dir}")
                
                if total_contacts > 0:
                    cv_rate = (contacts_with_cvs / total_contacts) * 100
                    self.stdout.write(f"CV availability rate: {cv_rate:.1f}%")
                
            self.stdout.write(
                self.style.SUCCESS("🎉 CV download process completed successfully!")
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ CV download failed: {str(e)}")
            )
            logger.error(f"CV download failed: {str(e)}")
            raise
