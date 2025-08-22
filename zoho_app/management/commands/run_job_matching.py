import logging
from django.core.management.base import BaseCommand
from django.utils import timezone
from etl.job_matcher import JobMatcher
from zoho_app.models import Contact, InternRole, JobMatch, Skill

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run job matching pipeline to match contacts with intern roles'

    def add_arguments(self, parser):
        parser.add_argument(
            '--contact-id',
            type=str,
            help='Run matching for specific contact ID only',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=10,
            help='Limit number of matches to display per contact',
        )
        parser.add_argument(
            '--show-stats',
            action='store_true',
            help='Show detailed statistics after matching',
        )
        parser.add_argument(
            '--clean-old-matches',
            action='store_true',
            help='Clean old matches before running new matching',
        )

    def handle(self, *args, **options):
        """Run job matching pipeline"""
        start_time = timezone.now()
        self.stdout.write(
            self.style.SUCCESS(f"Starting Job Matching Pipeline at {start_time}...")
        )
        
        matcher = JobMatcher()
        
        try:
            # Check prerequisites
            self.stdout.write("Checking prerequisites...")
            
            contact_count = Contact.objects.count()
            role_count = InternRole.objects.count()
            skill_count = Skill.objects.count()
            
            if contact_count == 0:
                self.stdout.write(
                    self.style.ERROR("❌ No contacts found. Please run ETL first.")
                )
                return
            
            if role_count == 0:
                self.stdout.write(
                    self.style.ERROR("❌ No intern roles found. Please run ETL first.")
                )
                return
                
            self.stdout.write(
                f"Found {contact_count} contacts, {role_count} intern roles, {skill_count} extracted skills"
            )
            
            # Clean old matches if requested
            if options['clean_old_matches']:
                self.stdout.write("Cleaning up old matches...")
                old_count = JobMatch.objects.count()
                JobMatch.objects.all().delete()
                self.stdout.write(f"Deleted {old_count} old matches")
            
            # Run matching
            if options['contact_id']:
                # Match specific contact
                contact_id = options['contact_id']
                self.stdout.write(f"Running job matching for contact {contact_id}...")
                
                result = matcher.process_contact_matches(contact_id)
                
                if result['status'] == 'success':
                    self.stdout.write(
                        self.style.SUCCESS(
                            f" Matching completed for contact {contact_id}: "
                            f"{result['total_matches']} matches found, "
                            f"{result['stored_matches']} stored"
                        )
                    )
                    
                    # Show top matches
                    matches = matcher.get_matches_for_contact(contact_id, options['limit'])
                    if matches:
                        self.stdout.write(f"\nTop {len(matches)} matches:")
                        for i, match in enumerate(matches, 1):
                            self.stdout.write(
                                f"{i}. Score: {match.match_score:.3f} - "
                                f"Role: {match.intern_role_id} - "
                                f"Reason: {match.match_reason}"
                            )
                else:
                    self.stdout.write(
                        self.style.ERROR(f"❌ Matching failed for contact {contact_id}: {result.get('error')}")
                    )
                    
            else:
                # Match all contacts
                self.stdout.write("Running job matching for all contacts...")
                
                results = matcher.process_all_contacts()
                
                end_time = timezone.now()
                processing_time = (end_time - start_time).total_seconds()
                
                self.stdout.write("\n" + "="*60)
                self.stdout.write(self.style.SUCCESS("JOB MATCHING PIPELINE RESULTS"))
                self.stdout.write("="*60)
                self.stdout.write(f"Processing time: {processing_time:.2f} seconds")
                self.stdout.write(f"Total contacts processed: {results['total_contacts']}")
                self.stdout.write(f"Contacts with matches: {results['contacts_with_matches']}")
                self.stdout.write(f"Total matches created: {results['total_matches']}")
                
                if results['total_contacts'] > 0:
                    match_rate = (results['contacts_with_matches'] / results['total_contacts']) * 100
                    self.stdout.write(f"Match rate: {match_rate:.1f}%")
                
                # Show statistics if requested
                if options['show_stats']:
                    self.show_detailed_stats(options['limit'])
                
            self.stdout.write(
                self.style.SUCCESS("🎉 Job Matching Pipeline completed successfully!")
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Job matching failed: {str(e)}")
            )
            logger.error(f"Job matching failed: {str(e)}")
            raise
    
    def show_detailed_stats(self, limit=10):
        """Show detailed matching statistics"""
        try:
            self.stdout.write("\n" + "="*60)
            self.stdout.write("DETAILED STATISTICS")
            self.stdout.write("="*60)
            
            # Top matches by score
            self.stdout.write(f"\nTop {limit} Matches by Score:")
            top_matches = JobMatch.objects.filter(
                status='active'
            ).order_by('-match_score')[:limit]
            
            for i, match in enumerate(top_matches, 1):
                try:
                    contact = Contact.objects.get(id=match.contact_id)
                    role = InternRole.objects.get(id=match.intern_role_id)
                    
                    self.stdout.write(
                        f"{i:2d}. Score: {match.match_score:.3f} | "
                        f"Contact: {contact.full_name or contact.email or match.contact_id} | "
                        f"Role: {role.role_title or role.name or match.intern_role_id}"
                    )
                except (Contact.DoesNotExist, InternRole.DoesNotExist):
                    self.stdout.write(
                        f"{i:2d}. Score: {match.match_score:.3f} | "
                        f"Contact: {match.contact_id} | "
                        f"Role: {match.intern_role_id}"
                    )
            
            # Match statistics
            total_active = JobMatch.objects.filter(status='active').count()
            industry_matches = JobMatch.objects.filter(
                status='active', industry_match=True
            ).count()
            location_matches = JobMatch.objects.filter(
                status='active', location_match=True
            ).count()
            policy_matches = JobMatch.objects.filter(
                status='active', work_policy_match=True
            ).count()
            skill_matches = JobMatch.objects.filter(
                status='active', skill_match=True
            ).count()
            
            self.stdout.write(f"\nMatch Statistics:")
            self.stdout.write(f"Total active matches: {total_active}")
            
            if total_active > 0:
                self.stdout.write(f"Industry matches: {industry_matches} ({industry_matches/total_active*100:.1f}%)")
                self.stdout.write(f"Location matches: {location_matches} ({location_matches/total_active*100:.1f}%)")
                self.stdout.write(f"Work policy matches: {policy_matches} ({policy_matches/total_active*100:.1f}%)")
                self.stdout.write(f"Skill matches: {skill_matches} ({skill_matches/total_active*100:.1f}%)")
            
        except Exception as e:
            self.stdout.write(
                self.style.WARNING(f"Error showing detailed stats: {e}")
            )
