#!/usr/bin/env python3
"""
Django management command to run enhanced job matching
Usage: python manage.py run_enhanced_matching [contact_id] [--min-score 0.2]
"""

import logging
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from zoho_app.models import Contact
from etl.job_matcher import match_jobs_for_contact, batch_match_jobs_for_contacts

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run enhanced job matching for contacts'

    def add_arguments(self, parser):
        parser.add_argument(
            'contact_id',
            nargs='?',
            type=str,
            help='Specific contact ID to match (optional - if not provided, matches all Ready to Pitch contacts)'
        )
        parser.add_argument(
            '--min-score',
            type=float,
            default=0.2,
            help='Minimum match score threshold (default: 0.2)'
        )
        parser.add_argument(
            '--ready-to-pitch-only',
            action='store_true',
            help='Only match contacts with role_success_stage = "Ready to Pitch"'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=50,
            help='Batch size for processing multiple contacts (default: 50)'
        )

    def handle(self, *args, **options):
        contact_id = options.get('contact_id')
        min_score = options['min_score']
        ready_to_pitch_only = options['ready_to_pitch_only']
        batch_size = options['batch_size']

        self.stdout.write("🚀 Starting Enhanced Job Matching...")
        self.stdout.write(f"   Min Score Threshold: {min_score}")

        try:
            if contact_id:
                # Match single contact
                self.match_single_contact(contact_id, min_score)
            else:
                # Match multiple contacts
                self.match_multiple_contacts(min_score, ready_to_pitch_only, batch_size)

        except Exception as e:
            raise CommandError(f"Job matching failed: {str(e)}")

    def match_single_contact(self, contact_id: str, min_score: float):
        """Match jobs for a single contact"""
        self.stdout.write(f"🎯 Matching jobs for contact: {contact_id}")
        
        try:
            # Check if contact exists
            contact = Contact.objects.get(id=contact_id)
            self.stdout.write(f"   Contact: {contact.full_name or contact.email or contact_id}")
            
            # Run matching
            result = match_jobs_for_contact(contact_id, min_score)
            
            if result['status'] == 'success':
                self.stdout.write(f" Matching completed successfully!")
                self.stdout.write(f"   Total potential matches: {result['total_matches']}")
                self.stdout.write(f"   Quality matches (>= {min_score}): {result['quality_matches']}")
                self.stdout.write(f"   Matches created: {result['matches_created']}")
                
                # Show top matches
                if result.get('top_matches'):
                    self.stdout.write(f"\n🏆 Top Matches:")
                    for i, match in enumerate(result['top_matches'][:3], 1):
                        self.stdout.write(f"   {i}. Role {match['intern_role_id']} - Score: {match['match_score']:.2f}")
                        if match.get('match_reason'):
                            self.stdout.write(f"      Reason: {match['match_reason'][:80]}...")
            else:
                self.stdout.write(f"❌ Matching failed: {result.get('error', 'Unknown error')}")
                
        except Contact.DoesNotExist:
            raise CommandError(f"Contact {contact_id} not found")

    def match_multiple_contacts(self, min_score: float, ready_to_pitch_only: bool, batch_size: int):
        """Match jobs for multiple contacts"""
        
        # Build query
        query = Q()
        if ready_to_pitch_only:
            query &= Q(role_success_stage='Ready to Pitch')
            self.stdout.write("🎯 Matching 'Ready to Pitch' contacts only")
        else:
            self.stdout.write("🎯 Matching all contacts")
        
        # Get contacts
        contacts = Contact.objects.filter(query)
        total_contacts = contacts.count()
        
        if total_contacts == 0:
            self.stdout.write("⚠ No contacts found to match")
            return
        
        self.stdout.write(f"   Found {total_contacts} contacts to process")
        
        # Process in batches
        processed = 0
        total_matches_created = 0
        successful_contacts = 0
        failed_contacts = 0
        
        for start_idx in range(0, total_contacts, batch_size):
            end_idx = min(start_idx + batch_size, total_contacts)
            batch_contacts = contacts[start_idx:end_idx]
            
            self.stdout.write(f"\n📦 Processing batch {start_idx//batch_size + 1}: contacts {start_idx + 1}-{end_idx}")
            
            # Get contact IDs for batch
            contact_ids = [contact.id for contact in batch_contacts]
            
            # Run batch matching
            batch_result = batch_match_jobs_for_contacts(contact_ids, min_score)
            
            # Update counters
            processed += len(contact_ids)
            successful_contacts += batch_result['successful_matches']
            failed_contacts += batch_result['failed_matches']
            total_matches_created += batch_result['total_matches_created']
            
            # Show batch results
            self.stdout.write(f"    Successful: {batch_result['successful_matches']}")
            self.stdout.write(f"   ❌ Failed: {batch_result['failed_matches']}")
            self.stdout.write(f"   🎲 Matches created: {batch_result['total_matches_created']}")
            
            # Show errors if any
            if batch_result['errors']:
                self.stdout.write(f"   ⚠ Errors:")
                for error in batch_result['errors'][:3]:  # Show first 3 errors
                    self.stdout.write(f"     - {error}")
                if len(batch_result['errors']) > 3:
                    self.stdout.write(f"     ... and {len(batch_result['errors']) - 3} more errors")
        
        # Final summary
        self.stdout.write(f"\n🎉 Enhanced Job Matching Complete!")
        self.stdout.write(f"   📊 Contacts processed: {processed}")
        self.stdout.write(f"    Successful: {successful_contacts}")
        self.stdout.write(f"   ❌ Failed: {failed_contacts}")
        self.stdout.write(f"   🎲 Total matches created: {total_matches_created}")
        self.stdout.write(f"   📈 Average matches per successful contact: {total_matches_created / max(successful_contacts, 1):.1f}")
        
        if successful_contacts > 0:
            success_rate = (successful_contacts / processed) * 100
            self.stdout.write(f"   📊 Success rate: {success_rate:.1f}%")
