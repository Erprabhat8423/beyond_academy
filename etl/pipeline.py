import logging
import json
from datetime import datetime, timezone
from django.utils import timezone as django_timezone
from django.db import transaction
from django.db.utils import IntegrityError

from zoho_app.models import Contact, Account, InternRole, SyncTracker, Deal
from zoho.api_client import ZohoClient

# Configure logging
logger = logging.getLogger(__name__)


def get_sync_tracker(entity_type):
    """Get sync tracker for a specific entity type"""
    try:
        return SyncTracker.objects.get(entity_type=entity_type)
    except SyncTracker.DoesNotExist:
        return SyncTracker.objects.create(
            entity_type=entity_type,
            last_sync_timestamp=None,
            records_synced=0
        )


def update_sync_tracker(entity_type, last_timestamp, records_count):
    """Update sync tracker with latest sync information"""
    try:
        # Ensure timestamp is timezone-aware
        if last_timestamp and hasattr(last_timestamp, 'tzinfo') and last_timestamp.tzinfo is None:
            last_timestamp = last_timestamp.replace(tzinfo=timezone.utc)
        
        tracker, created = SyncTracker.objects.get_or_create(
            entity_type=entity_type,
            defaults={
                'last_sync_timestamp': last_timestamp,
                'records_synced': records_count
            }
        )
        if not created:
            tracker.last_sync_timestamp = last_timestamp
            tracker.records_synced += records_count
            tracker.save()
        
        logger.info(f"Updated sync tracker for {entity_type}: {records_count} records, last_timestamp: {last_timestamp}")
        
    except Exception as e:
        logger.error(f"Error updating sync tracker for {entity_type}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")


def build_incremental_criteria(last_sync_timestamp):
    """Build Zoho API criteria for incremental sync based on Modified_Time"""
    if not last_sync_timestamp:
        return None
    
    # Format timestamp for Zoho API (ISO format)
    # Zoho expects format: YYYY-MM-DDTHH:mm:ss+HH:mm
    if hasattr(last_sync_timestamp, 'tzinfo') and last_sync_timestamp.tzinfo:
        formatted_time = last_sync_timestamp
    else:
        formatted_time = django_timezone.make_aware(last_sync_timestamp)
    
    formatted_time_str = formatted_time.strftime("%Y-%m-%dT%H:%M:%S%z")
    # Add colon to timezone offset for Zoho API compatibility
    if formatted_time_str.endswith('+0000'):
        formatted_time_str = formatted_time_str[:-5] + '+00:00'
    
    criteria = f"(Modified_Time:greater_than:{formatted_time_str})"
    logger.info(f"Built incremental criteria: {criteria}")
    return criteria


def get_latest_modified_time(records):
    """Get the latest Modified_Time from a list of records"""
    if not records:
        return None
    
    latest_time = None
    for record in records:
        modified_time_str = record.get('Modified_Time')
        if modified_time_str:
            try:
                # Parse Zoho timestamp format
                modified_time = datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
                if latest_time is None or modified_time > latest_time:
                    latest_time = modified_time
            except ValueError:
                continue
    
    return latest_time


def parse_datetime_field(date_str):
    """Parse datetime string from Zoho API with timezone awareness"""
    if not date_str:
        return None
    try:
        # Handle ISO format with Z
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'
        
        parsed_dt = datetime.fromisoformat(date_str)
        
        # If datetime is naive (no timezone), make it timezone-aware
        if parsed_dt.tzinfo is None:
            # Assume UTC for naive datetimes from Zoho
            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
        
        return parsed_dt
    except ValueError:
        return None


def list_to_json_string(lst):
    """Convert list to JSON string, return None if empty"""
    if not lst:
        return None
    try:
        return json.dumps(lst)
    except (TypeError, ValueError):
        return str(lst) if lst else None


def extract_nested_id(obj):
    """Extract ID from nested object"""
    if isinstance(obj, dict):
        return obj.get('id')
    return None


def extract_nested_name(obj):
    """Extract name from nested object"""
    if isinstance(obj, dict):
        return obj.get('name')
    return str(obj) if obj else None


def extract_nested_email(obj):
    """Extract email from nested object"""
    if isinstance(obj, dict):
        return obj.get('email')
    return None


def sync_contacts(incremental=True):
    """Sync contacts from Zoho CRM to Django database"""
    logger.info("Starting contact sync...")
    zoho = ZohoClient()
    
    # Determine sync criteria
    criteria = None
    last_sync_info = ""
    
    if incremental:
        tracker = get_sync_tracker('contacts')
        if tracker.last_sync_timestamp:
            criteria = build_incremental_criteria(tracker.last_sync_timestamp)
            last_sync_info = f" (incremental since {tracker.last_sync_timestamp})"
        else:
            last_sync_info = " (full sync - no previous sync found)"
    else:
        last_sync_info = " (full sync - forced)"
    
    logger.info(f"Getting contact data from Zoho{last_sync_info}...")
    
    # Define fields to fetch - comprehensive list including all model fields
    # Based on your working ETL code, using the exact field names that Zoho API provides
    contact_fields = [
        # Core fields
        'id', 'Email', 'First_Name', 'Last_Name', 'Phone', 'Account_Name',
        'Title', 'Department', 'Modified_Time', 'Created_Time', 'Full_Name',
        
        # Location and Industry fields
        'Location', 'Industry', 'Industry_Choice_1', 'Industry_choice_2', 'Industry_Choice_3',
        'Industry_1_Areas', 'Industry_2_Areas', 'Current_Location_V2', 'Location_Other',
        'Alternative_Location1', 'Country_city_of_residence',
        
        # Student and Academic fields
        'Skills', 'Student_Status', 'University_Name', 'Graduation_Date', 'Student_Bio',
        'Uni_Start_Date', 'English_Level', 'Age_on_Start_Date', 'Date_of_Birth',
        
        # Placement and Role fields
        'Placement_status', 'Start_date', 'End_date', 'Role_Success_Stage',
        'Role_Owner', 'Role_Success_Notes', 'Role_confirmed_date', 'Paid_Role',
        'Likelihood_to_convert', 'Job_Title', 'Job_offered_after',
        
        # Contact and Communication fields
        'Link_to_CV', 'Contact_Email', 'Secondary_Email', 'Do_Not_Contact',
        'Email_Opt_Out', 'Unsubscribed_Time', 'Follow_up_Date',
        
        # Personal Information
        'Gender', 'Nationality', 'Timezone', 'Contact_Last_Name',
        
        # Visa and Travel fields
        'Visa_Eligible', 'Requires_a_visa', 'Visa_Type_Exemption', 'Visa_successful',
        'Visa_Alt_Options', 'Visa_Note_s', 'Visa_Owner', 'Visa_F_U_Date',
        'Arrival_date_time', 'Departure_date_time', 'Departure_flight_number',
        'Arrival_drop_off_address',
        
        # Interview and Assessment fields
        'Interview', 'Interview_successful', 'Interviewer', 'MyInterview_URL',
        'Intro_Call_Date', 'Call_Scheduled_Date_Time', 'Call_Booked_Date_Time',
        'Call_to_Conversion_Time_days', 'Enrolment_to_Intro_Call_Lead_Time',
        
        # Approval and Process fields (with $ prefixes for system fields)
        '$approval', 'Approval_date', '$approval_state', '$process_flow',
        '$review', '$review_process', 'Student_decision', 'Company_decision',
        
        # Administrative fields (with proper field names and $ prefixes)
        'Layout', '$field_states', 'Record_Status__s', 'Last_Activity_Time', 
        'Last_Enriched_Time__s', 'Lead_Created_Time', 'Change_Log_Time__s', 'Created_By',
        
        # Partnership and Organization fields
        'Partner_Organisation', 'From_University_partner', 'Community_Owner',
        'Admission_Member', 'PS_Assigned_Date',
        
        # Accommodation fields
        'Accommodation_finalised', 'House_rules',
        
        # Financial and Agreement fields
        'Signed_Agreement', 'Agreement_finalised', 'books_cust_id',
        'Other_Payment_Status', 'Total', 'T_C_Link', 'Send_Mail2',
        
        # Duration and Timeline fields
        'Duration', 'Number_of_Days', 'Days_Count', 'Days_Since_Conversion',
        'Average_no_of_days', 'Placement_Lead_Time_days', 'Placement_Deadline',
        'Placement_Urgency', 'Decision_Date', 'Cohort_Start_Date',
        
        # Cancellation and Issues
        'Reason_for_Cancellation', 'Cancellation_Notes', 'Cancelled_Date_Time',
        'Date_of_Cancellation', 'Refund_date',
        
        # Rating and Feedback
        'Rating', 'Rating_New', 'Warm_Call',
        
        # Marketing and UTM fields
        'UTM_Campaign', 'UTM_Medium', 'UTM_Content', 'UTM_GCLID',
        
        # Other fields (with proper field names and system prefixes)
        'Description', 'Additional_Information', 'Notes1', 'Name1',
        'Other_industry', 'Token', 'Tag', 'Type', 'Enrich_Status__s',
        '$is_duplicate', 'Locked__s', '$locked_for_me', '$in_merge',
        
        # Missing fields from your working ETL that we should include
        'End_date_Auto_populated', 'Modified_By'
    ]
    
    try:
        # Fetch data from Zoho
        zoho_contacts = zoho.get_paginated_data(
            module='Contacts',
            fields=contact_fields,
            criteria=criteria,
            sort_by='Modified_Time',
            sort_order='asc'
        )
        
        logger.info(f"Retrieved {len(zoho_contacts)} contacts from Zoho")
        
        if not zoho_contacts:
            logger.info("No contacts to sync")
            return
        
        # Process contacts
        synced_count = 0
        latest_modified = None
        
        for contact_data in zoho_contacts:
            try:
                with transaction.atomic():
                    # Parse and prepare contact data - comprehensive mapping using correct Zoho field names
                    contact_fields_mapped = {
                        # Core fields
                        'id': contact_data.get('id'),
                        'email': contact_data.get('Email'),
                        'first_name': contact_data.get('First_Name'),
                        'last_name': contact_data.get('Last_Name'),
                        'phone': contact_data.get('Phone'),
                        'account_name': extract_nested_name(contact_data.get('Account_Name')),
                        'title': contact_data.get('Title'),
                        'department': contact_data.get('Department'),
                        'updated_time': parse_datetime_field(contact_data.get('Modified_Time')),
                        'created_time': parse_datetime_field(contact_data.get('Created_Time')),
                        'full_name': contact_data.get('Full_Name'),
                        
                        # Location and Industry fields
                        'location': contact_data.get('Location'),
                        'industry': contact_data.get('Industry'),
                        'industry_choice_1': contact_data.get('Industry_Choice_1'),
                        'industry_choice_2': contact_data.get('Industry_choice_2'),  # Note: lowercase 'choice'
                        'industry_choice_3': contact_data.get('Industry_Choice_3'),
                        'industry_1_areas': contact_data.get('Industry_1_Areas'),
                        'industry_2_areas': contact_data.get('Industry_2_Areas'),
                        'current_location_v2': contact_data.get('Current_Location_V2'),
                        'location_other': contact_data.get('Location_Other'),
                        'alternative_location1': contact_data.get('Alternative_Location1'),
                        'country_city_of_residence': contact_data.get('Country_city_of_residence'),
                        
                        # Student and Academic fields
                        'skills': contact_data.get('Skills'),
                        'student_status': contact_data.get('Student_Status'),
                        'university_name': contact_data.get('University_Name'),
                        'graduation_date': parse_datetime_field(contact_data.get('Graduation_Date')),
                        'student_bio': contact_data.get('Student_Bio'),
                        'uni_start_date': parse_datetime_field(contact_data.get('Uni_Start_Date')),
                        'english_level': contact_data.get('English_Level'),
                        'age_on_start_date': contact_data.get('Age_on_Start_Date'),
                        'date_of_birth': parse_datetime_field(contact_data.get('Date_of_Birth')),
                        
                        # Placement and Role fields
                        'placement_status': contact_data.get('Placement_status'),  # Note: lowercase 'status'
                        'start_date': parse_datetime_field(contact_data.get('Start_date')),  # Note: lowercase 'date'
                        'end_date': parse_datetime_field(contact_data.get('End_date')),  # Note: lowercase 'date'
                        'role_success_stage': contact_data.get('Role_Success_Stage'),
                        'role_owner': extract_nested_name(contact_data.get('Role_Owner')),
                        'role_success_notes': contact_data.get('Role_Success_Notes'),
                        'role_confirmed_date': parse_datetime_field(contact_data.get('Role_confirmed_date')),  # Note: lowercase
                        'paid_role': contact_data.get('Paid_Role'),
                        'likelihood_to_convert': contact_data.get('Likelihood_to_convert'),  # Note: lowercase
                        'job_title': contact_data.get('Job_Title'),
                        'job_offered_after': contact_data.get('Job_offered_after'),  # Note: lowercase
                        
                        # Contact and Communication fields
                        'link_to_cv': contact_data.get('Link_to_CV'),
                        'contact_email': contact_data.get('Contact_Email'),
                        'secondary_email': contact_data.get('Secondary_Email'),
                        'do_not_contact': contact_data.get('Do_Not_Contact', False),
                        'email_opt_out': contact_data.get('Email_Opt_Out', False),
                        'unsubscribed_time': parse_datetime_field(contact_data.get('Unsubscribed_Time')),
                        'follow_up_date': parse_datetime_field(contact_data.get('Follow_up_Date')),  # Note: lowercase 'up'
                        
                        # Personal Information
                        'gender': contact_data.get('Gender'),
                        'nationality': contact_data.get('Nationality'),
                        'timezone': contact_data.get('Timezone'),
                        'contact_last_name': contact_data.get('Contact_Last_Name'),
                        
                        # Visa and Travel fields
                        'visa_eligible': contact_data.get('Visa_Eligible'),
                        'requires_a_visa': contact_data.get('Requires_a_visa'),
                        'visa_type_exemption': contact_data.get('Visa_Type_Exemption'),
                        'visa_successful': contact_data.get('Visa_successful'),  # Note: lowercase
                        'visa_alt_options': list_to_json_string(contact_data.get('Visa_Alt_Options')),
                        'visa_notes': contact_data.get('Visa_Note_s'),  # Note: different field name
                        'visa_owner': extract_nested_name(contact_data.get('Visa_Owner')),
                        'visa_f_u_date': parse_datetime_field(contact_data.get('Visa_F_U_Date')),
                        'arrival_date_time': parse_datetime_field(contact_data.get('Arrival_date_time')),  # Note: lowercase
                        'departure_date_time': parse_datetime_field(contact_data.get('Departure_date_time')),  # Note: lowercase
                        'departure_flight_number': contact_data.get('Departure_flight_number'),  # Note: lowercase
                        'arrival_drop_off_address': contact_data.get('Arrival_drop_off_address'),  # Note: lowercase
                        
                        # Interview and Assessment fields
                        'interview': contact_data.get('Interview'),
                        'interview_successful': contact_data.get('Interview_successful'),
                        'interviewer': extract_nested_name(contact_data.get('Interviewer')),
                        'myinterview_url': contact_data.get('MyInterview_URL'),
                        'intro_call_date': parse_datetime_field(contact_data.get('Intro_Call_Date')),
                        'call_scheduled_date_time': parse_datetime_field(contact_data.get('Call_Scheduled_Date_Time')),
                        'call_booked_date_time': parse_datetime_field(contact_data.get('Call_Booked_Date_Time')),
                        'call_to_conversion_time_days': contact_data.get('Call_to_Conversion_Time_days'),  # Note: lowercase 'days'
                        'enrolment_to_intro_call_lead_time': contact_data.get('Enrolment_to_Intro_Call_Lead_Time'),
                        
                        # Approval and Process fields (using $ prefixes for system fields)
                        'approval': json.dumps(contact_data.get('$approval')) if contact_data.get('$approval') else None,
                        'approval_date': parse_datetime_field(contact_data.get('Approval_date')),  # Note: lowercase 'date'
                        'approval_state': contact_data.get('$approval_state'),
                        'process_flow': contact_data.get('$process_flow', False),
                        'review': json.dumps(contact_data.get('$review')) if contact_data.get('$review') else None,
                        'review_process': json.dumps(contact_data.get('$review_process')) if contact_data.get('$review_process') else None,
                        'student_decision': contact_data.get('Student_decision'),  # Note: lowercase 'decision'
                        'company_decision': contact_data.get('Company_decision'),  # Note: lowercase 'decision'
                        
                        # Administrative fields (using proper field names with suffixes)
                        'layout_id': extract_nested_id(contact_data.get('Layout')),
                        'layout_display_label': contact_data.get('Layout', {}).get('display_label') if contact_data.get('Layout') else None,
                        'layout_name': contact_data.get('Layout', {}).get('name') if contact_data.get('Layout') else None,
                        'field_states': json.dumps(contact_data.get('$field_states')) if contact_data.get('$field_states') else None,
                        'record_status': contact_data.get('Record_Status__s'),  # Note: __s suffix
                        'last_activity_time': parse_datetime_field(contact_data.get('Last_Activity_Time')),
                        'last_enriched_time': parse_datetime_field(contact_data.get('Last_Enriched_Time__s')),  # Note: __s suffix
                        'lead_created_time': parse_datetime_field(contact_data.get('Lead_Created_Time')),
                        'change_log_time': parse_datetime_field(contact_data.get('Change_Log_Time__s')),  # Note: __s suffix
                        'created_by_email': extract_nested_email(contact_data.get('Created_By')),
                        
                        # Partnership and Organization fields
                        'partner_organisation': contact_data.get('Partner_Organisation'),
                        'from_university_partner': contact_data.get('From_University_partner'),  # Note: lowercase 'partner'
                        'community_owner': extract_nested_name(contact_data.get('Community_Owner')),
                        'admission_member': contact_data.get('Admission_Member'),
                        'ps_assigned_date': parse_datetime_field(contact_data.get('PS_Assigned_Date')),
                        
                        # Accommodation fields
                        'accommodation_finalised': contact_data.get('Accommodation_finalised'),
                        'house_rules': contact_data.get('House_rules'),  # Note: lowercase 'rules'
                        
                        # Financial and Agreement fields
                        'signed_agreement': contact_data.get('Signed_Agreement'),
                        'agreement_finalised': contact_data.get('Agreement_finalised'),
                        'books_cust_id': contact_data.get('books_cust_id'),  # Note: lowercase
                        'other_payment_status': contact_data.get('Other_Payment_Status'),
                        'total': contact_data.get('Total'),
                        't_c_link': contact_data.get('T_C_Link'),
                        'send_mail2': contact_data.get('Send_Mail2', False),
                        
                        # Duration and Timeline fields
                        'duration': contact_data.get('Duration'),
                        'number_of_days': contact_data.get('Number_of_Days'),
                        'days_count': contact_data.get('Days_Count'),
                        'days_since_conversion': contact_data.get('Days_Since_Conversion'),
                        'average_no_of_days': contact_data.get('Average_no_of_days'),  # Note: lowercase
                        'placement_lead_time_days': contact_data.get('Placement_Lead_Time_days'),  # Note: lowercase 'days'
                        'placement_deadline': parse_datetime_field(contact_data.get('Placement_Deadline')),
                        'placement_urgency': contact_data.get('Placement_Urgency'),
                        'decision_date': parse_datetime_field(contact_data.get('Decision_Date')),
                        'cohort_start_date': parse_datetime_field(contact_data.get('Cohort_Start_Date')),
                        
                        # Cancellation and Issues
                        'reason_for_cancellation': contact_data.get('Reason_for_Cancellation'),
                        'cancellation_notes': contact_data.get('Cancellation_Notes'),
                        'cancelled_date_time': parse_datetime_field(contact_data.get('Cancelled_Date_Time')),
                        'date_of_cancellation': parse_datetime_field(contact_data.get('Date_of_Cancellation')),
                        'refund_date': parse_datetime_field(contact_data.get('Refund_date')),  # Note: lowercase 'date'
                        
                        # Rating and Feedback
                        'rating': list_to_json_string(contact_data.get('Rating')),
                        'rating_new': contact_data.get('Rating_New'),
                        'warm_call': contact_data.get('Warm_Call'),
                        
                        # Marketing and UTM fields
                        'utm_campaign': contact_data.get('UTM_Campaign'),
                        'utm_medium': contact_data.get('UTM_Medium'),
                        'utm_content': contact_data.get('UTM_Content'),
                        'utm_gclid': contact_data.get('UTM_GCLID'),  # Note: GCLID not Gclid
                        
                        # Other fields
                        'description': contact_data.get('Description'),
                        'additional_information': contact_data.get('Additional_Information'),
                        'notes1': contact_data.get('Notes1'),
                        'name1': contact_data.get('Name1'),
                        'other_industry': contact_data.get('Other_industry'),  # Note: lowercase 'industry'
                        'token': contact_data.get('Token'),
                        'tag': list_to_json_string(contact_data.get('Tag')),
                        'type': contact_data.get('Type'),
                        'enrich_status': contact_data.get('Enrich_Status__s'),  # Note: __s suffix
                        'is_duplicate': contact_data.get('$is_duplicate', False),
                        'locked': contact_data.get('Locked__s', False),  # Note: __s suffix
                        'locked_for_me': contact_data.get('$locked_for_me', False),
                        'in_merge': contact_data.get('$in_merge', False),
                        
                        # Additional field from your working ETL
                        'end_date_auto_populated': parse_datetime_field(contact_data.get('End_date_Auto_populated')),
                        
                        # Account ID relationship
                        'account_id': extract_nested_id(contact_data.get('Account_Name')),
                    }
                    
                    # Create or update contact
                    contact, created = Contact.objects.update_or_create(
                        id=contact_fields_mapped['id'],
                        defaults=contact_fields_mapped
                    )
                    
                    synced_count += 1
                    
                    # Track latest modified time
                    if contact_fields_mapped['updated_time']:
                        if latest_modified is None or contact_fields_mapped['updated_time'] > latest_modified:
                            latest_modified = contact_fields_mapped['updated_time']
                    
                    if synced_count % 100 == 0:
                        logger.info(f"Processed {synced_count} contacts...")
                        
            except Exception as e:
                logger.error(f"Error processing contact {contact_data.get('id')}: {str(e)}")
                continue
        
        # Update sync tracker
        if latest_modified:
            update_sync_tracker('contacts', latest_modified, synced_count)
        
        logger.info(f"Contacts sync completed successfully. Synced {synced_count} contacts")
        
    except Exception as e:
        logger.error(f"Error in contact sync: {str(e)}")
        raise


def sync_deals_for_account(zoho_client, account_id):
    """Sync deals for a specific account"""
    deal_fields = [
        'id', 'Description', 'Deal_Name', 'Account_Name', 'Stage', 
        'Start_date', 'End_date', 'Created_Time', 'Modified_Time'
    ]
    
    try:
        # Fetch deals for this account
        deals_data = zoho_client.get_related_records(
            module='Accounts',
            record_id=account_id,
            related_module='Deals',
            fields=deal_fields
        )
        
        deals_synced = 0
        
        for deal_data in deals_data:
            try:
                with transaction.atomic():
                    # Parse and prepare deal data
                    deal_fields_mapped = {
                        'id': deal_data.get('id'),
                        'deal_name': deal_data.get('Deal_Name'),
                        'description': deal_data.get('Description'),
                        'account_id': account_id,
                        'account_name': extract_nested_name(deal_data.get('Account_Name')),
                        'stage': deal_data.get('Stage'),
                        'start_date': parse_datetime_field(deal_data.get('Start_date')),
                        'end_date': parse_datetime_field(deal_data.get('End_date')),
                        'created_time': parse_datetime_field(deal_data.get('Created_Time')),
                        'modified_time': parse_datetime_field(deal_data.get('Modified_Time')),
                    }
                    
                    # Create or update deal
                    deal, created = Deal.objects.update_or_create(
                        id=deal_fields_mapped['id'],
                        defaults=deal_fields_mapped
                    )
                    
                    deals_synced += 1
                    
            except Exception as e:
                logger.error(f"Error processing deal {deal_data.get('id')} for account {account_id}: {str(e)}")
                continue
        
        logger.info(f"Synced {deals_synced} deals for account {account_id}")
        return deals_synced
        
    except Exception as e:
        logger.error(f"Error fetching deals for account {account_id}: {str(e)}")
        return 0


def sync_accounts(incremental=True):
    """Sync accounts from Zoho CRM to Django database"""
    logger.info("Starting account sync...")
    zoho = ZohoClient()
    
    # Determine sync criteria
    criteria = None
    last_sync_info = ""
    
    if incremental:
        tracker = get_sync_tracker('accounts')
        if tracker.last_sync_timestamp:
            criteria = build_incremental_criteria(tracker.last_sync_timestamp)
            last_sync_info = f" (incremental since {tracker.last_sync_timestamp})"
        else:
            last_sync_info = " (full sync - no previous sync found)"
    else:
        last_sync_info = " (full sync - forced)"
    
    logger.info(f"Getting account data from Zoho{last_sync_info}...")
    
    # Define fields to fetch - comprehensive account fields using exact field names from your working ETL
    account_fields = [
        # Core fields
        'id', 'Account_Name', 'Industry', 'Billing_Address', 'Shipping_Address',
        'Owner', 'Modified_Time', 'Created_Time',
        
        # Company and Business fields  
        'Company_Work_Policy', 'Company_Industry', 'Company_Desciption',  # Note: typo in original
        'Company_Industry_Other', 'No_Employees', 'Standard_working_hours',  # Note: lowercase
        'Company_Address', 'Industry_areas',  # Note: lowercase
        
        # Location fields
        'Location', 'Location_other', 'City', 'Postcode', 'Country',  # Note: lowercase 'other'
        'Street', 'State_Region',
        
        # University fields
        'Uni_Region', 'Uni_Country', 'Uni_State_if_in_US', 'Uni_Timezone',
        
        # Status and Management fields (using correct field names)
        'Management_Status', 'Approval_status', 'Account_Status', 'Record_Status__s',  # Note: __s suffix
        'Cleanup_Status', 'Cleanup_Phase', 'Uni_Outreach_Status',
        'Placement_s_Revision_Required', 'Due_Diligence_Fields_to_Revise',  # Note: 's_' in middle
        
        # Process and Review fields (with $ prefixes for system fields)
        '$process_flow', '$review', '$review_process', '$approval_state',
        'Enrich_Status__s', 'Gold_Rating', 'Classic_Partnership', '$pathfinder',  # Note: __s suffix and $ prefixes
        
        # Dates and Timeline
        'Cleanup_Start_Date', 'Last_Activity_Time', 'Last_Full_Due_Diligence_Date',
        'Follow_up_Date', 'Next_Reply_Date',  # Note: lowercase 'up'
        
        # Administrative fields (with proper system field prefixes and suffixes)
        'Layout', '$field_states', 'Locked__s', '$locked_for_me', '$is_duplicate', '$in_merge',  # Note: Layout not Layout_ID
        'Tag', 'Type',
        
        # Role and Opportunity fields
        'Roles_available', 'Roles', 'Upon_to_Remote_interns',  # Note: lowercase variations
        
        # Notes and Documentation
        'Outreach_Notes', 'Account_Notes', 'Cleanup_Notes', '$approval'  # Note: $ prefix for approval
    ]
    
    try:
        # Fetch data from Zoho
        zoho_accounts = zoho.get_paginated_data(
            module='Accounts',
            fields=account_fields,
            criteria=criteria,
            sort_by='Modified_Time',
            sort_order='asc'
        )
        
        logger.info(f"Retrieved {len(zoho_accounts)} accounts from Zoho")
        
        if not zoho_accounts:
            logger.info("No accounts to sync")
            return
        
        # Process accounts
        synced_count = 0
        latest_modified = None
        
        for account_data in zoho_accounts:
            try:
                with transaction.atomic():
                    # Parse and prepare account data - using field names from your working ETL
                    owner_data = account_data.get('Owner', {})
                    tag_data = account_data.get('Tag')
                    tag = list_to_json_string(tag_data) if tag_data else None
                    account_fields_mapped = {
                        # Core fields
                        'id': account_data.get('id'),
                        'name': account_data.get('Account_Name'),
                        'industry': account_data.get('Industry'),
                        'billing_address': json.dumps(account_data.get('Billing_Address')) if account_data.get('Billing_Address') else None,
                        'shipping_address': json.dumps(account_data.get('Shipping_Address')) if account_data.get('Shipping_Address') else None,
                        'owner_id': extract_nested_id(owner_data),
                        'owner_name': extract_nested_name(owner_data),
                        'owner_email': extract_nested_email(owner_data),
                        
                        # Company and Business fields (using exact field names from your working ETL)
                        'company_work_policy': list_to_json_string(account_data.get('Company_Work_Policy')),
                        'company_industry': account_data.get('Company_Industry'),
                        'company_description': account_data.get('Company_Desciption'),  # Note: typo in Zoho field name
                        'company_industry_other': account_data.get('Company_Industry_Other'),
                        'no_employees': account_data.get('No_Employees'),
                        'standard_working_hours': account_data.get('Standard_working_hours'),  # Note: lowercase
                        'company_address': account_data.get('Company_Address'),
                        'industry_areas': account_data.get('Industry_areas'),  # Note: lowercase
                        
                        # Location fields (using exact field names)
                        'location': account_data.get('Location'),
                        'location_other': account_data.get('Location_other'),  # Note: lowercase 'other'
                        'city': account_data.get('City'),
                        'postcode': account_data.get('Postcode'),
                        'country': account_data.get('Country'),
                        'street': account_data.get('Street'),
                        'state_region': account_data.get('State_Region'),
                        
                        # University fields
                        'uni_region': account_data.get('Uni_Region'),
                        'uni_country': account_data.get('Uni_Country'),
                        'uni_state_if_in_us': account_data.get('Uni_State_if_in_US'),
                        'uni_timezone': account_data.get('Uni_Timezone'),
                        
                        # Status and Management fields (using correct field names with suffixes)
                        'management_status': account_data.get('Management_Status'),
                        'approval_status': account_data.get('Approval_status'),  # Note: lowercase 'status'
                        'account_status': account_data.get('Account_Status'),
                        'record_status': account_data.get('Record_Status__s'),  # Note: __s suffix
                        'cleanup_status': account_data.get('Cleanup_Status'),
                        'cleanup_phase': account_data.get('Cleanup_Phase'),
                        'uni_outreach_status': account_data.get('Uni_Outreach_Status'),
                        'placement_revision_required': account_data.get('Placement_s_Revision_Required'),  # Note: 's_' in middle
                        'due_diligence_fields_to_revise': list_to_json_string(account_data.get('Due_Diligence_Fields_to_Revise')),
                        
                        # Process and Review fields (using $ prefixes for system fields)
                        'process_flow': account_data.get('$process_flow', False),
                        'review': account_data.get('$review'),
                        'review_process': json.dumps(account_data.get('$review_process')) if account_data.get('$review_process') else None,
                        'approval_state': account_data.get('$approval_state'),
                        'enrich_status': account_data.get('Enrich_Status__s'),  # Note: __s suffix
                        'gold_rating': account_data.get('Gold_Rating', False),
                        'classic_partnership': account_data.get('Classic_Partnership'),
                        'pathfinder': account_data.get('$pathfinder', False),  # Note: $ prefix
                        
                        # Dates and Timeline
                        'cleanup_start_date': parse_datetime_field(account_data.get('Cleanup_Start_Date')),
                        'last_activity_time': parse_datetime_field(account_data.get('Last_Activity_Time')),
                        'last_full_due_diligence_date': parse_datetime_field(account_data.get('Last_Full_Due_Diligence_Date')),
                        'follow_up_date': parse_datetime_field(account_data.get('Follow_up_Date')),  # Note: lowercase 'up'
                        'next_reply_date': parse_datetime_field(account_data.get('Next_Reply_Date')),
                        
                        # Administrative fields (using proper system field prefixes and suffixes)
                        'layout_id': extract_nested_id(account_data.get('Layout')),  # Note: Layout not Layout_ID
                        'layout_display_label': account_data.get('Layout', {}).get('display_label') if account_data.get('Layout') else None,
                        'layout_name': account_data.get('Layout', {}).get('name') if account_data.get('Layout') else None,
                        'field_states': json.dumps(account_data.get('$field_states')) if account_data.get('$field_states') else None,
                        'locked': account_data.get('Locked__s', False),  # Note: __s suffix
                        'locked_for_me': account_data.get('$locked_for_me', False),
                        'is_duplicate': account_data.get('$is_duplicate', False),
                        'in_merge': account_data.get('$in_merge', False),
                        'tag': tag,
                        'is_dnc': any("DNC" in str(item.get("name", "")) for item in tag_data if isinstance(item, dict)) if tag_data and isinstance(tag_data, list) else False,
                        'type': account_data.get('Type'),
                        
                        # Role and Opportunity fields (using correct field names)
                        'roles_available': account_data.get('Roles_available'),  # Note: lowercase 'available'
                        'roles': account_data.get('Roles'),
                        'upon_to_remote_interns': account_data.get('Upon_to_Remote_interns', False),  # Note: lowercase 'interns'
                        
                        # Notes and Documentation (using correct field names)
                        'outreach_notes': account_data.get('Outreach_Notes'),
                        'account_notes': account_data.get('Account_Notes'),
                        'cleanup_notes': account_data.get('Cleanup_Notes'),
                        'approval': json.dumps(account_data.get('$approval')) if account_data.get('$approval') else None,  # Note: $ prefix
                    }
                    
                    # Create or update account
                    account, created = Account.objects.update_or_create(
                        id=account_fields_mapped['id'],
                        defaults=account_fields_mapped
                    )
                    
                    # Sync deals for this account
                    # deals_count = sync_deals_for_account(zoho, account_fields_mapped['id'])
                    
                    synced_count += 1
                    
                    # Track latest modified time
                    modified_time = parse_datetime_field(account_data.get('Modified_Time'))
                    if modified_time:
                        if latest_modified is None or modified_time > latest_modified:
                            latest_modified = modified_time
                    
                    if synced_count % 100 == 0:
                        logger.info(f"Processed {synced_count} accounts...")
                        
            except Exception as e:
                logger.error(f"Error processing account {account_data.get('id')}: {str(e)}")
                continue
        
        # Update sync tracker
        if latest_modified:
            update_sync_tracker('accounts', latest_modified, synced_count)
        
        logger.info(f"Accounts sync completed successfully. Synced {synced_count} accounts")
        
    except Exception as e:
        logger.error(f"Error in account sync: {str(e)}")
        raise


def sync_intern_roles(incremental=True):
    """Sync intern roles from Zoho CRM to Django database"""
    logger.info("Starting intern roles sync...")
    zoho = ZohoClient()
    
    # Custom module - using the exact module name from your working ETL
    module_name = 'Intern_Roles'  # Your ETL uses this instead of 'Deals'
    
    # Determine sync criteria
    criteria = None
    last_sync_info = ""
    
    if incremental:
        tracker = get_sync_tracker('intern_roles')
        if tracker.last_sync_timestamp:
            criteria = build_incremental_criteria(tracker.last_sync_timestamp)
            last_sync_info = f" (incremental since {tracker.last_sync_timestamp})"
        else:
            last_sync_info = " (full sync - no previous sync found)"
    else:
        last_sync_info = " (full sync - forced)"
    
    logger.info(f"Getting intern role data from Zoho{last_sync_info}...")
    
    # Define fields to fetch - using exact field names from your working ETL
    role_fields = [
        # Core fields from your working ETL
        'id', 'Name', 'Role_Title', 'Role_Description_Requirements', 'Role_Status', 
        'Role_Function', 'Role_Department_Size', 'Role_Attachments_JD', 
        'Role_Tags', 'Start_Date', 'End_Date', 'Created_Time', 'Modified_Time',
        
        # Company relationship field
        'Intern_Company',  # Your ETL uses this field name
        
        # Work and Location fields
        'Company_Work_Policy', 'Location', 'Open_to_Remote',
        
        # Status and Management fields (with proper suffixes)
        'Due_Diligence_Status_2', 'Account_Outreach_Status', 'Record_Status__s',  # Note: __s suffix
        'Approval_State', 'Management_Status', 'Placement_Fields_to_Revise',
        'Placement_Revision_Notes', 'Gold_Rating', 'Locked__s'  # Note: __s suffix
    ]
    
    try:
        # Fetch data from Zoho
        zoho_roles = zoho.get_paginated_data(
            module=module_name,
            fields=role_fields,
            criteria=criteria,
            sort_by='Modified_Time',
            sort_order='asc'
        )
        
        logger.info(f"Retrieved {len(zoho_roles)} intern roles from Zoho")
        
        if not zoho_roles:
            logger.info("No intern roles to sync")
            return
        
        # Process intern roles
        synced_count = 0
        latest_modified = None
        
        for role_data in zoho_roles:
            try:
                with transaction.atomic():
                    # Parse and prepare role data - using exact field mapping from your working ETL
                    intern_company_data = role_data.get('Intern_Company', {})
                    role_fields_mapped = {
                        # Core fields (using exact field names from your working ETL)
                        'id': role_data.get('id'),
                        'name': role_data.get('Name'),  # Note: 'Name' not 'Deal_Name'
                        'role_title': role_data.get('Role_Title'),
                        'role_description_requirements': role_data.get('Role_Description_Requirements'),
                        'role_status': role_data.get('Role_Status'),
                        'role_function': role_data.get('Role_Function'),
                        'role_department_size': role_data.get('Role_Department_Size'),
                        'role_attachments_jd': list_to_json_string(role_data.get('Role_Attachments_JD')),
                        'role_tags': list_to_json_string(role_data.get('Role_Tags')),
                        'start_date': parse_datetime_field(role_data.get('Start_Date')),
                        'end_date': parse_datetime_field(role_data.get('End_Date')),
                        'created_time': parse_datetime_field(role_data.get('Created_Time')),
                        
                        # Company relationship fields (using exact field names)
                        'intern_company_id': extract_nested_id(intern_company_data),
                        'intern_company_name': extract_nested_name(intern_company_data),
                        'company_work_policy': list_to_json_string(role_data.get('Company_Work_Policy')),
                        'location': role_data.get('Location'),
                        'open_to_remote': role_data.get('Open_to_Remote'),
                        
                        # Status and Management fields (using exact field names with suffixes)
                        'due_diligence_status_2': role_data.get('Due_Diligence_Status_2'),
                        'account_outreach_status': role_data.get('Account_Outreach_Status'),
                        'record_status': role_data.get('Record_Status__s'),  # Note: __s suffix
                        'approval_state': role_data.get('Approval_State'),
                        'management_status': role_data.get('Management_Status'),
                        'placement_fields_to_revise': list_to_json_string(role_data.get('Placement_Fields_to_Revise')),
                        'placement_revision_notes': role_data.get('Placement_Revision_Notes'),
                        'gold_rating': role_data.get('Gold_Rating', False),
                        'locked': role_data.get('Locked__s', False),  # Note: __s suffix
                    }
                    
                    # Create or update intern role
                    role, created = InternRole.objects.update_or_create(
                        id=role_fields_mapped['id'],
                        defaults=role_fields_mapped
                    )
                    
                    synced_count += 1
                    
                    # Track latest modified time
                    modified_time = parse_datetime_field(role_data.get('Modified_Time'))
                    if modified_time:
                        if latest_modified is None or modified_time > latest_modified:
                            latest_modified = modified_time
                    
                    if synced_count % 100 == 0:
                        logger.info(f"Processed {synced_count} intern roles...")
                        
            except Exception as e:
                logger.error(f"Error processing intern role {role_data.get('id')}: {str(e)}")
                continue
        
        # Update sync tracker
        if latest_modified:
            update_sync_tracker('intern_roles', latest_modified, synced_count)
        
        logger.info(f"Intern roles sync completed successfully. Synced {synced_count} roles")
        
    except Exception as e:
        logger.error(f"Error in intern roles sync: {str(e)}")
        raise


def sync_deals(incremental=True):
    """Sync all deals from Zoho CRM to Django database"""
    logger.info("Starting deals sync...")
    zoho = ZohoClient()
    
    # Determine sync criteria
    criteria = None
    last_sync_info = ""
    
    if incremental:
        tracker = get_sync_tracker('deals')
        if tracker.last_sync_timestamp:
            criteria = build_incremental_criteria(tracker.last_sync_timestamp)
            last_sync_info = f" (incremental since {tracker.last_sync_timestamp})"
        else:
            last_sync_info = " (full sync - no previous sync found)"
    else:
        last_sync_info = " (full sync - forced)"
    
    logger.info(f"Getting deal data from Zoho{last_sync_info}...")
    
    # Define fields to fetch
    deal_fields = [
        'id', 'Description', 'Deal_Name', 'Account_Name', 'Stage', 
        'Start_date', 'End_date', 'Created_Time', 'Modified_Time'
    ]
    
    try:
        # Fetch data from Zoho
        zoho_deals = zoho.get_paginated_data(
            module='Deals',
            fields=deal_fields,
            criteria=criteria,
            sort_by='Modified_Time',
            sort_order='asc'
        )
        
        logger.info(f"Retrieved {len(zoho_deals)} deals from Zoho")
        
        if not zoho_deals:
            logger.info("No deals to sync")
            return
        
        # Process deals
        synced_count = 0
        latest_modified = None
        
        for deal_data in zoho_deals:
            try:
                with transaction.atomic():
                    # Parse and prepare deal data
                    deal_fields_mapped = {
                        'id': deal_data.get('id'),
                        'deal_name': deal_data.get('Deal_Name'),
                        'description': deal_data.get('Description'),
                        'account_id': extract_nested_id(deal_data.get('Account_Name')),
                        'account_name': extract_nested_name(deal_data.get('Account_Name')),
                        'stage': deal_data.get('Stage'),
                        'start_date': parse_datetime_field(deal_data.get('Start_date')),
                        'end_date': parse_datetime_field(deal_data.get('End_date')),
                        'created_time': parse_datetime_field(deal_data.get('Created_Time')),
                        'modified_time': parse_datetime_field(deal_data.get('Modified_Time')),
                    }
                    
                    # Create or update deal
                    deal, created = Deal.objects.update_or_create(
                        id=deal_fields_mapped['id'],
                        defaults=deal_fields_mapped
                    )
                    
                    synced_count += 1
                    
                    # Track latest modified time
                    if deal_fields_mapped['modified_time']:
                        if latest_modified is None or deal_fields_mapped['modified_time'] > latest_modified:
                            latest_modified = deal_fields_mapped['modified_time']
                    
                    if synced_count % 100 == 0:
                        logger.info(f"Processed {synced_count} deals...")
                        
            except Exception as e:
                logger.error(f"Error processing deal {deal_data.get('id')}: {str(e)}")
                continue
        
        # Update sync tracker
        if latest_modified:
            update_sync_tracker('deals', latest_modified, synced_count)
        
        logger.info(f"Deals sync completed successfully. Synced {synced_count} deals")
        
    except Exception as e:
        logger.error(f"Error in deals sync: {str(e)}")
        raise


def run_full_etl_pipeline():
    """Run the complete ETL pipeline"""
    logger.info("Starting full ETL pipeline...")
    
    try:
        # Run all sync operations
        sync_contacts()
        sync_accounts()  # This now includes deals for each account
        sync_intern_roles()
        sync_deals()  # Additional standalone deals sync
        
        logger.info("✅ Full ETL pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ ETL pipeline failed: {str(e)}")
        raise
