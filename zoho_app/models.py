from django.db import models
from django.utils import timezone

class SyncTracker(models.Model):
    ENTITY_TYPE_CHOICES = [
        ('contacts', 'Contacts'),
        ('accounts', 'Accounts'),
        ('intern_roles', 'Intern Roles'),
        ('deals', 'Deals'),
        ('role_deals', 'Role Deals'),
    ]
    entity_type = models.CharField(max_length=50, choices=ENTITY_TYPE_CHOICES, unique=True)
    last_sync_timestamp = models.DateTimeField(null=True, blank=True)
    records_synced = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.entity_type} - {self.last_sync_timestamp}"


class Contact(models.Model):
    id = models.CharField(primary_key=True, max_length=255)
    email = models.CharField(max_length=255, null=True, blank=True)
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    phone = models.CharField(max_length=255, null=True, blank=True)
    account_id = models.CharField(max_length=255, null=True, blank=True)
    title = models.CharField(max_length=255, null=True, blank=True)
    department = models.CharField(max_length=255, null=True, blank=True)
    updated_time = models.DateTimeField(null=True, blank=True)
    
    # Additional fields (all optional, large set)
    age_on_start_date = models.IntegerField(null=True, blank=True)
    timezone = models.CharField(max_length=255, null=True, blank=True)
    contact_last_name = models.CharField(max_length=255, null=True, blank=True)
    field_states = models.TextField(null=True, blank=True)
    arrival_drop_off_address = models.TextField(null=True, blank=True)
    gender = models.CharField(max_length=255, null=True, blank=True)
    interview = models.CharField(max_length=255, null=True, blank=True)
    process_flow = models.BooleanField(null=True)
    end_date = models.DateTimeField(null=True, blank=True)
    role_owner = models.CharField(max_length=255, null=True, blank=True)
    paid_role = models.CharField(max_length=255, null=True, blank=True)
    role_success_notes = models.TextField(null=True, blank=True)
    approval = models.TextField(null=True, blank=True)
    departure_date_time = models.DateTimeField(null=True, blank=True)
    approval_date = models.DateTimeField(null=True, blank=True)
    requires_a_visa = models.CharField(max_length=255, null=True, blank=True)
    contact_email = models.CharField(max_length=255, null=True, blank=True)
    follow_up_date = models.DateTimeField(null=True, blank=True)
    review_process = models.TextField(null=True, blank=True)
    admission_member = models.CharField(max_length=255, null=True, blank=True)
    english_level = models.CharField(max_length=255, null=True, blank=True)
    placement_status = models.CharField(max_length=255, null=True, blank=True)
    likelihood_to_convert = models.CharField(max_length=255, null=True, blank=True)
    role_success_stage = models.CharField(max_length=255, null=True, blank=True)
    call_to_conversion_time_days = models.IntegerField(null=True, blank=True)
    lead_created_time = models.DateTimeField(null=True, blank=True)
    university_name = models.CharField(max_length=255, null=True, blank=True)
    job_title = models.CharField(max_length=255, null=True, blank=True)
    layout_id = models.CharField(max_length=255, null=True, blank=True)
    layout_display_label = models.CharField(max_length=255, null=True, blank=True)
    layout_name = models.CharField(max_length=255, null=True, blank=True)
    intro_call_date = models.DateTimeField(null=True, blank=True)
    visa_alt_options = models.TextField(null=True, blank=True)
    student_decision = models.CharField(max_length=255, null=True, blank=True)
    arrival_date_time = models.DateTimeField(null=True, blank=True)
    rating_new = models.CharField(max_length=255, null=True, blank=True)
    role_confirmed_date = models.DateTimeField(null=True, blank=True)
    start_date = models.DateTimeField(null=True, blank=True)
    last_activity_time = models.DateTimeField(null=True, blank=True)
    industry = models.CharField(max_length=255, null=True, blank=True)
    visa_f_u_date = models.DateTimeField(null=True, blank=True)
    location_other = models.CharField(max_length=255, null=True, blank=True)
    placement_lead_time_days = models.IntegerField(null=True, blank=True)
    graduation_date = models.DateTimeField(null=True, blank=True)
    other_payment_status = models.TextField(null=True, blank=True)
    days_since_conversion = models.IntegerField(null=True, blank=True)
    name1 = models.TextField(null=True, blank=True)
    average_no_of_days = models.IntegerField(null=True, blank=True)
    duration = models.TextField(null=True, blank=True)
    warm_call = models.TextField(null=True, blank=True)
    other_industry = models.TextField(null=True, blank=True)
    call_scheduled_date_time = models.DateTimeField(null=True, blank=True)
    interviewer = models.TextField(null=True, blank=True)
    visa_successful = models.TextField(null=True, blank=True)
    utm_campaign = models.TextField(null=True, blank=True)
    rating = models.TextField(null=True, blank=True)
    alternative_location1 = models.TextField(null=True, blank=True)
    enrolment_to_intro_call_lead_time = models.IntegerField(null=True, blank=True)
    review = models.TextField(null=True, blank=True)
    reason_for_cancellation = models.TextField(null=True, blank=True)
    cancelled_date_time = models.DateTimeField(null=True, blank=True)
    uni_start_date = models.DateTimeField(null=True, blank=True)
    notes1 = models.TextField(null=True, blank=True)
    partner_organisation = models.TextField(null=True, blank=True)
    date_of_birth = models.DateTimeField(null=True, blank=True)
    call_booked_date_time = models.DateTimeField(null=True, blank=True)
    date_of_cancellation = models.DateTimeField(null=True, blank=True)
    in_merge = models.BooleanField(null=True)
    approval_state = models.TextField(null=True, blank=True)
    location = models.TextField(null=True, blank=True)
    industry_choice_1 = models.TextField(null=True, blank=True)
    industry_choice_3 = models.TextField(null=True, blank=True)
    company_decision = models.TextField(null=True, blank=True)
    student_bio = models.TextField(null=True, blank=True)
    token = models.TextField(null=True, blank=True)
    additional_information = models.TextField(null=True, blank=True)
    placement_deadline = models.DateTimeField(null=True, blank=True)
    created_time = models.DateTimeField(null=True, blank=True)
    change_log_time = models.DateTimeField(null=True, blank=True)
    community_owner = models.TextField(null=True, blank=True)
    created_by_email = models.TextField(null=True, blank=True)
    current_location_v2 = models.TextField(null=True, blank=True)
    decision_date = models.DateTimeField(null=True, blank=True)
    utm_medium = models.TextField(null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    do_not_contact = models.BooleanField(null=True)
    industry_choice_2 = models.TextField(null=True, blank=True)
    job_offered_after = models.TextField(null=True, blank=True)
    full_name = models.TextField(null=True, blank=True)
    ps_assigned_date = models.DateTimeField(null=True, blank=True)
    account_name = models.TextField(null=True, blank=True)
    email_opt_out = models.BooleanField(null=True)
    books_cust_id = models.TextField(null=True, blank=True)
    student_status = models.TextField(null=True, blank=True)
    days_count = models.IntegerField(null=True, blank=True)
    record_status = models.TextField(null=True, blank=True)
    nationality = models.TextField(null=True, blank=True)
    type = models.TextField(null=True, blank=True)
    cancellation_notes = models.TextField(null=True, blank=True)
    departure_flight_number = models.TextField(null=True, blank=True)
    locked = models.BooleanField(null=True)
    tag = models.TextField(null=True, blank=True)
    last_enriched_time = models.DateTimeField(null=True, blank=True)
    country_city_of_residence = models.TextField(null=True, blank=True)
    refund_date = models.DateTimeField(null=True, blank=True)
    visa_type_exemption = models.TextField(null=True, blank=True)
    industry_2_areas = models.TextField(null=True, blank=True)
    locked_for_me = models.BooleanField(null=True)
    from_university_partner = models.TextField(null=True, blank=True)
    placement_urgency = models.TextField(null=True, blank=True)
    enrich_status = models.TextField(null=True, blank=True)
    visa_eligible = models.TextField(null=True, blank=True)
    utm_content = models.TextField(null=True, blank=True)
    cohort_start_date = models.DateTimeField(null=True, blank=True)
    secondary_email = models.TextField(null=True, blank=True)
    is_duplicate = models.BooleanField(null=True)
    signed_agreement = models.TextField(null=True, blank=True)
    myinterview_url = models.TextField(null=True, blank=True)
    interview_successful = models.TextField(null=True, blank=True)
    skills = models.TextField(null=True, blank=True)
    link_to_cv = models.TextField(null=True, blank=True)
    accommodation_finalised = models.TextField(null=True, blank=True)
    send_mail2 = models.BooleanField(null=True)
    utm_gclid = models.TextField(null=True, blank=True)
    unsubscribed_time = models.DateTimeField(null=True, blank=True)
    t_c_link = models.TextField(null=True, blank=True)
    number_of_days = models.IntegerField(null=True, blank=True)
    agreement_finalised = models.TextField(null=True, blank=True)
    end_date_auto_populated = models.DateTimeField(null=True, blank=True)
    industry_1_areas = models.TextField(null=True, blank=True)
    total = models.TextField(null=True, blank=True)
    visa_owner = models.TextField(null=True, blank=True)
    visa_notes = models.TextField(null=True, blank=True)
    house_rules = models.TextField(null=True, blank=True)

    partnership_specialist_id = models.CharField(max_length=255, null=True, blank=True)

    # New field: Placement_Automation - can be null, 'Yes', 'No', or a date string
    placement_automation = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return self.full_name or self.email or self.id


class Account(models.Model):
    id = models.CharField(max_length=255, primary_key=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    industry = models.CharField(max_length=255, blank=True, null=True)
    billing_address = models.TextField(blank=True, null=True)
    shipping_address = models.TextField(blank=True, null=True)

    owner_id = models.CharField(max_length=255, blank=True, null=True)
    owner_name = models.CharField(max_length=255, blank=True, null=True)
    owner_email = models.CharField(max_length=255, blank=True, null=True)
    cleanup_start_date = models.DateTimeField(blank=True, null=True)
    field_states = models.TextField(blank=True, null=True)
    management_status = models.CharField(max_length=255, blank=True, null=True)
    company_work_policy = models.TextField(blank=True, null=True)
    last_activity_time = models.DateTimeField(blank=True, null=True)
    last_full_due_diligence_date = models.DateTimeField(blank=True, null=True)
    company_industry = models.CharField(max_length=255, blank=True, null=True)
    company_description = models.TextField(blank=True, null=True)
    process_flow = models.BooleanField(default=False)
    approval_status = models.CharField(max_length=255, blank=True, null=True)
    street = models.TextField(blank=True, null=True)
    locked_for_me = models.BooleanField(default=False)
    classic_partnership = models.CharField(max_length=255, blank=True, null=True)
    state_region = models.CharField(max_length=255, blank=True, null=True)
    cleanup_status = models.CharField(max_length=255, blank=True, null=True)
    uni_region = models.CharField(max_length=255, blank=True, null=True)
    approval = models.TextField(blank=True, null=True)
    uni_outreach_status = models.CharField(max_length=255, blank=True, null=True)
    enrich_status = models.CharField(max_length=255, blank=True, null=True)
    roles_available = models.TextField(blank=True, null=True)
    roles = models.TextField(blank=True, null=True)
    city = models.CharField(max_length=255, blank=True, null=True)
    postcode = models.CharField(max_length=255, blank=True, null=True)
    outreach_notes = models.TextField(blank=True, null=True)
    company_industry_other = models.CharField(max_length=255, blank=True, null=True)
    no_employees = models.CharField(max_length=255, blank=True, null=True)
    industry_areas = models.TextField(blank=True, null=True)
    placement_revision_required = models.CharField(max_length=255, blank=True, null=True)
    country = models.CharField(max_length=255, blank=True, null=True)
    is_duplicate = models.BooleanField(default=False)
    uni_state_if_in_us = models.CharField(max_length=255, blank=True, null=True)
    follow_up_date = models.DateTimeField(blank=True, null=True)
    review_process = models.TextField(blank=True, null=True)
    layout_id = models.CharField(max_length=255, blank=True, null=True)
    layout_display_label = models.CharField(max_length=255, blank=True, null=True)
    layout_name = models.CharField(max_length=255, blank=True, null=True)
    review = models.CharField(max_length=255, blank=True, null=True)
    cleanup_notes = models.TextField(blank=True, null=True)
    gold_rating = models.BooleanField(default=False)
    account_notes = models.TextField(blank=True, null=True)
    standard_working_hours = models.CharField(max_length=255, blank=True, null=True)
    due_diligence_fields_to_revise = models.TextField(blank=True, null=True)
    uni_country = models.CharField(max_length=255, blank=True, null=True)
    cleanup_phase = models.CharField(max_length=255, blank=True, null=True)
    next_reply_date = models.DateTimeField(blank=True, null=True)
    record_status = models.CharField(max_length=255, blank=True, null=True)
    type = models.CharField(max_length=255, blank=True, null=True)
    in_merge = models.BooleanField(default=False)
    uni_timezone = models.CharField(max_length=255, blank=True, null=True)
    upon_to_remote_interns = models.BooleanField(default=False)
    locked = models.BooleanField(default=False)
    company_address = models.TextField(blank=True, null=True)
    tag = models.TextField(blank=True, null=True)
    is_dnc = models.BooleanField(default=False)
    approval_state = models.CharField(max_length=255, blank=True, null=True)
    pathfinder = models.BooleanField(default=False)
    location = models.CharField(max_length=255, blank=True, null=True)
    location_other = models.CharField(max_length=255, blank=True, null=True)
    account_status = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.name or self.id


class InternRole(models.Model):
    id = models.CharField(max_length=255, primary_key=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    role_title = models.CharField(max_length=255, blank=True, null=True)
    role_description_requirements = models.TextField(blank=True, null=True)
    role_status = models.CharField(max_length=255, blank=True, null=True)
    role_function = models.CharField(max_length=255, blank=True, null=True)
    role_department_size = models.CharField(max_length=255, blank=True, null=True)
    role_attachments_jd = models.TextField(blank=True, null=True)
    role_tags = models.TextField(blank=True, null=True)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    created_time = models.DateTimeField(blank=True, null=True)
    intern_company_id = models.CharField(max_length=255, blank=True, null=True)
    intern_company_name = models.CharField(max_length=255, blank=True, null=True)
    company_work_policy = models.TextField(blank=True, null=True)
    location = models.CharField(max_length=255, blank=True, null=True)
    open_to_remote = models.CharField(max_length=255, blank=True, null=True)
    due_diligence_status_2 = models.CharField(max_length=255, blank=True, null=True)
    account_outreach_status = models.CharField(max_length=255, blank=True, null=True)
    record_status = models.CharField(max_length=255, blank=True, null=True)
    approval_state = models.CharField(max_length=255, blank=True, null=True)
    management_status = models.CharField(max_length=255, blank=True, null=True)
    placement_fields_to_revise = models.TextField(blank=True, null=True)
    placement_revision_notes = models.TextField(blank=True, null=True)
    gold_rating = models.BooleanField(default=False)
    locked = models.BooleanField(default=False)

    def __str__(self):
        return self.role_title or self.name


class Deal(models.Model):
    id = models.CharField(max_length=255, primary_key=True)
    deal_name = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    account_id = models.CharField(max_length=255, blank=True, null=True)
    account_name = models.CharField(max_length=255, blank=True, null=True)
    stage = models.CharField(max_length=255, blank=True, null=True)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    created_time = models.DateTimeField(blank=True, null=True)
    modified_time = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.deal_name or self.id


# class ContactDeal(models.Model):
#     id = models.CharField(max_length=255, primary_key=True)
#     deal_name = models.CharField(max_length=255, blank=True, null=True)
#     description = models.TextField(blank=True, null=True)
#     contact_id = models.CharField(max_length=255, blank=True, null=True)
#     account_id = models.CharField(max_length=255, blank=True, null=True)
#     account_name = models.CharField(max_length=255, blank=True, null=True)
#     stage = models.CharField(max_length=255, blank=True, null=True)
#     deal_type = models.CharField(max_length=255, blank=True, null=True)
#     closing_date = models.DateTimeField(blank=True, null=True)
#     start_date = models.DateTimeField(blank=True, null=True)
#     end_date = models.DateTimeField(blank=True, null=True)
#     created_time = models.DateTimeField(blank=True, null=True)
#     modified_time = models.DateTimeField(blank=True, null=True)
#     created_by_id = models.CharField(max_length=255, blank=True, null=True)
#     created_by_name = models.CharField(max_length=255, blank=True, null=True)
#     created_by_email = models.CharField(max_length=255, blank=True, null=True)
#     created_at = models.DateTimeField(auto_now_add=True)
#     updated_at = models.DateTimeField(auto_now=True)

#     def __str__(self):
#         return f"{self.deal_name} (Contact: {self.contact_id})" or self.id


class Document(models.Model):
    contact_id = models.CharField(max_length=255)
    document_id = models.CharField(max_length=255)
    document_name = models.CharField(max_length=255)
    document_type = models.CharField(max_length=50)
    file_path = models.CharField(max_length=500)
    file_size = models.IntegerField(blank=True, null=True)
    download_date = models.DateTimeField(auto_now_add=True)
    zoho_created_time = models.DateTimeField(blank=True, null=True)
    zoho_modified_time = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.document_name} ({self.document_type})"


class Skill(models.Model):
    contact_id = models.CharField(max_length=255)
    document_id = models.IntegerField()
    skill_name = models.CharField(max_length=255)
    skill_category = models.CharField(max_length=100, blank=True, null=True)
    proficiency_level = models.CharField(max_length=50, blank=True, null=True)
    years_experience = models.CharField(max_length=50, blank=True, null=True)
    confidence_score = models.FloatField(blank=True, null=True)
    extraction_method = models.CharField(max_length=50)
    source_context = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.skill_name} - {self.skill_category}"


class RoleDealSync(models.Model):
    """
    Track deal syncing for intern roles to avoid unnecessary API calls
    """
    intern_role_id = models.CharField(max_length=255, unique=True)
    total_rejected_deals = models.IntegerField(default=0)
    last_sync_date = models.DateField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Role {self.intern_role_id} - {self.total_rejected_deals} rejected deals (synced: {self.last_sync_date})"


class JobMatch(models.Model):
    contact_id = models.CharField(max_length=255)
    intern_role_id = models.CharField(max_length=255)
    match_score = models.FloatField(default=0.0)
    industry_match = models.BooleanField(default=False)
    location_match = models.BooleanField(default=False)
    work_policy_match = models.BooleanField(default=False)
    skill_match = models.BooleanField(default=False)
    matched_industries = models.TextField(blank=True, null=True)
    matched_skills = models.TextField(blank=True, null=True)
    match_reason = models.TextField(blank=True, null=True)
    status = models.CharField(max_length=50, default='active')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.contact_id} ↔ {self.intern_role_id} [{self.match_score}]"


class OutreachLog(models.Model):
    """
    Track outreach emails sent to companies
    """
    intern_role_id = models.CharField(max_length=255)
    role_title = models.CharField(max_length=255, blank=True, null=True)
    company_id = models.CharField(max_length=255, blank=True, null=True)
    company_name = models.CharField(max_length=255, blank=True, null=True)
    
    # Email details
    subject = models.CharField(max_length=500)
    email_type = models.CharField(max_length=50, default='initial')  # initial, follow_up, final
    sender_email = models.CharField(max_length=255)
    sender_name = models.CharField(max_length=255, blank=True, null=True)
    recipients = models.TextField()  # JSON list of recipient emails
    
    # Email tracking fields for message IDs and threading
    message_id = models.CharField(max_length=500, blank=True, null=True)  # Unique email message ID
    thread_id = models.CharField(max_length=500, blank=True, null=True)  # Email thread ID for grouping
    in_reply_to = models.CharField(max_length=500, blank=True, null=True)  # References parent message
    parent_outreach_log = models.ForeignKey('self', on_delete=models.SET_NULL, blank=True, null=True, related_name='follow_up_emails')  # Links to original outreach for follow-ups
    
    # Candidates included
    candidate_ids = models.TextField()  # JSON list of candidate IDs
    candidates_count = models.IntegerField(default=0)
    
    # Outreach metadata
    is_urgent = models.BooleanField(default=False)
    is_sent = models.BooleanField(default=False)
    sent_at = models.DateTimeField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    
    # Follow-up tracking
    follow_up_count = models.IntegerField(default=0)
    last_follow_up_date = models.DateTimeField(blank=True, null=True)
    next_follow_up_date = models.DateTimeField(blank=True, null=True)
    
    # Response tracking
    response_received = models.BooleanField(default=False)
    response_date = models.DateTimeField(blank=True, null=True)
    response_type = models.CharField(max_length=50, blank=True, null=True)  # interested, not_interested, request_more_info
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"Outreach for {self.role_title} ({self.email_type}) - {self.sent_at or 'Not sent'}"


class EmailLimiter(models.Model):
    """
    Track email frequency to companies to enforce weekly limits
    """
    company_id = models.CharField(max_length=255, unique=True)
    company_name = models.CharField(max_length=255, blank=True, null=True)
    last_email_date = models.DateTimeField()
    emails_sent_this_week = models.IntegerField(default=0)
    week_start_date = models.DateField()
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.company_name} - {self.emails_sent_this_week} emails this week"


class FollowUpTask(models.Model):
    """
    Track follow-up tasks for outreach emails
    """
    outreach_log = models.ForeignKey(OutreachLog, on_delete=models.CASCADE, related_name='follow_ups')
    follow_up_type = models.CharField(max_length=50)  # follow_up, final, move_to_next
    scheduled_date = models.DateTimeField()
    completed = models.BooleanField(default=False)
    completed_at = models.DateTimeField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['scheduled_date']

    def __str__(self):
        return f"{self.follow_up_type} for {self.outreach_log.role_title} - {self.scheduled_date}"


class CandidateOutreachHistory(models.Model):
    """
    Track outreach history for each candidate to avoid duplicate outreach
    """
    contact_id = models.CharField(max_length=255)
    intern_role_id = models.CharField(max_length=255)
    outreach_log = models.ForeignKey(OutreachLog, on_delete=models.CASCADE, related_name='candidate_history')
    
    # Outreach cycle information
    cycle_number = models.IntegerField(default=1)  # 1st cycle, 2nd cycle, etc.
    initial_outreach_date = models.DateTimeField()
    last_follow_up_date = models.DateTimeField(blank=True, null=True)
    
    # Status tracking
    status = models.CharField(max_length=50, default='active')  # active, responded, completed, moved_to_next
    response_received = models.BooleanField(default=False)
    response_date = models.DateTimeField(blank=True, null=True)
    response_type = models.CharField(max_length=50, blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ['contact_id', 'intern_role_id', 'cycle_number']
        ordering = ['-created_at']

    def __str__(self):
        return f"Candidate {self.contact_id} → Role {self.intern_role_id} (Cycle {self.cycle_number})"