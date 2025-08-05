from django.contrib import admin
from .models import Contact, Account, InternRole, Document, Skill, JobMatch, SyncTracker


@admin.register(Contact)
class ContactAdmin(admin.ModelAdmin):
    list_display = ['id', 'full_name', 'email', 'phone', 'student_status', 'placement_status', 'updated_time']
    list_filter = ['student_status', 'placement_status', 'industry', 'location']
    search_fields = ['full_name', 'email', 'phone', 'id']
    readonly_fields = ['id', 'created_time', 'updated_time']
    list_per_page = 50


@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ['id', 'name', 'industry', 'location', 'management_status', 'gold_rating']
    list_filter = ['industry', 'management_status', 'gold_rating', 'company_industry']
    search_fields = ['name', 'id', 'location']
    readonly_fields = ['id']
    list_per_page = 50


@admin.register(InternRole)
class InternRoleAdmin(admin.ModelAdmin):
    list_display = ['id', 'role_title', 'intern_company_name', 'location', 'role_status', 'start_date', 'gold_rating']
    list_filter = ['role_status', 'location', 'gold_rating', 'role_function']
    search_fields = ['role_title', 'intern_company_name', 'id']
    readonly_fields = ['id', 'created_time']
    list_per_page = 50


@admin.register(Document)
class DocumentAdmin(admin.ModelAdmin):
    list_display = ['contact_id', 'document_name', 'document_type', 'file_size', 'download_date']
    list_filter = ['document_type', 'download_date']
    search_fields = ['contact_id', 'document_name']
    readonly_fields = ['download_date', 'created_at', 'updated_at']
    list_per_page = 50


@admin.register(Skill)
class SkillAdmin(admin.ModelAdmin):
    list_display = ['contact_id', 'skill_name', 'skill_category', 'proficiency_level', 'extraction_method', 'confidence_score']
    list_filter = ['skill_category', 'proficiency_level', 'extraction_method']
    search_fields = ['contact_id', 'skill_name', 'skill_category']
    readonly_fields = ['created_at', 'updated_at']
    list_per_page = 50


@admin.register(JobMatch)
class JobMatchAdmin(admin.ModelAdmin):
    list_display = ['contact_id', 'intern_role_id', 'match_score', 'industry_match', 'location_match', 'skill_match', 'status']
    list_filter = ['status', 'industry_match', 'location_match', 'work_policy_match', 'skill_match']
    search_fields = ['contact_id', 'intern_role_id']
    readonly_fields = ['created_at', 'updated_at']
    list_per_page = 50
    
    def get_queryset(self, request):
        return super().get_queryset(request).order_by('-match_score')


@admin.register(SyncTracker)
class SyncTrackerAdmin(admin.ModelAdmin):
    list_display = ['entity_type', 'last_sync_timestamp', 'records_synced', 'updated_at']
    list_filter = ['entity_type']
    readonly_fields = ['created_at', 'updated_at']
