from django.core.management.base import BaseCommand
from zoho_app.email_reply_parser import EmailReplyParser

class Command(BaseCommand):
    help = 'Processes email replies from the IMAP server.'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting email reply processing...'))
        parser = EmailReplyParser()
        result = parser.process_replies()
        self.stdout.write(self.style.SUCCESS(f'Processing complete: {result}'))
