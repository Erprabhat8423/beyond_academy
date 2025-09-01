"""
Email Reply Parser

This module handles parsing email replies from an IMAP mailbox to track responses
to outreach campaigns.
"""

import email
import imaplib
import logging
import time
from email.header import decode_header
from typing import Dict, Any, Optional, List

from django.utils import timezone
from django.conf import settings

from zoho_app.models import OutreachLog

logger = logging.getLogger(__name__)


class EmailReplyParser:
    """
    Parses email replies from an IMAP mailbox and updates outreach logs.
    """

    def __init__(self):
        self.imap_server = settings.IMAP_SERVER
        self.imap_user = settings.IMAP_USER
        self.imap_password = settings.IMAP_PASSWORD
        self.mail = None

    def connect(self):
        """Connect to the IMAP server."""
        try:
            self.mail = imaplib.IMAP4_SSL(self.imap_server)
            self.mail.login(self.imap_user, self.imap_password)
            logger.info("Successfully connected to IMAP server.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IMAP server: {e}")
            return False

    def reconnect(self):
        """Reconnect to the IMAP server."""
        logger.info("Reconnecting to IMAP server...")
        self.logout()
        return self.connect()

    def logout(self):
        """Logout from the IMAP server if connected."""
        if self.mail:
            try:
                self.mail.logout()
            except Exception as e:
                logger.warning(f"Error during logout, connection might be already closed: {e}")
        self.mail = None

    def process_replies(self) -> Dict[str, Any]:
        """
        Process all unread emails, identify replies, and update outreach logs.
        """
        # Configurable batch size and delay between batches
        batch_size = getattr(settings, 'IMAP_BATCH_SIZE', 20)
        batch_delay = getattr(settings, 'IMAP_BATCH_DELAY_SECONDS', 1)

        # First connect briefly to get the list of unread IDs, then disconnect.
        if not self.connect():
            return {'status': 'error', 'message': 'IMAP connection failed'}

        try:
            self.mail.select('inbox')
            status, messages = self.mail.search(None, 'UNSEEN')
            if status != 'OK':
                logger.error("Failed to search for emails.")
                self.logout()
                return {'status': 'error', 'message': 'Failed to search for emails'}

            email_ids = messages[0].split()
            total_unread = len(email_ids)
            logger.info(f"Found {total_unread} unread emails to process.")

        except Exception as e:
            logger.error(f"An error occurred while listing unread emails: {e}")
            self.logout()
            return {'status': 'error', 'message': str(e)}
        finally:
            # close the short-lived connection used to list IDs
            self.logout()

        processed_count = 0

        # Process IDs in batches, establishing a fresh connection per batch
        for start in range(0, total_unread, batch_size):
            end = min(start + batch_size, total_unread)
            batch = email_ids[start:end]
            batch_number = (start // batch_size) + 1
            logger.info(f"Processing batch {batch_number}: emails {start + 1}-{end} of {total_unread}")

            if not self.connect():
                logger.error("Failed to connect for batch processing. Stopping.")
                break

            try:
                # Ensure mailbox selected for this connection
                self.mail.select('inbox')

                for email_id in batch:
                    try:
                        if self.process_single_email(email_id):
                            processed_count += 1
                    except imaplib.IMAP4.abort as e:
                        logger.warning(f"IMAP abort while processing {email_id.decode()}: {e}. Will reconnect and retry once.")
                        # try once after reconnect
                        self.logout()
                        if self.connect():
                            try:
                                self.mail.select('inbox')
                                if self.process_single_email(email_id):
                                    processed_count += 1
                            except Exception as e2:
                                logger.error(f"Retry failed for email {email_id.decode()}: {e2}")
                        else:
                            logger.error("Reconnect failed during abort handling.")
                    except Exception as e:
                        logger.error(f"Error processing email ID {email_id.decode()}: {e}")

            except Exception as e:
                logger.error(f"Batch {batch_number} processing error: {e}")
            finally:
                # Cleanly logout after each batch
                self.logout()

            # Small delay to avoid hammering the server
            time.sleep(batch_delay)

        return {
            'status': 'success',
            'total_unread': total_unread,
            'replies_processed': processed_count
        }

    def process_single_email(self, email_id: bytes) -> bool:
        """
        Fetch, parse, and process a single email.
        """
        try:
            # First, fetch only the headers to check if it's a reply
            status, header_data = self.mail.fetch(email_id, '(BODY[HEADER.FIELDS (IN-REPLY-TO REFERENCES)])')
            if status != 'OK':
                logger.error(f"Failed to fetch headers for email {email_id.decode()}.")
                return False

            header_text = header_data[0][1].decode('utf-8')
            msg_headers = email.message_from_string(header_text)

            in_reply_to = msg_headers.get('In-Reply-To')
            references = msg_headers.get('References')

            outreach_log = self.find_outreach_log(in_reply_to, references)

            if not outreach_log:
                # Not a reply to a tracked email, so we can skip it.
                # We don't mark as seen, in case it's a reply to a future outreach.
                return False

            logger.info(f"Found reply for OutreachLog ID {outreach_log.id}. Fetching full email.")

            # Now fetch the full email since we know it's a reply we care about
            status, msg_data = self.mail.fetch(email_id, '(RFC822)')
            if status != 'OK':
                logger.error(f"Failed to fetch full email for {email_id.decode()}.")
                return False

            msg = email.message_from_bytes(msg_data[0][1])
            message_id_of_reply = msg.get('Message-ID')
            
            # Extract email body
            body = self.get_email_body(msg)
            
            # Update the outreach log using existing model fields
            # Note: OutreachLog has fields `response_received`, `response_date`, and `response_type`.
            outreach_log.response_received = True
            outreach_log.response_date = timezone.now()
            # set a generic response type; downstream code can refine this if needed
            outreach_log.response_type = 'reply'

            # Persist a short excerpt of the reply body into error_message (avoid schema change here).
            # If desired later, add a dedicated `response_content` TextField to the model and store full body.
            try:
                existing = outreach_log.error_message or ''
                snippet = body[:2000] if body else ''
                outreach_log.error_message = (existing + '\n\n[reply captured] ' + snippet).strip()
            except Exception:
                # Fallback: ignore storing body if any unexpected issue
                pass

            # Save reply message id into message_id field of the reply log if not present.
            # We avoid changing outgoing message_id; instead, store reply id in error_message as well.
            if message_id_of_reply:
                outreach_log.error_message = (outreach_log.error_message or '') + f"\n\n[reply_message_id] {message_id_of_reply}"

            outreach_log.save()

            logger.info(f"Updated OutreachLog {outreach_log.id} with reply information.")
            
            # Mark email as seen
            self.mail.store(email_id, '+FLAGS', '\\Seen')
            return True

        except Exception as e:
            logger.error(f"Error processing email ID {email_id.decode()}: {e}")
            return False

    def find_outreach_log(self, in_reply_to: Optional[str], references: Optional[str]) -> Optional[OutreachLog]:
        """
        Find the original OutreachLog based on In-Reply-To or References headers.
        """
        if in_reply_to:
            # The In-Reply-To header should contain the Message-ID of the email it's replying to.
            try:
                log = OutreachLog.objects.get(message_id=in_reply_to)
                return log
            except OutreachLog.DoesNotExist:
                pass

        if references:
            # The References header can contain a list of Message-IDs in the thread.
            # We check each one to find a match.
            reference_ids = references.split()
            for ref_id in reference_ids:
                try:
                    log = OutreachLog.objects.get(message_id=ref_id)
                    return log
                except OutreachLog.DoesNotExist:
                    continue
        
        return None

    def get_email_body(self, msg: email.message.Message) -> str:
        """
        Extract the text body from an email message.
        """
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get('Content-Disposition'))

                if content_type == 'text/plain' and 'attachment' not in content_disposition:
                    charset = part.get_content_charset()
                    if charset:
                        try:
                            body = part.get_payload(decode=True).decode(charset)
                            break
                        except (UnicodeDecodeError, AttributeError):
                            body = part.get_payload(decode=True).decode('latin-1') # fallback
                            break
                    else:
                        body = part.get_payload(decode=True).decode()
                        break
        else:
            charset = msg.get_content_charset()
            if charset:
                try:
                    body = msg.get_payload(decode=True).decode(charset)
                except (UnicodeDecodeError, AttributeError):
                    body = msg.get_payload(decode=True).decode('latin-1') # fallback
            else:
                body = msg.get_payload(decode=True).decode()
        return body
