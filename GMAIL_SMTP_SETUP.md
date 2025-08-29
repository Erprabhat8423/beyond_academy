# Gmail SMTP Setup Guide

This guide will help you configure Gmail SMTP for the outreach automation system.

## Prerequisites

1. Gmail account with 2-Factor Authentication enabled
2. Gmail App Password generated

## Step 1: Enable 2-Factor Authentication

1. Go to your [Google Account settings](https://myaccount.google.com/)
2. Navigate to **Security** → **2-Step Verification**
3. Follow the instructions to enable 2-Factor Authentication if not already enabled

## Step 2: Generate Gmail App Password

1. In Google Account settings, go to **Security**
2. Under **2-Step Verification**, click **App passwords**
3. Select **Mail** and **Other (custom name)**
4. Enter "Beyond Academy Outreach" as the app name
5. Click **Generate**
6. Copy the 16-character app password (remove spaces)

## Step 3: Configure Environment Variables

Edit your `.env` file and update the Gmail configuration:

```bash
# Gmail SMTP Configuration for Email Sending
GMAIL_EMAIL=your_actual_gmail_address@gmail.com
GMAIL_APP_PASSWORD=your_16_character_app_password_here
```

**Important Notes:**
- Use your actual Gmail address for `GMAIL_EMAIL`
- Use the 16-character app password (not your regular password) for `GMAIL_APP_PASSWORD`
- Remove any spaces from the app password

## Step 4: Test Configuration

Run the test script to verify your setup:

```bash
python test_gmail_smtp.py
```

## Step 5: Test Actual Email Sending

1. Edit `test_gmail_smtp.py`
2. Uncomment the lines in the `test_email_creation()` function:
   ```python
   # Uncomment these lines:
   print("\nSending test email...")
   success = automation.send_email(
       test_email_content,
       settings.EMAIL_HOST_USER,
       "Beyond Academy Test"
   )
   
   if success:
       print("✅ Test email sent successfully!")
       print(f"Check your inbox at {settings.EMAIL_HOST_USER}")
   else:
       print("❌ Failed to send test email")
       print("Check the logs above for error details")
   ```
3. Run the test again: `python test_gmail_smtp.py`
4. Check your Gmail inbox for the test email

## Email Features Implemented

### Message Threading
- **Message-ID**: Unique identifier for each email
- **Thread-ID**: Groups related emails together
- **In-Reply-To**: References parent message for follow-ups
- **References**: Builds email threading chain

### Attachments Support
- Automatically attaches candidate resumes
- Supports PDF attachments
- Error handling for missing files

### Professional Email Headers
- Proper sender name and reply-to configuration
- Custom headers for tracking
- RFC-compliant message formatting

## Email Templates

The system includes 4 email templates:

1. **Initial Outreach**: First contact with candidate information
2. **Follow-up 1**: Sent after 48 hours if no response
3. **Follow-up 2**: Sent after 96 hours (48h after follow-up 1)
4. **Follow-up 3**: Final follow-up after 144 hours

## Troubleshooting

### Common Issues

1. **Authentication Error**: 
   - Verify 2FA is enabled
   - Use app password, not regular password
   - Check for typos in email/password

2. **Connection Timeout**:
   - Check internet connection
   - Verify firewall settings
   - Try different EMAIL_TIMEOUT value

3. **Attachment Issues**:
   - Verify file paths exist
   - Check file permissions
   - Ensure files aren't too large (Gmail limit: 25MB)

### Test Commands

```bash
# Test Django email configuration
python manage.py shell
>>> from django.core.mail import send_mail
>>> send_mail('Test', 'Test message', 'from@example.com', ['to@example.com'])

# Test outreach automation
python manage.py run_etl --dry-run

# Test message tracking
python test_message_tracking.py
```

## Security Best Practices

1. **Never commit credentials**: Keep `.env` file in `.gitignore`
2. **Use app passwords**: Never use your main Gmail password
3. **Rotate passwords**: Regularly update app passwords
4. **Monitor usage**: Check Gmail sent folder for automated emails
5. **Rate limiting**: Respect Gmail's sending limits (500 emails/day for free accounts)

## Production Considerations

1. **Email Limits**: Gmail free accounts have daily sending limits
2. **Deliverability**: Consider using professional email service for high volume
3. **Monitoring**: Set up email delivery monitoring
4. **Backup**: Consider backup SMTP provider
5. **Compliance**: Ensure GDPR/CAN-SPAM compliance

## Next Steps

Once Gmail SMTP is configured:

1. Test with a small batch of emails
2. Monitor delivery rates and responses
3. Adjust email templates based on feedback
4. Set up email analytics if needed
5. Consider upgrading to Google Workspace for higher limits
