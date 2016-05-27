import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import os

def send_email(email_subject, filename_list, email_list, bcc_list=[]):
	
	msg = MIMEMultipart()
	attachments = filename_list

	#encode and attach each file
	print "adding attachments to msg..."
	for file in attachments:
		part = MIMEBase('application', "octet-stream")
		part.set_payload( open(file, "rb").read() )
		encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(file))
		msg.attach(part)

	#get uploads@wisepricer.com credentials
	email_from_display = os.environ['UL_USER']
	username = os.environ['UL_USER']
	password = os.environ['UL_PASS']

	#format emails to be displayed, NOT including bcc
	email_to_display = ", ".join(email_list)

	#format email
	msg["From"] = email_from_display
	msg["To"] = email_to_display
	msg["Subject"] = email_subject
	msg.preamble = email_subject #for when displaying msg in text editor

	#logging in to email server
	print "logging into email server..."
	server = smtplib.SMTP("smtp.gmail.com:587")
	server.starttls()
	server.login(username,password)

	#send mail request
	print "sending email request..."
	full_email_list = email_list + bcc_list
	server.sendmail(email_from_display, full_email_list, msg.as_string())

	print "sent message to %s" % email_list
	print "bcc: %s" % bcc_list
	server.quit()
