import smtplib
import win32com.client as win32

from pyetltools.core.connector import Connector


class EmailConnector(Connector):
    def __init__(self, key, server, smtp_username, from_email, to_emails=None, method="SMTP"):
        super().__init__(key),
        self.server = server
        self.from_email=from_email
        self.to_emails=to_emails
        self.smtp_username=smtp_username
        self.method=method


    def validate_config(self):
        assert self.server, "Server cannot be None"
        assert self.from_email, "From email cannot be none"
        if not self.method in ["SMTP","OUTLOOK"]:
            raise Exception("Email connector sending method not set to either SMTP or OUTLOOK.")
        super().validate_config()


    def send_email(self, subject, text):
        assert self.to_emails, "To email cannot be None"
        return self.send_email_to(self.to_emails, subject, text)

    def send_email_to(self, to, subject, text, text_html=None, attachments=[]):
        self.send_email_to_by(self.method, to, subject, text, text_html, attachments)

    def send_email_to_by(self, method, to, subject, text, text_html, attachments=[]):
        if method=="SMTP":
            send_email_to_via_smtp(to, subject, text, text_html)
        elif method=="OUTLOOK":
            send_email_to_via_outlook(to, subject, text, text_html,  attachments)
        else:
            raise Exception(f"Unknown email sending method: {self.method}" )


    def send_email_to(self, to, subject, text,text_html=None,attachments=[]):
        if self.method=="SMTP":
            self.send_email_to_via_smtp(self.from_email, to, subject, text,self.server,self.smtp_username, self.get_password())
        elif self.method=="OUTLOOK":
            send_email_to_via_outlook(to, subject, text, text_html, attachments)
        else:
            raise Exception(f"Unknown email sending method: {self.method}" )



def _get_to_emails( to, method):
    if isinstance(to, list):
        if method=="OUTLOOK":
            return ";".join(to)
        else:
            return ",".join(to)
    else:
        return  to

def send_email_to_via_outlook( to, subject, text, text_html=None, attachments=[]):
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = _get_to_emails(to, "OUTLOOK")
    mail.Subject = subject
    mail.Body = text
    if text_html:
        mail.HTMLBody = text_html

    for at in attachments:
        mail.Attachments.Add(at)

    mail.Send()

def send_email_to_via_smtp(from_email, to, subject, text, server, smtp_username, password ):
    # Prepare actual message
    to_emails = _get_to_emails(to, "SMTP")
    message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n\

    %s
    """ % (from_email, to_emails, subject, text)
    server = smtplib.SMTP(server)
    # server.ehlo()
    # server.starttls()
    server.login(smtp_username, password)
    server.sendmail(from_email, to_emails, message)
    server.quit()