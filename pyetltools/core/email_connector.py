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

    def send_email_to(self, to, subject, text):
        if self.method=="SMTP":
            self.send_email_to_via_smtp(to, subject, text)
        elif self.method=="OUTLOOK":
            self.send_email_to_via_outlook(to, subject, text)
        else:
            raise Exception(f"Unknown email sending method: {self.method}" )

    def _get_to_emails(self, to):
        if isinstance(to, list):
            if self.method=="OUTLOOK":
                return ";".join(to)
            else:
                return ",".join(to)
        else:
            return  to

    def send_email_to_via_smtp(self,to, subject, text):
        # Prepare actual message
        to_emails=self._get_to_emails(to)
        message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n\
    
        %s
        """ % (self.from_email, to_emails, subject, text)
        server = smtplib.SMTP(self.server)
        #server.ehlo()
        #server.starttls()
        server.login(self.smtp_username, self.get_password())
        server.sendmail(self.from_email, self.to_emails, message)
        server.quit()

    def send_email_to_via_outlook(self, to, subject, text):
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = self._get_to_emails(to)
        mail.Subject = subject
        mail.Body = text
        #mail.HTMLBody = '<h2>HTML Message body</h2>'  # this field is optional

        # To attach a file to the email (optional):
        #attachment = "Path to the attachment"
        # mail.Attachments.Add(attachment)

        mail.Send()