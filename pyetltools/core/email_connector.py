import smtplib

from pyetltools.core.connector import Connector


class EmailConnector(Connector):
    def __init__(self, key, server, smtp_username, from_email, to_emails=None):
        super().__init__(key),
        self.server = server
        self.from_email=from_email
        self.to_emails=to_emails
        self.smtp_username=smtp_username

    def validate_config(self):
        assert self.server, "Server cannot be None"
        assert self.from_email, "From email cannot be none"
        super().validate_config()


    def send_email(self, subject, text):
        assert self.to_emails, "To email cannot be None"
        return self.send_email_to(self.to_emails, subject, text)

    def send_email_to(self, to, subject, text):
        # Prepare actual message
        message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n\
    
        %s
        """ % (self.from_email, ", ".join(self.to_emails), subject, text)
        server = smtplib.SMTP(self.server)
        #server.ehlo()
        #server.starttls()
        server.login(self.smtp_username, self.get_password())
        server.sendmail(self.from_email, self.to_emails, message)
        server.quit()
