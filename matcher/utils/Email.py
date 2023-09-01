import re

import smtplib
from random import randint
from email.mime.text import MIMEText

from app.configs.general_bot_config import MAIL_LOGIN, MAIL_PASSWORD, SMTP_SERVER, SMTP_PORT
from app.tgbot.utils.BotLogger import BotLogger
from app.tgbot.utils.message_loader import messages


class Email:
    _email_regex = re.compile(r'([A-Za-z\d]+[.-_])*[A-Za-z\d]+@(niuitmo.ru)$')

    def __init__(self):
        self.logger = BotLogger(__name__)
        self.sender = MAIL_LOGIN
        self.password = MAIL_PASSWORD

    async def send_email(self, address, code_message):
        server = None
        try:
            server = smtplib.SMTP(host=SMTP_SERVER, port=SMTP_PORT)
            server.starttls()
            server.login(self.sender, self.password)
            await self.logger.print_info(f'connected to the email server {self.sender}')
            msg = MIMEText(messages['2.6'].format(code=code_message))
            msg['Subject'] = 'Письмо с кодом подтверждения'
            server.sendmail(self.sender, address, msg.as_string())
            await self.logger.print_info(f'email {code_message} to {address} sent successfully')
        finally:
            if server is not None:
                server.quit()

    @staticmethod
    def is_valid_email(email) -> bool:
        return re.match(Email._email_regex, email) is not None

    @staticmethod
    def generate_code() -> str:
        res = ''
        for _ in range(6):
            res += str(randint(0, 9))
        return res
