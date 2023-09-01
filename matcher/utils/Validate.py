import re

from app.tgbot.utils.BotLogger import logging_decorator_factory, BotLogger

logger = BotLogger(__name__)

logging_decorator = logging_decorator_factory(logger)


class Validate:
    @staticmethod
    async def validate_full_name(full_name: str) -> bool:
        if len(full_name) > 100:
            await logger.print_warning("full_name len is bigger than 100")
            return False

        # _full_name_regex = re.compile(r'^[А-Яа-яёA-Za-z ]+$')
        _full_name_regex = re.compile(r'^[^_]+$')

        if re.match(_full_name_regex, full_name) is None:
            await logger.print_warning("full_name contains not valid simbols")
            return False

        return True

    @staticmethod
    async def take_reason_cancellation(text: str) -> str | None:

        if len(text) < 200:
            await logger.print_warning("message is too short")
            return f"Ожидается длина более 200 символов, ваше сообщение - {len(text)} символов"

        if len(text) > 1000:
            await logger.print_warning("user_info len is bigger than 1000")
            return f"Ожидается длина менее 1000 символов, ваше сообщение - {len(text)} символов"

        _text_regex = re.compile(r'^[^_]+$')

        if re.match(_text_regex, text) is None:
            await logger.print_warning(f"user_info contains not valid simbols: {text}")
            return "Сообщение содержит запрещенный символ _"

        return None

    @staticmethod
    async def validate_text(text: str) -> bool:
        if len(text) > 400:
            await logger.print_warning("user_info len is bigger than 400")
            return False

        _text_regex = re.compile(r'^[^_]+$')

        if re.match(_text_regex, text) is None:
            await logger.print_warning(f"user_info contains not valid simbols: {text}")
            return False

        return True

