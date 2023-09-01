from aiogram.types import Message

from matcher.models.MyMessage import MyMessage
from matcher.utils.BotLogger import BotLogger, logging_decorator_factory

logger = BotLogger(__name__)

logging_decorator = logging_decorator_factory(logger)


@logging_decorator
async def save_sending_message_attribute(message: Message, state_data: dict):
    message_id = getattr(message, "message_id", None)
    chat = getattr(message, "chat", None)
    chat_id = getattr(chat, "id", None)
    if (message_id is not None) and (chat_id is not None):
        state_data['previous_message'] = MyMessage(
            message_id=message_id,
            chat_id=chat_id,
            with_reply_markup=message.reply_markup is not None,
            text=message.html_text,
            reply_markup=message.reply_markup if (message.reply_markup is not None) else None,
            parse_mode='HTML').to_dict()
        await logger.print_info("save message")
    else:
        await logger.print_error("Handler don't return send message: chat_id is None or message_id is None")
        # TODO need rise ERROR, but remember about barrier
    return state_data
