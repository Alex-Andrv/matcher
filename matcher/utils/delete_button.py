from aiogram import Bot
from aiogram.utils.exceptions import MessageToEditNotFound, MessageNotModified, MessageToDeleteNotFound, \
    MessageCantBeDeleted
from typeguard import check_type

from matcher.utils.BotLogger import BotLogger, logging_decorator_factory
from matcher.utils.save_message import MyMessage

logger = BotLogger(__name__)

logging_decorator = logging_decorator_factory(logger)


@logging_decorator
async def delete_button_on_previous_message(bot: Bot, state_data: dict):
    previous_message: MyMessage = MyMessage.from_dict(state_data.get('previous_message', None))
    if previous_message is not None:
        check_type(previous_message, MyMessage)
        previous_message_id: int = previous_message.message_id
        chat_id: int = previous_message.chat_id
        with_reply_markup: bool = previous_message.with_reply_markup
        # if with_reply_markup:
        try:
            await logger.print_info("drop reply_markup")
            await bot.delete_message(chat_id, previous_message_id)
        except MessageToEditNotFound:
            await logger.print_error(str(MessageToEditNotFound))
            # Может быть валидным поведением, но пока пусть будет error
        except MessageNotModified:
            await logger.print_error(str(MessageNotModified))
        except MessageToDeleteNotFound:
            await logger.print_error(str(MessageToDeleteNotFound))
        except MessageCantBeDeleted:
            await logger.print_error(str(MessageCantBeDeleted) + " " + "may be 48 hour 48 hours have passed")
            if with_reply_markup:
                try:
                    await bot.edit_message_reply_markup(chat_id, previous_message_id)
                except MessageToEditNotFound:
                    await logger.print_error(str(MessageToEditNotFound))
                    # Может быть валидным поведением, но пока пусть будет error
                except MessageNotModified:
                    await logger.print_error(str(MessageNotModified))
                except MessageCantBeDeleted:
                    await logger.print_error(str(MessageToDeleteNotFound))

    else:
        await logger.print_warning("previous_message is None")
    return state_data
