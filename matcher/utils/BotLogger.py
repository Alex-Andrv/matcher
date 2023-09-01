import asyncio
import logging
import time
from functools import wraps

from aiogram import Bot, types, Dispatcher
from aiogram.dispatcher import FSMContext

# Создаем бота для алярмов
from aiogram.types import User

from matcher.configs.general_bot_config import ALARM_BOT_TOKEN, CHAT_ID_ALARM, CHAT_ID_ALARM_WITH_HR


class BotLogger(logging.LoggerAdapter):

    def __init__(self, name: str, extra=None, with_user_info: bool = True):
        super().__init__(logging.getLogger(name), extra or {})
        self.alarm_bot: Bot = Bot(token=ALARM_BOT_TOKEN)
        self.with_user_info = with_user_info

    async def _get_current_user_info(self) -> str:
        user: types.User = types.User.get_current()
        dispatcher = Dispatcher.get_current()
        if dispatcher and user and self.with_user_info:
            state: FSMContext = dispatcher.current_state()
            data: dict = await state.get_data()
            state_str = await state.get_state()
            return f"user = {user}, state = {state_str}, data = {data} "
        else:
            return ""

    async def print_warning(self, message: str, stacklevel: int = 4):
        message = message + " " + (await self._get_current_user_info())
        print(f'\033[0;33;40m[WARNING]: {message} \033[0;0m')
        self.warning(message, stacklevel=stacklevel)

    async def print_error(self, message: str, stacklevel: int = 4):
        message = message + " " + (await self._get_current_user_info())
        print(f'\033[0;31;40m[ERROR]: {message} \033[0;0m')
        self.error(message, exc_info=True, stacklevel=stacklevel)

        await self.alarm_bot.send_message(CHAT_ID_ALARM, message)
        await self.alarm_bot.send_message(CHAT_ID_ALARM, "https://www.youtube.com/watch?v=sjakGpdgWUw")

    async def print_info(self, message: str, stacklevel: int = 4):
        message = message + " " + (await self._get_current_user_info())
        print(f'\033[0;34m[INFO]: {message} \033[0;0m')
        self.info(message, stacklevel=stacklevel)

    async def print_dev(self, message: str):
        message = message + " " + (await self._get_current_user_info())
        print(f'\033[0;36m[DEV]: {message} \033[0;0m')

    async def send_suggestion(self, text):
        user: User = User.get_current()
        user_name: str = user.username
        t_user_id: int = user.id
        await self.alarm_bot.send_message(CHAT_ID_ALARM_WITH_HR,
                                          f"""
        Предложение от нашего замечательного пользователя: {text} \n 
        user: {user_name}
        t_user_id: {t_user_id}
        """)

    async def change_user_info(self, text):
        user: User = User.get_current()
        user_name: str = user.username
        t_user_id: int = user.id
        await self.alarm_bot.send_message(CHAT_ID_ALARM_WITH_HR,
                                          f"""
           Пользователь изменил информацию о себе: {text} \n 
           user: {user_name}
           t_user_id: {t_user_id}
           """)

    def __del__(self):
        asyncio.run(self.alarm_bot.close())


def logging_decorator_factory(logger: BotLogger):
    def logging_decorator(function_to_decorate):
        @wraps(function_to_decorate)
        async def a_wrapper_accepting_arbitrary_arguments(*args, **kwargs):
            start = time.time()
            await logger.print_info(f"start execution {function_to_decorate.__name__}, execution start time = {start}")
            res = await function_to_decorate(*args, **kwargs)
            end = time.time()
            delta = end - start
            await logger.print_info(f"finish execution {function_to_decorate.__name__}, execution time = {delta}")
            return res

        return a_wrapper_accepting_arbitrary_arguments

    return logging_decorator
