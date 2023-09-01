from aiogram.dispatcher import FSMContext

from app.tgbot.utils.BotLogger import BotLogger

logger = BotLogger(__name__)


async def get_attr_from_state(state: FSMContext, attribute: str) -> str | dict | int | None:
    await logger.print_info(f"get from state attribute = {attribute}")
    try:
        async with state.proxy() as data:
            return data[attribute]
    except KeyError as e:
        await logger.print_error(f"can't get attribute = {attribute}")
        raise KeyError(e)


async def get_attr_from_state_with_default(state: FSMContext, attribute: str, default=None) -> \
        str | int | dict | None:
    await logger.print_info(f"get from state attribute = {attribute}")
    async with state.proxy() as data:
        return data.get(attribute, default)


async def set_attr_to_state(state: FSMContext, attribute: str, value: str | dict | int | None):
    await logger.print_info(f"set state attribute = {attribute}, value = {value}")
    async with state.proxy() as data:
        data[attribute] = value
