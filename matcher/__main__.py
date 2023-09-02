import asyncio
import logging
from datetime import datetime, timedelta
from typing import List
import json

import redis
import asyncpg
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from asyncpg import Connection

from matcher.opb_model.OpbTask import OpbTask
from matcher.configs.general_bot_config import DB_NAME, DB_USER, DB_HOST, DB_PASSWORD, DB_PORT, BOT_TOKEN, REDIS_DP, \
    REDIS_PASSWORD, REDIS_PORT, REDIS_HOST
from matcher.configs.log_config import LOG_LEVEL, LOG_FILEMODE, LOG_FILENAME, LOG_FORMAT
from matcher.models.Criterion import Criterion, MeetingFormat
from matcher.models.Group import Group
from matcher.models.MyUser import MyUser, Role
from matcher.models.WorkPlace import WorkPlace
from matcher.repositorys.criterion_repo import CriterionRepo
from matcher.repositorys.group_repo import GroupRepo
from matcher.repositorys.start_next_matching_algo_repo import NextMatchingRepo
from matcher.repositorys.users_repo import UserRepo
from matcher.repositorys.waiting_companions import WaitingCompanionRepo
from matcher.repositorys.work_place import WorkPlaceRepo
from matcher.utils.BotLogger import BotLogger
from matcher.utils.delete_button import delete_button_on_previous_message
from matcher.utils.save_message import save_sending_message_attribute

logging.basicConfig(
    level=LOG_LEVEL,
    filename=LOG_FILENAME,
    filemode=LOG_FILEMODE,
    format=LOG_FORMAT)

logger = BotLogger(name=__name__, extra=None, with_user_info=False)


async def get_postgres_connection():
    return await asyncpg.connect(
        dsn=f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


async def get_redis_connection():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DP, password=REDIS_PASSWORD, decode_responses=True)


async def get_next_matching_date():
    connection: Connection = await get_postgres_connection()
    next_matching: datetime = await NextMatchingRepo(connection).next_matching()
    await connection.close()
    return next_matching


async def get_homies(connection):
    rows = await connection.fetch(
        "SELECT meetings.t_user_id as t_user_id FROM feedbacks LEFT JOIN meetings on feedbacks.meeting_id=meetings.id WHERE is_meeting_took_place = true")
    return [row['t_user_id'] for row in rows]


async def get_waiting_companions(connection, next_matching):
    t_user_ids: List[int] = await WaitingCompanionRepo(connection).get_all_users_in_queue_less_time(next_matching)
    user_repo: UserRepo = UserRepo(connection)
    criterion_repo: CriterionRepo = CriterionRepo(connection)
    group_repo: GroupRepo = GroupRepo(connection)
    work_repo: WorkPlaceRepo = WorkPlaceRepo(connection)

    users = dict()

    for t_user_id in t_user_ids:
        user: MyUser = await user_repo.get_by_t_user_id(t_user_id)
        criterion: Criterion = await criterion_repo.get_criterion_by_t_user_id(t_user_id)
        groups: List[Group] = await group_repo.get_group_by_t_user_id(t_user_id)
        works: List[WorkPlace] = await work_repo.get_work_place_by_t_user_id(t_user_id)
        homies: List[int] = await get_homies(connection)
        users[t_user_id] = {"user": user, "criterion": criterion, "groups": groups, "works": works, "homies": homies}
    return users


async def update_next_matching(new_next_matching, connection):
    await NextMatchingRepo(connection).update_next_matching(new_next_matching)


def timestamp_to_week_day(timestamp: datetime):
    # Используем функцию weekday() для получения номера дня недели (понедельник = 0, воскресенье = 6)
    weekday_num = timestamp.weekday()
    day_name = ["понедельник", "вторник", "среду", "четверг", "пятницу", "субботу", "воскресенье"]

    # Используем список calendar.day_name для получения названия дня недели на основе номера
    return day_name[weekday_num]

async def get_fsm(redis_conn, free_user):
    data = redis_conn.get(f'fsm:{free_user}:{free_user}:data')
    state = redis_conn.get(f'fsm:{free_user}:{free_user}:state')
    return data, state

async def delete_and_change_state_message(bot, message, free_user, new_state: str):
    redis_conn = await get_redis_connection()
    data, state = get_fsm(redis_conn, free_user)
    json_data = json.loads(data)
    await delete_button_on_previous_message(bot, json_data)
    json_data = await save_sending_message_attribute(message, json_data)
    redis_conn.set(f'fsm:{free_user}:{free_user}:data', json_data.dump())
    redis_conn.set(f'fsm:{free_user}:{free_user}:state', new_state)
    redis_conn.close()

async def apologize_for_mismatching(free_users: List[int], new_next_matching):
    cancel_queue_buttons = InlineKeyboardMarkup()
    cancel_queue_buttons.add(InlineKeyboardButton(text="Покинуть очередь", callback_data='cancel_queue'))

    for free_user in free_users:
        bot = Bot(token=BOT_TOKEN)
        message = "К сожалению, на этот раз не получилось найти подходящего собеседника. 😔 Но не переживай! Следующий матчинг будет во {matching_date}, и мы надеемся, что он будет успешный! 🤝😊"
        message.format(matching_date=timestamp_to_week_day(new_next_matching))
        message = await bot.send_message(free_user, message, reply_markup=cancel_queue_buttons)
        await delete_and_change_state_message(bot, message, free_user, "ReadyStates:add_to_queue")


async def ping_user(bot: Bot, t_user_id: int, user_info: dict):
    message = "Я Нашел тебе напарника!\u2028\u2028 Это *{name}*\n*Пол*: {sex}\n*{direction_name}*: {direction}\n*О себе*: {info}\n *Интересы:* {interests}\n*Формат встречи: * {meeting_format}\n Напиши напарнику в [телеграм](https://t.me/{user_name}), и договоритесь о времени встречи или видеозвонка.\n\nВы можете устроить онлайн-встречу \uD83D\uDCBB или запланировать совместный кофе-брейк ☕️\n"

    if user_info['criterion'].meeting_format != MeetingFormat.ONLINE:
        message += f" *Где встретимся*: {'; '.join(map(lambda preferred_place: preferred_place.value, user_info['criterion'].preferred_places))}"

    my_user = user_info['user']

    if user_info['user']['role'] == Role.STUDENT:
        groups: List[Group] = user_info['groups']

        send_message = message.format(
            name=my_user.full_name,
            sex=my_user.sex.value,
            direction_name="Факультет",
            direction='; '.join(map(lambda group: group.faculty_name, groups)),
            info=my_user.user_info,
            interests=', '.join(map(lambda interest: interest.value, user_info['criterion'].interests)),
            meeting_format=user_info['criterion'].meeting_format)
    else:
        work_places: List[WorkPlace] = user_info['works']
        send_message = message.format(
            name=my_user.full_name,
            sex=my_user.sex.value,
            direction_name="Отдел",
            direction='; '.join(map(lambda work_place: work_place.name, work_places)),
            info=my_user.user_info,
            interests=', '.join(map(lambda interest: interest.value, user_info['criterion'].interests)),
            meeting_format=user_info['criterion'].meeting_format)

    await bot.send_message(
        t_user_id,
        send_message,
        parse_mode='Markdown')

    advise_message = "\uD83D\uDCA1 Если не знаешь, что написать собеседнику, то отправь это сообщение:\n\nПривет!\nБот @itmoffee_bot сказал, что мы коллеги. Удобно договориться о встрече сегодня или завтра?"

    start_approve_button = InlineKeyboardMarkup()
    start_approve_button.add(InlineKeyboardButton(text='Супер, понятно', callback_data='confirm'))

    return await bot.send_message(
        t_user_id,
        advise_message,
        reply_markup=start_approve_button)

async def sent_matching_result(matching, new_next_matching, users):
    for user1, user2 in matching:
        bot = Bot(token=BOT_TOKEN)
        message = await ping_user(bot, user1, users['user2'])
        await delete_and_change_state_message(bot, message, user1, "ApproveStates:approve")
        message = await ping_user(bot, user2, users['user1'])
        await delete_and_change_state_message(bot, message, user2, "ApproveStates:approve")


async def matching(users, new_next_matching):
    free_users, matching = OpbTask(users).solve()
    await apologize_for_mismatching(free_users, new_next_matching)
    await sent_matching_result(matching, new_next_matching, users)


async def get_ready_users(next_matching):
    connection: Connection = await get_postgres_connection()
    new_next_matching: datetime = next_matching + timedelta(days=7)
    await update_next_matching(new_next_matching, connection)
    users = await get_waiting_companions(connection, next_matching)
    await connection.close()
    return users, new_next_matching


async def run():
    while True:
        next_matching: datetime = await get_next_matching_date()
        now: datetime = datetime.now()
        await logger.print_info(
            f"next_matching = {next_matching.second} c, now = {now.second} c, need to wait = {(next_matching - now).total_seconds()} c")
        if next_matching > now:
            await asyncio.sleep((next_matching - now).total_seconds())
        await logger.print_info(f"matching start")

        users, new_next_matching = await get_ready_users(next_matching)

        await matching(users, new_next_matching)

if __name__ == '__main__':
    asyncio.run(run())
