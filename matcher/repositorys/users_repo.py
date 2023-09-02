from typing import List

from asyncpg import Connection, Record
from typeguard import check_type

from matcher.models.MyUser import MyUser, Sex, Role


def _get_my_user(user: Record) -> MyUser | None:
    if not user:
        return None
    check_type(user['t_user_id'], int)
    check_type(user['email'], str)
    check_type(user['full_name'], str)
    check_type(Sex(user['sex']), Sex)
    check_type(user['user_name'], str)
    # check_type('direction', Direction(user['direction']), Direction)
    # check_type('course', Course(user['course']), Course)
    check_type(user['user_info'], str)
    check_type(user['ban'], bool)
    check_type(user['is_student'], bool)
    check_type(user['is_worker'], bool)
    check_type(Role(user['role']), Role)
    check_type(user['old_user'], bool)

    return MyUser(t_user_id=user['t_user_id'],
                  email=user['email'],
                  full_name=user['full_name'],
                  sex=Sex(user['sex']),
                  user_name=user['user_name'],
                  # direction=Direction(user['direction']),
                  # course=Course(user['course']),
                  user_info=user['user_info'],
                  ban=user['ban'],
                  is_student=user['is_student'],
                  is_worker=user['is_worker'],
                  role=Role(user['role']),
                  old_user=user['old_user'])


def _get_my_users(users: list[Record]) -> List[MyUser]:
    return [_get_my_user(user) for user in users]


class UserRepo:
    """Db abstraction layer"""

    def __init__(self, conn: Connection):
        self.conn = conn

    async def get_by_email(self, email) -> MyUser | None:
        return _get_my_user(await self.conn.fetchrow('SELECT * FROM users WHERE email=$1', email))

    async def get_by_t_user_id(self, t_user_id) -> MyUser | None:
        return _get_my_user(await self.conn.fetchrow('SELECT * FROM users WHERE t_user_id=$1', t_user_id))

    async def upsert(self, my_user: MyUser) -> bool:
        return await self.conn.execute(
            """
            INSERT INTO users(
                t_user_id, user_name,
                email, full_name,
                user_info, sex, 
                is_student, is_worker, 
                role, ban, old_user)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(t_user_id) DO UPDATE SET
                (user_name, email, 
                full_name, 
                user_info, 
                sex, is_student, is_worker, role,
                ban, old_user) 
                    = 
                (excluded.user_name,excluded.email,
                excluded.full_name, excluded.user_info,
                excluded.sex, excluded.is_student, 
                excluded.is_worker, excluded.role, 
                excluded.ban, excluded.old_user);
            """,
            my_user.t_user_id, my_user.user_name,
            my_user.email, my_user.full_name,
            my_user.user_info,
            my_user.sex,
            my_user.is_student,
            my_user.is_worker,
            my_user.role,
            my_user.ban,
            my_user.old_user) == 'INSERT 0 1'
