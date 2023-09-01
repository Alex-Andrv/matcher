from asyncpg import Connection

from app.tgbot.models.MyUser import MyUser


class VisitorRepo:
    """Db abstraction layer"""

    def __init__(self, conn: Connection):
        self.conn = conn

    async def insert_if_not_exists(self, t_user_id) -> MyUser | None:
        return await self.conn.fetchrow(
            'INSERT INTO visitors(t_user_id, time) VALUES($1,now()) ON CONFLICT DO NOTHING', t_user_id)

