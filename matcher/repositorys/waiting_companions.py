from datetime import datetime

from asyncpg import Connection


def _get_matching_time(row):
    return row['matching_time']


def _get_list_users(rows):
    return [row['t_user_id'] for row in rows]


class WaitingCompanionRepo:
    """Db abstraction layer"""

    def __init__(self, conn: Connection):
        self.conn = conn

    async def upsert_user_in_queue(self, t_user_id, matching_time: datetime) -> bool:
        return await self.conn.execute(
            """
            INSERT INTO waiting_companions(
                 t_user_id, matching_time)
            VALUES($1,$2) ON CONFLICT(t_user_id) DO UPDATE SET
                (t_user_id, matching_time) 
                    = 
                (excluded.t_user_id,excluded.matching_time);
            """,
        t_user_id, matching_time) == 'INSERT 0 1'

    async def delete_user_from_queue(self, t_user_id: int) -> bool:
        return await self.conn.execute(
            """
            DELETE FROM waiting_companions 
               WHERE t_user_id=$1
            """, t_user_id) == 'DELETE 1'

    async def get_matching_time_by_t_user_id(self, t_user_id) -> datetime:
        return _get_matching_time(await self.conn.fetchrow(
            """
            SELECT matching_time FROM waiting_companions 
               WHERE t_user_id=$1
            """, t_user_id))

    async def get_all_users_in_queue_less_time(self, next_matching: datetime):
        return _get_list_users(await self.conn.fetch(
            """
            SELECT t_user_id FROM waiting_companions WHERE time <= $1
            """, next_matching))