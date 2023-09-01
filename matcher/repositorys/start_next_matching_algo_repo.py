from datetime import datetime

from asyncpg import Connection


def _get_time(record):
    return record['next_matching']


class NextMatchingRepo:
    def __init__(self, conn: Connection):
        self.conn = conn

    async def next_matching(self) -> datetime:
        return _get_time(await self.conn.fetchrow(
            'SELECT next_matching FROM start_next_matching_algo'))

    async def update_next_matching(self, new_next_matching: datetime):
        return await self.conn.execute('UPDATE next_matching SET next_matching=$1', new_next_matching)


