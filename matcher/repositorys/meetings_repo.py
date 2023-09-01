from asyncpg import Connection


def _get_meeting_is(row):
    return row['id']


class MeetingRepo:
    """Db abstraction layer"""

    def __init__(self, conn: Connection):
        self.conn = conn

    async def get_last_meeting_id(self, t_user_id: int):
        return _get_meeting_is(await self.conn.fetchrow(
            'SELECT id '
            'FROM meetings '
            'WHERE t_user_id = $1 '
            'ORDER BY time_matching DESC ' 
            'LIMIT 1;',
            t_user_id))

    def get_by_members(self, first_user_id, second_user_id):
        assert first_user_id <= second_user_id
        return self.conn.fetch(
            'SELECT id, f.t_user_id as first_user_id, s.t_user_id as second_user_id, f.time as time '
            'FROM meetings as f INNER JOIN meetings as s USING(id) '
            'where f.t_user_id = $1 and s.t_user_id = $2 and f.t_user_id <> s.t_user_id',
            first_user_id, second_user_id)

    def add_meeting(self, first_user_id: int, second_user_id: int, waiting_id: int):
        assert first_user_id <= second_user_id

        return self.conn.fetchval(
            """with row as (
                    INSERT INTO meetings (t_user_id, time, waiting_id) VALUES ($1, now(), $3) RETURNING id)
                INSERT INTO meetings (id, t_user_id, time, waiting_id) (
                SELECT id, $2, now(), $3
                FROM row) RETURNING id;""",
            first_user_id, second_user_id, waiting_id)

    def get_by_meeting_id(self, meeting_id):
        return self.conn.fetch(
            'SELECT id, t_user_id, time '
            'FROM meetings where id = $1', meeting_id)

