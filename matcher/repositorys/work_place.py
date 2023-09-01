from typing import List

from asyncpg import Connection

from matcher.models.WorkPlace import WorkPlace


def _get_work_place(work_place) -> WorkPlace:
    return WorkPlace(
        id=work_place['id'],
        name=work_place['name'],
        short_name=work_place['short_name']
    )

def _get_work_places(work_places) -> List[WorkPlace]:
    return [_get_work_place(work_place) for work_place in work_places]


class WorkPlaceRepo:
    """Db abstraction layer"""

    def __init__(self, conn: Connection):
        self.conn = conn

    async def get_work_place_by_t_user_id(self, t_user_id) -> List[WorkPlace]:
        return _get_work_places(await self.conn.fetch(
            'SELECT confirm_workplace.id as id, name, short_name  '
            'FROM confirm_isudata_work_places inner join confirm_workplace on confirm_isudata_work_places.workplace_id = confirm_workplace.id '
            'WHERE isudata_id = $1', t_user_id))
