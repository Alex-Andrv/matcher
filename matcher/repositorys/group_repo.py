from typing import List

from asyncpg import Connection

from matcher.models.Group import Group


def _get_group(group) -> Group:
    return Group(
        name=group['name'],
        course=group['course'],
        faculty_name=group['faculty_name'],
        qualification_name=group['qualification_name']
    )


def _get_groups(groups) -> List[Group]:
    return [_get_group(group) for group in groups]


class GroupRepo:
    """Db abstraction layer"""

    def __init__(self, conn: Connection):
        self.conn = conn

    async def get_group_by_t_user_id(self, t_user_id) -> List[Group]:
        return _get_groups(await self.conn.fetch(
            'SELECT name, course, faculty_name, qualification_name  '
            'FROM confirm_isudata_groups inner join confirm_group on confirm_isudata_groups.group_id = confirm_group.name '
            'WHERE isudata_id = $1', t_user_id))
