from typing import List

from asyncpg import Connection

from matcher.models.Criterion import Criterion, MeetingFormat, Interest, PreferredPlaces


def skipp_empty(array):
    return filter(lambda a: a != '', array)

def _get_criterion(criterion):
    if not criterion:
        return None
    return Criterion(
        t_user_id=criterion['t_user_id'],
        interests=list(map(lambda interest: Interest(interest), skipp_empty(criterion['interests']))),
        meeting_format=MeetingFormat(criterion['meeting_format']),
        preferred_places=list(map(lambda preferred_place: PreferredPlaces(preferred_place), skipp_empty(criterion['preferred_places'])))
    )

def _get_criterions(criterions) -> List[Criterion]:
    return [_get_criterion(criterion) for criterion in criterions]


class CriterionRepo:
    """Db abstraction layer"""

    def __init__(self, conn: Connection):
        self.conn = conn

    async def upsert(self, criterion: Criterion) -> bool:
        return await self.conn.execute(
            """
            INSERT INTO criterion(
                t_user_id, interests,
                meeting_format, preferred_places)
            VALUES($1,$2,$3,$4) ON CONFLICT(t_user_id) DO UPDATE SET
                (t_user_id, interests, 
                meeting_format, preferred_places) 
                    = 
                (excluded.t_user_id,excluded.interests,
                excluded.meeting_format, excluded.preferred_places);
            """,
            criterion.t_user_id,
            list(map(lambda x: x.value, criterion.interests)),
            criterion.meeting_format,
            list(map(lambda x: x.value, criterion.preferred_places))) == 'INSERT 0 1'

    async def get_criterion_by_t_user_id(self, t_user_id: int) -> Criterion | None:
        return _get_criterion(await self.conn.fetchrow('SELECT * FROM criterion WHERE t_user_id=$1', t_user_id))
