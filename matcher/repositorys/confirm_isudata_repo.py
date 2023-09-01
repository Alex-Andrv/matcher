from asyncpg import Connection, Record
from typeguard import check_type

from app.tgbot.models.IsuData import IsuData
from app.tgbot.models.MyUser import Sex


def _get_my_isudata(isudata: Record) -> IsuData | None:
    if not isudata:
        return None
    check_type('t_user_id', isudata['t_user_id'], int)
    check_type('sub', isudata['sub'], str)
    check_type('gender', isudata['gender'], str)
    check_type('name', isudata['name'], str)
    check_type('isu', isudata.get('isu'), int | None)
    check_type('preferred_username', isudata['preferred_username'], str)
    check_type('given_name', isudata['given_name'], str)
    check_type('middle_name', isudata.get('middle_name'), str | None)
    check_type('family_name', isudata['family_name'], str)
    check_type('email', isudata['email'], str)
    check_type('email_verified', isudata['email_verified'], bool)
    check_type('is_student', isudata['is_student'], bool)
    check_type('is_worker', isudata['is_worker'], bool)
    return IsuData(
        t_user_id = isudata['t_user_id'],
        sub = isudata['sub'],
        gender = Sex.MEN if isudata['gender'] == "male" else Sex.WOMEN,
        name = isudata['name'],
        isu = isudata.get('isu'),
        preferred_username = isudata['preferred_username'],
        given_name = isudata['given_name'],
        middle_name = isudata.get('middle_name'),
        family_name = isudata['family_name'],
        email = isudata['email'],
        email_verified = isudata['email_verified'],
        is_student = isudata['is_student'],
        is_worker = isudata['is_worker']
    )

class ConfirmIsudataRepo:
    def __init__(self, conn: Connection):
        self.conn = conn

    async def get_isu_data(self, t_user_id):
        return _get_my_isudata(await self.conn.fetchrow(
            """
            SELECT *
            FROM confirm_isudata
            WHERE t_user_id=$1
            """,
            t_user_id))