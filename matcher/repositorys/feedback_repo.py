from typing import List

from asyncpg import Connection

from app.tgbot.models.Feedback import Feedback


def _get_feedback(feedback):
    return Feedback(
        t_user_id=feedback.t_user_id,
        meeting_id=feedback.meeting_id,
        is_meeting_took_place=feedback.is_meeting_took_place,
        rating=feedback.rating,
        cancellation_reason=feedback.cancellation_reason
    )


def _get_feedbacks(feedbacks) -> List[Feedback]:
    return [_get_feedback(feedback) for feedback in feedbacks]


class FeedbackRepo:
    """Db abstraction layer"""

    def __init__(self, conn: Connection):
        self.conn = conn

    async def add_feedback(self, feedback: Feedback) -> bool:
        return await self.conn.execute(
            """
            INSERT INTO feedbacks 
                (t_user_id, waiting_id,
                 is_meeting_took_place, 
                 rating, cancellation_reason) 
            VALUES ($1,$2,$3,$4,$5)
            """,
            feedback.t_user_id, feedback.meeting_id,
            feedback.is_meeting_took_place,
            feedback.rating,
            feedback.cancellation_reason) == 'INSERT 0 1'

    async def all_feedback_by_t_user_id(self, t_user_id: object) -> List[Feedback]:
        return _get_feedbacks(await self.conn.fetch('SELECT * FROM feedbacks WHERE t_user_id=$1', t_user_id))
