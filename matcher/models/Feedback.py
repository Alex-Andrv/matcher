from dataclasses import dataclass
from typing import Self

from typeguard import typechecked


@dataclass(slots=True, frozen=True)
class Feedback:
    t_user_id: int
    meeting_id: int
    is_meeting_took_place: bool
    rating: int | None
    cancellation_reason: str | None


class FeedbackBuilder:
    t_user_id: int | None
    meeting_id: int | None
    is_meeting_took_place: bool | None
    rating: int | None = None
    cancellation_reason: str | None = None

    @typechecked
    def set_t_user_id(self, t_user_id: int) -> Self:
        self.t_user_id = t_user_id
        return self

    @typechecked
    def set_meeting_id(self, meeting_id: int) -> Self:
        self.meeting_id = meeting_id
        return self

    @typechecked
    def set_is_meeting_took_place(self, is_meeting_took_place: bool) -> Self:
        self.is_meeting_took_place = is_meeting_took_place
        return self

    @typechecked
    def set_rating(self, rating: int) -> Self:
        self.rating = rating
        return self

    @typechecked
    def set_cancellation_reason(self, cancellation_reason: str) -> Self:
        self.cancellation_reason = cancellation_reason
        return self

    @typechecked
    def to_feedback(self) -> Feedback:
        return Feedback(
            t_user_id=self.t_user_id,
            meeting_id=self.meeting_id,
            is_meeting_took_place=self.is_meeting_took_place,
            rating=self.rating,
            cancellation_reason=self.cancellation_reason)
