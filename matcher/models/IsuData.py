from dataclasses import dataclass

from app.tgbot.models.MyUser import Sex


@dataclass(slots=True, frozen=True)
class IsuData:
    t_user_id: int
    sub: str
    gender: Sex
    name: str
    isu: str | None
    preferred_username: str
    given_name: str
    middle_name: str | None
    family_name: str
    email: str
    email_verified: bool
    is_student: bool
    is_worker: bool

