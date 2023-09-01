from dataclasses import dataclass
from datetime import datetime

from app.tgbot.models.Criterion import Criterion


@dataclass(slots=True, frozen=True)
class WaitingCompanion:
    id: int
    t_user_id: int
    time: datetime
    criterion: Criterion
    status: int | None
