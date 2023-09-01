from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class WorkPlace:
    id: int
    name: str
    short_name: str