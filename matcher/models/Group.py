from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class Group:
    name: str
    course: int
    faculty_name: str
    qualification_name: str | None
