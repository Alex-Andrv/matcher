import typing
from dataclasses import dataclass
from enum import Enum
from typing import List, Self, Any

from typeguard import typechecked, check_type

from matcher.exseptions.exseptions import CriterionBuilderConvertError


class Interest(str, Enum):
    ART = "искусство"
    SCIENCE = "наука"
    SPORT = "спорт"
    ENTREPRENEURSHIP = "предпринимательство"
    PROGRAMMING = "программирование"
    ADVENTURES = "путешествия и приключения"
    GAMES = "видеоигры"
    MOVIE = "кино и сериалы"
    PETS = "питомцы"


class MeetingFormat(str, Enum):
    OFFLINE = 'очно',
    ONLINE = 'онлайн'
    ANY = 'неважно'


class PreferredPlaces(str, Enum):
    KRONVERKSKY = 'Кронверкский проспект 49',
    LOMONOSOVA = 'улица Ломоносова 9',
    BIRZHEVAYA = 'Биржевая линия 14-16',
    GRIVTSOVA = 'переулок Гривцова 14-16, лит. А',
    TCHAIKOVSKY = 'улица Чайковского 11/2, лит. А',
    PESOCHNAYA = 'Песочная набережная 14',
    KHRUSTALNAYA = 'Хрустальная улица 14 лит. А'


@dataclass(slots=True, frozen=True)
class Criterion:
    t_user_id: int
    interests: List[Interest]
    meeting_format: MeetingFormat
    preferred_places: List[PreferredPlaces]


class CriterionBuilder:
    t_user_id: int
    interests: typing.Set[Interest]
    meeting_format: MeetingFormat | None
    preferred_places: typing.Set[PreferredPlaces]

    def __init__(self, t_user_id):
        self.t_user_id = t_user_id
        self.interests = set()
        self.preferred_places = set()
        self.meeting_format = None

    @typechecked
    def xor_interests(self, new_interests: List[Interest]):
        assert new_interests is not None
        for new_interest in new_interests:
            if new_interest in self.interests:
                self.interests.remove(new_interest)
            else:
                self.interests.add(new_interest)
        return self

    @typechecked
    def set_meeting_format(self, meeting_format: MeetingFormat) -> Self:
        self.meeting_format = meeting_format
        return self

    @typechecked
    def xor_preferred_places(self, preferred_places: List[PreferredPlaces]):
        assert preferred_places is not None
        for preferred_place in preferred_places:
            if preferred_place in self.preferred_places:
                self.preferred_places.remove(preferred_place)
            else:
                self.preferred_places.add(preferred_place)
        return self

    @typechecked
    def to_criterion(self) -> Criterion:
        if not (self.meeting_format and self.t_user_id):
            raise CriterionBuilderConvertError("Can't convert builder to criterion, because builder contain not set value")
        return Criterion(self.t_user_id, list(self.interests), self.meeting_format, list(self.preferred_places))

    @staticmethod
    @typechecked
    def from_criterion(criterion: Criterion) -> Self:
        return CriterionBuilder(criterion.t_user_id) \
            .xor_interests(criterion.interests) \
            .set_meeting_format(criterion.meeting_format) \
            .xor_preferred_places(criterion.preferred_places)

    def to_dict(self) -> dict[str, Any]:
        # вообще не факт что это говно работает TODO
        return {
            't_user_id': self.t_user_id,
            'interests': list(self.interests),
            'meeting_format': self.meeting_format,
            'preferred_places': list(self.preferred_places)
        }

    @staticmethod
    @typechecked
    def from_dict(message: dict | None) -> Self:
        # вообще не факт что это говно работает TODO
        if not message:
            return None

        check_type('t_user_id', message['t_user_id'], int)
        check_type('interests', message['interests'], List[str])
        check_type('meeting_format', message['meeting_format'], str | None)
        check_type('preferred_places', message['preferred_places'], List[str])

        t_user_id: int | None = typing.cast(int | None, message['t_user_id'])
        my_builder: CriterionBuilder = CriterionBuilder(t_user_id)

        interests: List[str] = typing.cast(List[str], message['interests'])
        my_builder.xor_interests(list(map(lambda interest: Interest(interest), interests)))

        meeting_format: str | None = typing.cast(str | None, message['meeting_format'])
        if meeting_format:
            my_builder.set_meeting_format(MeetingFormat(meeting_format))

        preferred_places: List[str] = typing.cast(List[str], message['preferred_places'])
        my_builder.xor_preferred_places(list(map(lambda preferred_place: PreferredPlaces(preferred_place), preferred_places)))

        return my_builder

