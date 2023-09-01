import typing
from dataclasses import dataclass
from enum import Enum
from typing import Self, Any

from ordered_enum import OrderedEnum
from typeguard import typechecked, check_type

from matcher.exseptions.exseptions import UserBuilderConvertError


# https://blog.yossarian.net/2020/03/02/Totally-ordered-enums-in-python-with-ordered_enum - more info
class Course(str, OrderedEnum):
    FIRST = "1 курс"  # 1 курс
    SECOND = "2 курс"  # 2 курс
    THIRD = "3 курс"  # 3 курс
    FOURTH = "4 курс"  # 4 курс
    MASTER = "Магистр"  # Магистр
    PHD = "Аспирант"  # Аспирант
    GRADUATE = "Выпускник"  # Выпускник

class Role(str, Enum):
    STUDENT = "студент"
    WORKER = "работник"

class Direction(str, Enum):
    KTiU = "КТиУ"
    TINT = "ТИНТ"
    NoZh = "НоЖ"
    FTMF = "ФТМФ"
    IVITSH = "ИВИТШ"
    IMRIP = "ИМРиП"
    FTMI = "ФТМИ"
    CPO = "ЦПО"
    VShTsK = "ВШЦК"
    OC_EIS = "ОЦ ЭИС"


class Sex(str, Enum):
    MEN = "Мужской",
    WOMEN = "Женский"

@dataclass(slots=True, frozen=True)
class MyUser:
    t_user_id: int
    email: str
    full_name: str
    sex: Sex
    user_name: str
    user_info: str
    ban: bool
    is_student: bool
    is_worker: bool
    role: Role
    old_user: bool


class MyUserBuilder:
    t_user_id: int | None = None
    email: str | None = None
    full_name: str | None = None
    sex: Sex | None = None
    user_name: str | None = None
    user_info: str | None = None
    ban: bool = False
    is_student: bool = False
    is_worker: bool = False
    role: Role | None = None
    old_user: bool = False

    @typechecked
    def set_t_user_id(self, t_user_id: int) -> Self:
        self.t_user_id = t_user_id
        return self

    @typechecked
    def set_email(self, email: str) -> Self:
        self.email = email
        return self

    @typechecked
    def set_full_name(self, full_name: str) -> Self:
        self.full_name = full_name
        return self

    @typechecked
    def set_sex(self, sex: Sex) -> Self:
        self.sex = sex
        return self

    @typechecked
    def set_user_name(self, user_name: str) -> Self:
        self.user_name = user_name
        return self

    @typechecked
    def set_user_info(self, user_info: str) -> Self:
        self.user_info = user_info
        return self

    @typechecked
    def set_ban(self, ban: bool) -> Self:
        self.ban = ban
        return self

    @typechecked
    def set_is_student(self, is_student: bool) -> Self:
        self.is_student = is_student
        return self

    @typechecked
    def set_is_work(self, is_worker: bool) -> Self:
        self.is_worker = is_worker
        return self

    @typechecked
    def set_role(self, role: Role) -> Self:
        self.role = role
        return self

    @typechecked
    def set_old_user(self, old_user: bool) -> Self:
        self.old_user = old_user
        return self

    @typechecked
    def to_user(self) -> MyUser:
        if not (self.t_user_id and self.email and
                self.full_name and self.sex and
                self.user_name and
                # self.direction and self.course and
                self.user_info and self.role):
            raise UserBuilderConvertError("Can't convert builder to user, because builder contain not set value")
        return MyUser(self.t_user_id, self.email,
                      self.full_name, self.sex,
                      self.user_name,
                      # self.direction, self.course,
                      # self.interest,
                      self.user_info, self.ban,
                      self.is_student, self.is_worker,
                      self.role, self.old_user)

    @staticmethod
    @typechecked
    def from_user(my_user: MyUser) -> Self:
        return MyUserBuilder() \
            .set_t_user_id(my_user.t_user_id) \
            .set_email(my_user.email) \
            .set_full_name(my_user.full_name) \
            .set_sex(my_user.sex) \
            .set_user_name(my_user.user_name) \
            .set_user_info(my_user.user_info) \
            .set_ban(my_user.ban) \
            .set_is_student(my_user.is_student) \
            .set_is_work(my_user.is_worker) \
            .set_role(my_user.role) \
            .set_old_user(my_user.old_user)
        # .set_direction(my_user.direction) \
        # .set_course(my_user.course) \
        # .set_interest(my_user.interest)

    def to_dict(self) -> dict[str, Any]:
        return {
            't_user_id': self.t_user_id,
            'email': self.email,
            'full_name': self.full_name,
            'sex': self.sex,
            'user_name': self.user_name,
            'user_info': self.user_info,
            'ban': self.ban,
            'is_student': self.is_student,
            'is_worker': self.is_worker,
            'role': self.role,
            'old_user': self.old_user
        }

    @staticmethod
    @typechecked
    def from_dict(message: dict | None) -> Self | None:
        if not message:
            return None

        check_type('t_user_id', message['t_user_id'], int | None)
        check_type('email', message['email'], str | None)
        check_type('full_name', message['full_name'], str | None)
        check_type('sex', message['sex'], str | None)
        check_type('user_name', message['user_name'], str | None)
        # check_type('direction', message['direction'], str | None)
        # check_type('course', message['course'], str | None)
        check_type('user_info', message['user_info'], str | None)
        check_type('ban', message['ban'], bool)
        check_type('is_student', message['is_student'], bool)
        check_type('is_worker', message['is_worker'], bool)
        check_type('role', message['role'], str | None)
        check_type('old_user', message['old_user'], bool)

        my_builder: MyUserBuilder = MyUserBuilder()

        t_user_id: int | None = typing.cast(int | None, message['t_user_id'])
        if t_user_id:
            my_builder.set_t_user_id(t_user_id)

        email: str | None = typing.cast(str | None, message['email'])
        if email:
            my_builder.set_email(email)

        full_name: str | None = typing.cast(str | None, message['full_name'])
        if full_name:
            my_builder.set_full_name(full_name)

        sex: Sex | None = typing.cast(str | None, message['sex'])
        if sex:
            my_builder.set_sex(Sex(sex))

        user_name: str | None = typing.cast(str | None, message['user_name'])
        if user_name:
            my_builder.set_user_name(user_name)

        user_info: str | None = typing.cast(str | None, message['user_info'])
        if user_info:
            my_builder.set_user_info(user_info)

        ban: bool = typing.cast(bool, message['ban'])
        my_builder.set_ban(ban)

        is_student: bool = typing.cast(bool, message['is_student'])
        my_builder.set_is_student(is_student)

        is_worker: bool = typing.cast(bool, message['is_worker'])
        my_builder.set_is_work(is_worker)

        role: Role | None = typing.cast(str | None, message['role'])
        if role:
            my_builder.set_role(Role(role))

        old_user: bool = typing.cast(bool, message['old_user'])
        my_builder.set_old_user(old_user)
        return my_builder
