import typing
from dataclasses import dataclass
from typing import Literal, Self, Any

from aiogram.types import InlineKeyboardMarkup
from typeguard import typechecked, check_type


@dataclass
class MyMessage:
    message_id: int
    chat_id: int
    with_reply_markup: bool
    text: str
    reply_markup: InlineKeyboardMarkup | None
    parse_mode: Literal["HTML"]

    def to_dict(self) -> dict[str, Any]:
        return {
            'message_id': self.message_id,
            'chat_id': self.chat_id,
            'with_reply_markup': self.with_reply_markup,
            'text': self.text,
            'reply_markup': self.reply_markup.to_python() if self.reply_markup else None,
            'parse_mode': self.parse_mode
        }

    @staticmethod
    @typechecked
    def from_dict(message: dict | None) -> Self | None:
        if not message:
            return None

        check_type('message_id', message['message_id'], int)
        check_type('chat_id', message['chat_id'], int)
        check_type('with_reply_markup', message['with_reply_markup'], bool)
        check_type('text', message['text'], str)
        check_type('reply_markup', message['reply_markup'], typing.Dict[str, typing.Any] | None)
        check_type('parse_mode', message['parse_mode'], Literal["HTML"])

        message_id: int = typing.cast(int, message['message_id'])
        chat_id: int = typing.cast(int, message['chat_id'])
        with_reply_markup: bool = typing.cast(bool, message['with_reply_markup'])
        text: str = typing.cast(str, message['text'])
        if message['reply_markup']:
            reply_markup: InlineKeyboardMarkup = InlineKeyboardMarkup.to_object(data=message['reply_markup'])
        else:
            reply_markup = None
        parse_mode: Literal["HTML"] = typing.cast(Literal["HTML"], message['parse_mode'])
        return MyMessage(message_id=message_id,
                         chat_id=chat_id,
                         with_reply_markup=with_reply_markup,
                         text=text,
                         reply_markup=reply_markup,
                         parse_mode=parse_mode)
