from datetime import datetime

from .column import Column
from .column import parse_list
from .util import clean_username

__all__ = [
    "users_table",
    "submissions_table",
    "journals_table",
    "comments_table",
    "settings_table",
    "history_table",
    "UsersColumns",
    "SubmissionsColumns",
    "JournalsColumns",
    "CommentsColumns",
    "SettingsColumns",
    "HistoryColumns",
]

users_table: str = "USERS"
submissions_table: str = "SUBMISSIONS"
journals_table: str = "JOURNALS"
comments_table: str = "COMMENTS"
settings_table: str = "SETTINGS"
history_table: str = "HISTORY"


class Columns:
    @classmethod
    def as_list(cls) -> list[Column]:
        return [getattr(cls, k) for k in cls.__annotations__.keys()]


class UsersColumns(Columns):
    USERNAME: Column = Column("USERNAME", str, unique=True, key=True, check="length({name}) > 0",
                              to_entry=clean_username)
    FOLDERS: Column = Column("FOLDERS", set)
    ACTIVE: Column = Column("ACTIVE", bool)
    USERPAGE: Column = Column("USERPAGE", str, to_entry=str.strip)


class SubmissionsColumns(Columns):
    ID: Column = Column("ID", int, unique=True, key=True, check="{name} > 0")
    AUTHOR: Column = Column("AUTHOR", str, check="length({name}) > 0")
    TITLE: Column = Column("TITLE", str)
    DATE: Column = Column("DATE", datetime)
    DESCRIPTION: Column = Column("DESCRIPTION", str)
    FOOTER: Column = Column("FOOTER", str)
    TAGS: Column = Column("TAGS", list)
    CATEGORY: Column = Column("CATEGORY", str)
    SPECIES: Column = Column("SPECIES", str)
    GENDER: Column = Column("GENDER", str)
    RATING: Column = Column("RATING", str)
    TYPE: Column = Column("TYPE", str, to_entry=str.lower, check="{name} in ('image', 'music', 'text', 'flash')")
    FILEURL: Column = Column("FILEURL", list[str], from_entry=lambda v: parse_list(v))
    FILEEXT: Column = Column("FILEEXT", list[str], from_entry=lambda v: parse_list(v))
    FILESAVED: Column = Column("FILESAVED", int, check="{name} in (0, 1, 2, 3, 4, 5, 6, 7)")
    FAVORITE: Column = Column("FAVORITE", set)
    MENTIONS: Column = Column("MENTIONS", set)
    FOLDER: Column = Column("FOLDER", str, to_entry=str.lower, check="{name} in ('gallery', 'scraps')")
    USERUPDATE: Column = Column("USERUPDATE", bool)


class JournalsColumns(Columns):
    ID: Column = Column("ID", int, unique=True, key=True, check="{name} > 0")
    AUTHOR: Column = Column("AUTHOR", str, check="length({name}) > 0")
    TITLE: Column = Column("TITLE", str)
    DATE: Column = Column("DATE", datetime)
    CONTENT: Column = Column("CONTENT", str)
    HEADER: Column = Column("HEADER", str)
    FOOTER: Column = Column("FOOTER", str)
    MENTIONS: Column = Column("MENTIONS", set)
    USERUPDATE: Column = Column("USERUPDATE", bool)


class CommentsColumns(Columns):
    ID: Column = Column("ID", int, key=True, check="{name} > 0")
    PARENT_TABLE: Column = Column("PARENT_TABLE", str, key=True,
                                  check=f"{'{name}'} in ('{submissions_table}', '{journals_table}')")
    PARENT_ID: Column = Column("PARENT_ID", int, key=True, check="{name} > 0")
    REPLY_TO: Column = Column("REPLY_TO", int, not_null=False, check="{name} == null or {name} > 0")
    AUTHOR: Column = Column("AUTHOR", str, check="length({name}) > 0")
    DATE: Column = Column("DATE", datetime, to_entry=lambda v: v.strftime("%Y-%m-%dT%H:%M:%S"),
                          from_entry=lambda v: datetime.strptime(v, "%Y-%m-%dT%H:%M:%S"))
    TEXT: Column = Column("TEXT", str)


class SettingsColumns(Columns):
    SETTING: Column = Column("SETTING", str, unique=True, key=True, check="length({name}) > 0")
    SVALUE: Column = Column("SVALUE", str, not_null=False, check="{name} == null or length({name}) > 0")


class HistoryColumns(Columns):
    TIME: Column = Column("TIME", datetime, unique=True, key=True,
                          to_entry=lambda v: v.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                          from_entry=lambda v: datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f"))
    EVENT: Column = Column("EVENT", str)
